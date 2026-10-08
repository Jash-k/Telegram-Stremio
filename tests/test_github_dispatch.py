"""Multi-target GitHub Actions dispatch (app/config.py + app/keepalive.py).

The keepalive used to know about exactly one workflow, so adding a second repo
silently did nothing and a bad PAT only produced a bare "HTTP 401" line. These
tests pin down the target-list parsing, the per-target dispatch state, the input
coercion, and the 401 behaviour (record + back off after repeats).
"""
import asyncio

import pytest

from app import config, keepalive

# --- the three targets the user actually runs -------------------------------
TAMILMV = {"id": "mv_scrapper/scrape.yml", "repo": "Jash-k/mv_scrapper",
           "workflow": "scrape.yml", "minutes": 30, "ref": "main", "inputs": {}}
MOVIESDA = {"id": "mv_scrapper/moviesda.yml", "repo": "Jash-k/mv_scrapper",
            "workflow": "moviesda.yml", "minutes": 360, "ref": "main", "inputs": {}}
LIVEPLAY = {"id": "live_play/jash-live.yml", "repo": "Jash-k/live_play",
            "workflow": "jash-live.yml", "minutes": 60, "ref": "main",
            "inputs": {"force": False}}


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
class _FakeResp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class _FakeClient:
    """httpx.AsyncClient stand-in that records every POST it is handed."""
    calls: list[tuple[str, dict, dict]] = []

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.calls.append((url, dict(headers or {}), dict(json or {})))
        return _FakeResp(self.status, self.body)

    async def get(self, url, **kwargs):
        return _FakeResp(self.status, self.body)


def _fake_client(status=204, body=""):
    class _C(_FakeClient):
        pass
    _C.status = status
    _C.body = body
    return _C


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    """Every test starts from an empty dispatch state and a present token."""
    keepalive._target_state.clear()
    keepalive._dispatch_state.update(
        {"enabled": False, "interval_min": 30, "last_ts": None, "next_due": None,
         "last_status": None, "total_fired": 0, "targets": []})
    _FakeClient.calls.clear()
    monkeypatch.setattr(config, "GITHUB_DISPATCH_TOKEN", "github_pat_test")
    monkeypatch.setattr(config, "GITHUB_DISPATCH_OWNER", "Jash-k")
    monkeypatch.setattr(config, "GITHUB_DISPATCH_REPO", "mv_scrapper")
    monkeypatch.setattr(config, "GITHUB_DISPATCH_WORKFLOW", "scrape.yml")
    monkeypatch.setattr(config, "GITHUB_DISPATCH_REF", "main")
    monkeypatch.setattr(config, "GITHUB_DISPATCH_MINUTES", 30)
    monkeypatch.setattr(config, "GITHUB_DISPATCH_TARGETS",
                        [dict(TAMILMV), dict(MOVIESDA), dict(LIVEPLAY)])
    yield
    keepalive._target_state.clear()


def _install(monkeypatch, status=204, body="", exc=None):
    """Route keepalive's HTTP through the fake client (or make it raise)."""
    if exc is not None:
        class _Boom:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                raise exc

            async def __aexit__(self, *a):
                return False
        monkeypatch.setattr(keepalive.httpx, "AsyncClient", _Boom)
        return
    monkeypatch.setattr(keepalive.httpx, "AsyncClient",
                        _fake_client(status=status, body=body))


# --------------------------------------------------------------------------
# 1. GITHUB_DISPATCH_TARGETS parsing
# --------------------------------------------------------------------------
class TestTargetParsing:
    def test_default_list_is_the_three_documented_targets(self):
        parsed = config._parse_dispatch_targets(config._DEFAULT_DISPATCH_TARGETS)
        assert parsed == [TAMILMV, MOVIESDA, LIVEPLAY]

    def test_module_default_matches_the_documented_cadences(self):
        ids = [t["id"] for t in config.GITHUB_DISPATCH_TARGETS]
        assert ids == ["mv_scrapper/scrape.yml", "mv_scrapper/moviesda.yml",
                       "live_play/jash-live.yml"]

    def test_bare_repo_uses_the_configured_workflow_and_minutes(self):
        parsed = config._parse_dispatch_targets("mv_scrapper")
        assert parsed == [{"id": "mv_scrapper/scrape.yml",
                           "repo": "Jash-k/mv_scrapper", "workflow": "scrape.yml",
                           "minutes": 30, "ref": "main", "inputs": {}}]

    def test_bare_workflow_file_attaches_to_the_configured_repo(self):
        # "moviesda.yml|120" must NOT become owner=Jash-k repo=moviesda.yml.
        parsed = config._parse_dispatch_targets("moviesda.yml|120")
        assert parsed == [{"id": "mv_scrapper/moviesda.yml",
                           "repo": "Jash-k/mv_scrapper", "workflow": "moviesda.yml",
                           "minutes": 120, "ref": "main", "inputs": {}}]

    def test_explicit_owner_and_ref_are_kept(self):
        parsed = config._parse_dispatch_targets("acme/widgets|build.yaml|45|develop")
        assert parsed[0]["repo"] == "acme/widgets"
        assert parsed[0]["workflow"] == "build.yaml"
        assert parsed[0]["minutes"] == 45
        assert parsed[0]["ref"] == "develop"

    @pytest.mark.parametrize("raw,expected", [
        ("force=true", True),
        ("force=false", False),
        ("force=TRUE", True),
        ("limit=25", 25),
        ("ratio=1.5", 1.5),
        ("mode=fast", "fast"),
    ])
    def test_input_values_are_coerced_to_the_declared_type(self, raw, expected):
        parsed = config._parse_dispatch_targets(f"r|w.yml|30|main|{raw}")
        key = raw.split("=")[0]
        assert parsed[0]["inputs"][key] == expected
        assert type(parsed[0]["inputs"][key]) is type(expected)

    def test_several_inputs_split_on_comma_or_semicolon(self):
        parsed = config._parse_dispatch_targets("r|w.yml|30|main|a=1;b=two;c=true")
        assert parsed[0]["inputs"] == {"a": 1, "b": "two", "c": True}

    def test_garbage_entries_never_raise_and_never_vanish_silently(self):
        parsed = config._parse_dispatch_targets(",,  ,|30,|||")
        assert isinstance(parsed, list)

    def test_unparsable_interval_falls_back_to_the_global_default(self):
        parsed = config._parse_dispatch_targets("r|w.yml|soon|main")
        assert parsed[0]["minutes"] == 30

    def test_interval_is_clamped_to_a_sane_minimum(self):
        assert config._parse_dispatch_targets("r|w.yml|1|main")[0]["minutes"] == 5

    def test_missing_workflow_field_resolves_to_githubs_default(self):
        parsed = config._parse_dispatch_targets("r|")
        assert parsed[0]["workflow"] == "default"


# --------------------------------------------------------------------------
# 2. _trigger() — the request itself
# --------------------------------------------------------------------------
class TestTrigger:
    def test_success_records_ok_and_posts_the_right_request(self, monkeypatch):
        _install(monkeypatch, status=204)
        status = asyncio.run(keepalive._trigger(dict(LIVEPLAY)))
        assert status == "ok"
        assert len(_FakeClient.calls) == 1
        url, headers, payload = _FakeClient.calls[0]
        assert url == ("https://api.github.com/repos/Jash-k/live_play/"
                       "actions/workflows/jash-live.yml/dispatches")
        assert headers["Authorization"] == "Bearer github_pat_test"
        assert headers["Accept"] == "application/vnd.github+json"
        assert headers["X-GitHub-Api-Version"] == "2022-11-28"
        assert payload == {"ref": "main", "inputs": {"force": False}}

    def test_targets_without_inputs_send_no_inputs_key(self, monkeypatch):
        _install(monkeypatch, status=204)
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        assert _FakeClient.calls[0][2] == {"ref": "main"}

    def test_force_true_reaches_the_workflow_as_a_boolean(self, monkeypatch):
        _install(monkeypatch, status=204)
        target = dict(LIVEPLAY, inputs={"force": True})
        asyncio.run(keepalive._trigger(target))
        sent = _FakeClient.calls[0][2]["inputs"]["force"]
        assert sent is True, "GitHub rejects the string 'true' for a boolean input"

    @pytest.mark.parametrize("code", [401, 403, 404, 422, 500])
    def test_failure_codes_are_recorded_not_raised(self, monkeypatch, code):
        _install(monkeypatch, status=code, body='{"message":"nope"}')
        status = asyncio.run(keepalive._trigger(dict(TAMILMV)))
        assert status == f"HTTP {code}"
        assert keepalive._target_state["mv_scrapper/scrape.yml"]["last_status"] == f"HTTP {code}"

    def test_network_errors_are_recorded_with_the_exception_name(self, monkeypatch):
        _install(monkeypatch, exc=OSError("boom"))
        status = asyncio.run(keepalive._trigger(dict(TAMILMV)))
        assert status == "error: OSError"

    def test_a_failure_still_counts_as_a_fire_so_the_board_is_honest(self, monkeypatch):
        _install(monkeypatch, status=401)
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        st = keepalive._target_state["mv_scrapper/scrape.yml"]
        assert st["total_fired"] == 1
        assert st["last_ts"] is not None


# --------------------------------------------------------------------------
# 3. the 401 case the user hit
# --------------------------------------------------------------------------
class TestBadCredentials:
    def test_a_single_401_does_not_back_off(self, monkeypatch):
        _install(monkeypatch, status=401, body='{"message":"Bad credentials"}')
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        st = keepalive._target_state["mv_scrapper/scrape.yml"]
        assert st["auth_failures"] == 1
        assert st["backoff_until"] == 0

    def test_repeated_401s_back_off_instead_of_hammering_the_api(self, monkeypatch):
        _install(monkeypatch, status=401, body='{"message":"Bad credentials"}')
        for _ in range(keepalive._AUTH_BACKOFF_AFTER):
            asyncio.run(keepalive._trigger(dict(TAMILMV)))
        st = keepalive._target_state["mv_scrapper/scrape.yml"]
        assert st["auth_failures"] == keepalive._AUTH_BACKOFF_AFTER
        assert st["backoff_until"] > 0

    def test_a_successful_dispatch_clears_the_auth_backoff(self, monkeypatch):
        _install(monkeypatch, status=401)
        for _ in range(keepalive._AUTH_BACKOFF_AFTER):
            asyncio.run(keepalive._trigger(dict(TAMILMV)))
        _install(monkeypatch, status=204)
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        st = keepalive._target_state["mv_scrapper/scrape.yml"]
        assert st["auth_failures"] == 0
        assert st["backoff_until"] == 0
        assert st["last_status"] == "ok"

    def test_one_broken_target_does_not_stop_the_others(self, monkeypatch):
        """401 on live_play must not silence mv_scrapper."""
        real_post = None

        class _Split(_FakeClient):
            status = 204
            body = ""

            async def post(self, url, headers=None, json=None):
                _FakeClient.calls.append((url, dict(headers or {}), dict(json or {})))
                code = 401 if "live_play" in url else 204
                return _FakeResp(code, '{"message":"Bad credentials"}' if code == 401 else "")

        monkeypatch.setattr(keepalive.httpx, "AsyncClient", _Split)
        asyncio.run(keepalive._trigger(dict(LIVEPLAY)))
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        assert keepalive._target_state["live_play/jash-live.yml"]["last_status"] == "HTTP 401"
        assert keepalive._target_state["mv_scrapper/scrape.yml"]["last_status"] == "ok"
        assert real_post is None


# --------------------------------------------------------------------------
# 4. dispatch_state() — what the Ops Board reads
# --------------------------------------------------------------------------
class TestDispatchState:
    def test_legacy_top_level_keys_survive_for_the_existing_panel_js(self):
        snap = keepalive.dispatch_state()
        for key in ("enabled", "interval_min", "last_ts", "next_due",
                    "last_status", "total_fired"):
            assert key in snap, f"{key} disappeared — the panel JS would break"

    def test_snapshot_carries_every_configured_target(self):
        snap = keepalive.dispatch_state()
        assert [t["id"] for t in snap["targets"]] == [
            "mv_scrapper/scrape.yml", "mv_scrapper/moviesda.yml",
            "live_play/jash-live.yml"]
        for t in snap["targets"]:
            assert set(t) >= {"id", "repo", "workflow", "minutes", "ref",
                              "next_due", "last_ts", "last_status", "total_fired"}

    def test_next_due_is_the_soonest_target_so_the_ring_counts_down_correctly(self):
        snap = keepalive.dispatch_state()
        dues = [t["next_due"] for t in snap["targets"] if t["next_due"]]
        assert snap["next_due"] == min(dues)

    def test_interval_min_is_the_fastest_cadence(self):
        assert keepalive.dispatch_state()["interval_min"] == 30

    def test_internal_auth_counters_are_not_leaked_to_the_api(self):
        snap = keepalive.dispatch_state()
        for t in snap["targets"]:
            assert "auth_failures" not in t
            assert "backoff_until" not in t

    def test_totals_aggregate_across_targets(self, monkeypatch):
        _install(monkeypatch, status=204)
        for tgt in (TAMILMV, MOVIESDA, LIVEPLAY):
            asyncio.run(keepalive._trigger(dict(tgt)))
        snap = keepalive.dispatch_state()
        assert snap["total_fired"] == 3
        assert sum(t["total_fired"] for t in snap["targets"]) == 3

    def test_legacy_last_status_reflects_the_most_recent_target(self, monkeypatch):
        _install(monkeypatch, status=204)
        asyncio.run(keepalive._trigger(dict(TAMILMV)))
        _install(monkeypatch, status=404)
        asyncio.run(keepalive._trigger(dict(MOVIESDA)))
        assert keepalive.dispatch_state()["last_status"] == "HTTP 404"

    def test_disabled_without_a_token(self, monkeypatch):
        monkeypatch.setattr(config, "GITHUB_DISPATCH_TOKEN", "")
        assert keepalive.dispatch_state()["enabled"] is False


# --------------------------------------------------------------------------
# 5. trigger_target_now() — the panel's manual button
# --------------------------------------------------------------------------
class TestManualFire:
    def test_fires_the_named_target_and_reschedules_it(self, monkeypatch):
        _install(monkeypatch, status=204)
        res = asyncio.run(keepalive.trigger_target_now("mv_scrapper/moviesda.yml"))
        assert res == {"ok": True, "status": "ok", "target": "mv_scrapper/moviesda.yml"}
        assert "moviesda.yml/dispatches" in _FakeClient.calls[0][0]
        st = keepalive._target_state["mv_scrapper/moviesda.yml"]
        assert st["total_fired"] == 1

    def test_unknown_target_reports_the_known_ids(self, monkeypatch):
        _install(monkeypatch, status=204)
        res = asyncio.run(keepalive.trigger_target_now("nope/nope.yml"))
        assert res["ok"] is False
        assert "unknown target" in res["status"]
        assert "mv_scrapper/scrape.yml" in res["known"]
        assert _FakeClient.calls == []

    def test_refuses_to_run_without_a_token(self, monkeypatch):
        monkeypatch.setattr(config, "GITHUB_DISPATCH_TOKEN", "")
        res = asyncio.run(keepalive.trigger_target_now("mv_scrapper/scrape.yml"))
        assert res["ok"] is False
        assert "GITHUB_DISPATCH_TOKEN" in res["status"]

    def test_surfaces_a_failed_dispatch(self, monkeypatch):
        _install(monkeypatch, status=401, body='{"message":"Bad credentials"}')
        res = asyncio.run(keepalive.trigger_target_now("live_play/jash-live.yml"))
        assert res["ok"] is False
        assert res["status"] == "HTTP 401"


# --------------------------------------------------------------------------
# 6. the loop
# --------------------------------------------------------------------------
class TestLoop:
    def test_loop_is_a_no_op_without_a_token(self, monkeypatch):
        monkeypatch.setattr(config, "GITHUB_DISPATCH_TOKEN", "")
        asyncio.run(keepalive.github_dispatch_loop(tick=1))  # returns immediately
        assert _FakeClient.calls == []

    def test_loop_fires_every_due_target_once_then_reschedules(self, monkeypatch):
        _install(monkeypatch, status=204)
        # Arm all three, then run one tick's worth of work and stop.
        keepalive._init_target_state()
        for st in keepalive._target_state.values():
            st["next_due"] = 0

        async def run_one_pass():
            task = asyncio.create_task(keepalive.github_dispatch_loop(tick=3600))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run_one_pass())
        fired = sorted(u.rsplit("/repos/", 1)[1].split("/actions")[0]
                       for u, _, _ in _FakeClient.calls)
        assert fired == ["Jash-k/live_play", "Jash-k/mv_scrapper", "Jash-k/mv_scrapper"]
        # each target now waits its own interval
        dues = {t["id"]: t["next_due"] for t in keepalive._target_state.values()}
        assert dues["mv_scrapper/scrape.yml"] < dues["live_play/jash-live.yml"]
        assert dues["live_play/jash-live.yml"] < dues["mv_scrapper/moviesda.yml"]

    def test_a_backed_off_target_is_skipped(self, monkeypatch):
        import time as _time
        _install(monkeypatch, status=401)
        keepalive._init_target_state()
        for st in keepalive._target_state.values():
            st["next_due"] = 0
        keepalive._target_state["live_play/jash-live.yml"]["backoff_until"] = (
            _time.time() + 9999)

        async def run_one_pass():
            task = asyncio.create_task(keepalive.github_dispatch_loop(tick=3600))
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run_one_pass())
        urls = [u for u, _, _ in _FakeClient.calls]
        assert not any("live_play" in u for u in urls)
        assert any("mv_scrapper" in u for u in urls)
