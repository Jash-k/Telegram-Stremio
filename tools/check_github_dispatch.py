#!/usr/bin/env python3
"""Diagnose GitHub `workflow_dispatch` failures without deploying.

GitHub answers every failure with a terse status code, so a "Bad credentials"
line in the Koyeb log tells you nothing about *why*. This checks the token step
by step — who it belongs to, which repos it can see, which workflows exist in
them, whether each one accepts workflow_dispatch — and fires a real dispatch
only if you pass --fire.

By default it checks every target in GITHUB_DISPATCH_TARGETS (or the built-in
default list), so one run validates mv_scrapper/scrape.yml,
mv_scrapper/moviesda.yml and live_play/jash-live.yml together.

Usage
-----
    python3 tools/check_github_dispatch.py                    # env / config.env
    python3 tools/check_github_dispatch.py --token ghp_xxx
    python3 tools/check_github_dispatch.py --fire             # send one real dispatch each
    python3 tools/check_github_dispatch.py --repo Jash-k/live_play --workflow jash-live.yml --fire
"""
import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

API = "https://api.github.com"

# What each status actually means for POST .../actions/workflows/{id}/dispatches
MEANING = {
    401: "Bad credentials — the token itself is wrong, expired, or revoked. "
         "GitHub checks this BEFORE permissions, so it is never a scope problem.",
    403: "Token recognised but refused. Usually a fine-grained PAT whose "
         "'Repository access' does not include this repo, or Actions: Read and "
         "write is missing, or an org SSO token that was never authorised.",
    404: "Token recognised but it cannot SEE this repo/workflow, or the "
         "workflow filename is wrong. GitHub hides private repos behind 404, so "
         "a typo and a missing permission look identical.",
    422: "The workflow exists but does not accept workflow_dispatch, a required "
         "input is missing, or `ref` is not a real branch.",
}

TOKEN_FIX = """
    Create a NEW token and replace the GITHUB_DISPATCH_TOKEN secret, then redeploy:
      • fine-grained PAT → Repository access: "Only select repositories" →
          Jash-k/mv_scrapper AND Jash-k/live_play
        Permissions → Repository permissions → Actions → Read and write
      • classic PAT → tick the `repo` scope (and `workflow`)
    Verify it here before deploying:  python3 tools/check_github_dispatch.py
""".rstrip("\n")


def _mask(tok: str) -> str:
    tok = (tok or "").strip()
    if not tok:
        return "<empty>"
    if len(tok) <= 12:
        return f"<{len(tok)} chars>"
    return f"{tok[:6]}…{tok[-4:]} ({len(tok)} chars)"


def _token_kind(tok: str) -> str:
    t = (tok or "").strip()
    for prefix, label in (
        ("github_pat_", "fine-grained PAT"),
        ("ghp_", "classic PAT"),
        ("gho_", "OAuth token"),
        ("ghs_", "Actions job token (GITHUB_TOKEN) — only valid INSIDE a workflow run"),
        ("ghu_", "GitHub App user token"),
        ("ghr_", "GitHub App installation token"),
    ):
        if t.startswith(prefix):
            return label
    return "unrecognised format (a PAT starts with ghp_ or github_pat_)"


async def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--token", default=None,
                   help="PAT to test (default: GITHUB_DISPATCH_TOKEN / config.env)")
    p.add_argument("--repo", action="append", default=None,
                   help="owner/repo to check (repeatable); overrides the config targets")
    p.add_argument("--workflow", action="append", default=None,
                   help="workflow filename or id, paired with --repo in order")
    p.add_argument("--ref", default=None, help="branch to dispatch against")
    p.add_argument("--fire", action="store_true",
                   help="actually POST a workflow_dispatch for each healthy target")
    args = p.parse_args()

    import httpx

    from app import config

    token = (args.token or os.getenv("GITHUB_DISPATCH_TOKEN")
             or config.GITHUB_DISPATCH_TOKEN or "").strip()
    env_raw = os.getenv("GITHUB_DISPATCH_TOKEN", "")

    # Targets: explicit --repo/--workflow pairs, else the configured ones.
    if args.repo:
        wfs = args.workflow or []
        targets = [{"id": f"{r.split('/')[-1]}/{wfs[i] if i < len(wfs) else 'default'}",
                    "repo": r if "/" in r else f"{config.GITHUB_DISPATCH_OWNER}/{r}",
                    "workflow": (wfs[i] if i < len(wfs) else
                                 config.GITHUB_DISPATCH_WORKFLOW),
                    "ref": args.ref or config.GITHUB_DISPATCH_REF or "main",
                    "minutes": 0, "inputs": {}}
                   for i, r in enumerate(args.repo)]
    else:
        targets = list(config.GITHUB_DISPATCH_TARGETS)
        if args.ref:
            for t in targets:
                t = dict(t)
        targets = [{**t, "ref": args.ref or t.get("ref") or "main"} for t in targets]

    print("=" * 76)
    print("GitHub workflow_dispatch diagnostic")
    print("=" * 76)
    print(f"token   : {_mask(token)}")
    print(f"kind    : {_token_kind(token)}")
    print(f"targets : {len(targets)}")
    for t in targets:
        cadence = f"every {t['minutes']}m" if t.get("minutes") else "on demand"
        extra = f" inputs={t['inputs']}" if t.get("inputs") else ""
        print(f"            · {t['repo']} @ {t['workflow']} (ref {t['ref']}, {cadence}){extra}")

    if not token:
        print("\n✗ No token found — this alone produces the 401 'Bad credentials' warning.")
        print(TOKEN_FIX)
        return 2
    if env_raw and env_raw != env_raw.strip():
        print("\n✗ GITHUB_DISPATCH_TOKEN has leading/trailing whitespace in the environment.")
        print("  Koyeb secrets keep trailing newlines from a copy/paste — retype it.")
    if env_raw.strip() and env_raw.strip()[0] in "\"'" and env_raw.strip()[-1] in "\"'":
        print("\n✗ GITHUB_DISPATCH_TOKEN looks quoted — paste the bare token, no quotes.")

    headers = {"Accept": "application/vnd.github+json",
               "Authorization": f"Bearer {token}",
               "X-GitHub-Api-Version": "2022-11-28"}

    failures = 0
    async with httpx.AsyncClient(timeout=25.0,
                                 headers=headers,
                                 follow_redirects=True) as c:
        # ---- 1. is the token valid at all? -------------------------------
        print("\n[1] GET /user — is the token recognised?")
        r = await c.get(f"{API}/user")
        if r.status_code == 401:
            print(f"    ✗ HTTP 401. {MEANING[401]}")
            print(TOKEN_FIX)
            print("\n    Nothing else can be checked until the token is accepted.")
            return 1
        if r.status_code != 200:
            print(f"    ✗ HTTP {r.status_code}: {r.text[:200]}")
            return 1
        me = r.json()
        print(f"    ✓ authenticated as @{me.get('login')} (id {me.get('id')})")
        scopes = r.headers.get("x-oauth-scopes")
        if scopes is not None:
            print(f"    classic scopes: {scopes or '(none)'}")
            if "repo" not in scopes and "workflow" not in scopes:
                print("    ✗ neither `repo` nor `workflow` scope — dispatch will fail.")
                failures += 1
        exp = r.headers.get("github-authentication-token-expiration")
        if exp:
            print(f"    token expires: {exp}")

        repo_cache: dict[str, dict] = {}
        for t in targets:
            repo, wf, ref = t["repo"], t["workflow"], t["ref"]
            print(f"\n[2] {repo} @ {wf}")

            # ---- 2. visibility -------------------------------------------
            if repo in repo_cache:
                info = repo_cache[repo]
                print(f"    ✓ visible (cached) — private={info.get('private')}")
            else:
                rr = await c.get(f"{API}/repos/{repo}")
                if rr.status_code == 404:
                    print(f"    ✗ HTTP 404. {MEANING[404]}")
                    print(f"    → add {repo} to the PAT's 'Repository access' selection.")
                    failures += 1
                    continue
                if rr.status_code != 200:
                    print(f"    ✗ HTTP {rr.status_code}: {rr.text[:160]}")
                    failures += 1
                    continue
                info = rr.json()
                repo_cache[repo] = info
                print(f"    ✓ visible (private={info.get('private')}, "
                      f"default_branch={info.get('default_branch')}, "
                      f"actions_enabled={info.get('has_actions')})")
                if not info.get("has_actions"):
                    print("    ✗ Actions are DISABLED on this repo — nothing can run.")
                    failures += 1

            if ref != info.get("default_branch"):
                br = await c.get(f"{API}/repos/{repo}/branches/{ref}")
                if br.status_code == 200:
                    print(f"    ✓ ref {ref!r} exists")
                else:
                    print(f"    ✗ ref {ref!r} → HTTP {br.status_code}: no such branch "
                          f"(default is {info.get('default_branch')!r}) — dispatch returns 422")
                    failures += 1
                    continue

            # ---- 3. which workflows exist? --------------------------------
            wr = await c.get(f"{API}/repos/{repo}/actions/workflows")
            found = None
            if wr.status_code == 200:
                for w in wr.json().get("workflows", []):
                    fn = (w.get("path") or "").split("/")[-1]
                    mark = ""
                    if wf in (fn, str(w.get("id"))):
                        found = w
                        mark = "   ← TARGET"
                    print(f"      · id={w.get('id'):<10} {fn:<26} state={w.get('state')}{mark}")
                if found is None:
                    print(f"    ✗ {wf!r} is NOT in this repo → dispatch returns 404.")
                    print("      Check the filename, including the .yml extension.")
                    failures += 1
                    continue
            else:
                print(f"      (could not list workflows: HTTP {wr.status_code})")

            # ---- 4. does it accept workflow_dispatch? ---------------------
            yml = await c.get(
                f"https://raw.githubusercontent.com/{repo}/{ref}"
                f"/.github/workflows/{wf}")
            if yml.status_code == 200:
                if "workflow_dispatch" in yml.text:
                    print(f"    ✓ {wf} declares workflow_dispatch")
                else:
                    print(f"    ✗ {wf} has NO `workflow_dispatch:` trigger — dispatch "
                          f"returns 422. Add it under `on:`.")
                    failures += 1
                    continue
            else:
                print(f"    ! could not read the workflow file to check its triggers "
                      f"(HTTP {yml.status_code}) — public raw fetch only")

            if found and found.get("state") != "active":
                print(f"    ✗ workflow state is {found.get('state')!r}, not 'active' "
                      f"— GitHub refuses the dispatch.")
                failures += 1
                continue

            # ---- 5. the real thing ---------------------------------------
            if not args.fire:
                print("    [5] dispatch not sent (pass --fire to send one)")
                continue
            payload: dict = {"ref": ref}
            if t.get("inputs"):
                payload["inputs"] = dict(t["inputs"])
            print(f"[5] POST dispatch ref={ref}"
                  + (f" inputs={payload['inputs']}" if "inputs" in payload else ""))
            dr = await c.post(
                f"{API}/repos/{repo}/actions/workflows/{wf}/dispatches", json=payload)
            if dr.status_code == 204:
                print("    ✓ HTTP 204 — run queued "
                      f"(https://github.com/{repo}/actions)")
            else:
                print(f"    ✗ HTTP {dr.status_code}: {dr.text[:300]}")
                if dr.status_code in MEANING:
                    print(f"      meaning: {MEANING[dr.status_code]}")
                if dr.status_code == 422 and t.get("inputs"):
                    print("      → an input name/type does not match the workflow's "
                          "`workflow_dispatch.inputs` block.")
                failures += 1

    print("\n" + "=" * 76)
    if failures:
        print(f"RESULT: {failures} problem(s) found — see the ✗ lines above.")
        return 1
    print("RESULT: token and every target look healthy."
          + ("" if args.fire else " Re-run with --fire to test a real dispatch."))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
