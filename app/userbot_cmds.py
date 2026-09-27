"""Userbot slash commands for the self-chat (Saved Messages).

Installed alongside the live-indexing handlers (same client, same install
path), so they exist exactly where the userbot exists. Every command is
read-mostly and wrapped: a dead DB or session must produce a useful error
line, never a crash or a traceback in Telegram.

  /stats                     one-screen system snapshot
  /heal                      trigger the Self-Heal pass (same code as panel)
  /feed                      run one PreDVD/Digital feed check now
  /primary list | <id|name>  see / switch the primary leech group (v14)
  /watch add|del|list <t>    pin a title; DM you when it lands
"""
import time

from .logger import LOGGER

_installed = False
_HELP = (
    "Commands (self-chat only):\n"
    "/stats — session, indexer, queues, last actions\n"
    "/heal — Self-Heal run (episode fix + relinks + unindexed retry)\n"
    "/feed — check release feed + drain leech queue now\n"
    "/primary list — leech groups | /primary <n|id> — switch primary\n"
    "/watch add <title> — DM me when it lands | /watch del | /watch list"
)


def _fmt_age(ts) -> str:
    if not ts:
        return "never"
    s = max(0, int(time.time() - float(ts)))
    if s < 90:
        return f"{s}s ago"
    if s < 5400:
        return f"{s // 60}m ago"
    if s < 172800:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


async def _stats_text() -> str:
    from . import client as client_mod
    from . import indexer

    lines = []
    try:
        sess = client_mod.session_status()
        led = "🟢 connected" if sess.get("connected") else "🔴 offline"
        who = sess.get("username") or sess.get("first_name") or "?"
        lines.append(f"SESSION {led} — @{who} (dc {sess.get('dc_id') or '?'})")
        if sess.get("last_error"):
            lines.append(f"  last error: {str(sess['last_error'])[:100]}")
    except Exception as exc:
        lines.append(f"SESSION — status unavailable ({exc})")

    try:
        st = indexer.status()
        run = "🏃 RUNNING" if st.get("running") else "😴 IDLE"
        lines.append(f"INDEXER {run} · processed {st.get('processed', 0)}"
                     f" · indexed {st.get('indexed_total', 0)} (since boot)")
        if st.get("current_chat"):
            lines.append(f"  now: chat {st['current_chat']} / {st.get('current_filter') or '-'}")
        if st.get("last_error"):
            lines.append(f"  last error: {str(st['last_error'])[:100]}")
        if st.get("last_indexed_ts"):
            lines.append(f"  last file indexed: {_fmt_age(st['last_indexed_ts'])}")
    except Exception as exc:
        lines.append(f"INDEXER — unavailable ({exc})")

    try:
        from . import db

        unidx = await db.col("unindexed").count_documents({})
        files = await db.col("files").count_documents({})
        meta = await db.col("meta").count_documents({})
        lines.append(f"DB ✅ · files {files:,} · titles {meta:,} · unindexed {unidx:,}")
    except Exception as exc:
        lines.append(f"DB 🔴 unavailable ({type(exc).__name__})")

    try:
        from . import predvd_automator

        q = await predvd_automator.queue_depth()
        prim = (await predvd_automator.get_leech_targets() or [{}])[0]
        lines.append(f"LEECH queue {q} · primary {prim.get('label') or prim.get('group_id') or '-'}")
    except Exception:
        pass

    try:
        from .keepalive import dispatch_state

        dsp = dispatch_state()
        if dsp.get("enabled"):
            lines.append(f"SCRAPER next feed pull in ~{max(0, int((dsp.get('next_due') or time.time()) - time.time())) // 60}m"
                         f" · last: {dsp.get('last_status') or '-'}")
        else:
            lines.append("SCRAPER auto-dispatch disabled (no token)")
    except Exception:
        pass

    try:
        from .opslog import recent_ops

        entries = await recent_ops(3)
        if entries:
            lines.append("LAST ACTIONS")
            icons = {"cleanup": "🧹", "selfheal": "🪄", "index": "📥"}
            for e in entries:
                bits = " ".join(f"{k}={v}" for k, v in e.items()
                                if k not in ("type", "status", "at") and not isinstance(v, dict))
                lines.append(f"  {icons.get(e['type'], '•')} {e['type']} {e['status']} ({_fmt_age(e.get('at'))}) {bits[:80]}")
    except Exception:
        pass

    return "\n".join(lines)[:4000]


async def handle_command(client, message):
    """Parse + dispatch one self-chat command. Handler-level try/except: the
    userbot's update loop must survive anything these commands do."""
    try:
        text = (message.text or message.caption or "").strip()
        if not text.startswith("/"):
            return
        parts = text.split(maxsplit=2)
        cmd = parts[0].split("@", 1)[0].lower()
        arg = parts[1].lower() if len(parts) > 1 else ""
        rest = parts[2].strip() if len(parts) > 2 else ""

        if cmd == "/start" and not arg:
            reply = _HELP
        elif cmd == "/stats":
            reply = await _stats_text()
        elif cmd == "/heal":
            from .indexer import schedule_repair

            res = schedule_repair(backfill=True)
            reply = ("🪄 " if res.get("ok") else "⚠️ ") + (res.get("message") or res.get("detail") or "")
        elif cmd == "/feed":
            try:
                from . import predvd_automator

                result = await predvd_automator.process_feed_iteration()
                reply = f"📡 feed check done — {str(result)[:600]}"
            except Exception as exc:
                reply = f"⚠️ feed check failed: {type(exc).__name__}: {exc}"
        elif cmd == "/primary":
            reply = await _primary_cmd(arg, rest)
        elif cmd == "/watch":
            reply = await _watch_cmd(arg, rest)
        else:
            return  # not ours (avoid touching unrelated self-chat messages)

        await message.reply_text(reply[:4000])
    except Exception as exc:
        LOGGER.debug("[CMDS] handler error: %s", exc)
        try:
            await message.reply_text(f"⚠️ command failed: {type(exc).__name__}: {exc}")
        except Exception:
            pass


async def _primary_cmd(arg: str, rest: str) -> str:
    from . import predvd_automator

    targets = []
    try:
        targets = await predvd_automator.get_leech_targets()
    except Exception as exc:
        return f"⚠️ could not read groups: {exc}"
    if arg in ("list", ""):
        if not targets:
            return "⚠️ no leech groups configured"
        lines = ["Leech groups:"]
        for i, t in enumerate(targets, 1):
            star = " ⭐ primary" if t.get("is_primary") else ""
            lines.append(f"  {i}. {t.get('label') or t.get('group_id')}{star} — {t.get('group_id')}")
        lines.append("Switch: /primary <number or id>")
        return "\n".join(lines)
    pick = (arg + (" " + rest if rest else "")).strip()
    chosen = None
    if pick.isdigit() and 1 <= int(pick) <= len(targets):
        chosen = targets[int(pick) - 1]
    else:
        for t in targets:
            if pick.lower() in (str(t.get("group_id", "")).lower(), str(t.get("label", "")).lower()):
                chosen = t
                break
    if chosen is None:
        return f"⚠️ no group “{pick}”. Use /primary list"
    try:
        res = await predvd_automator.set_primary_group(chosen.get("group_id"), chosen.get("label") or "")
        ok = res.get("ok", True) if isinstance(res, dict) else True
        return ("⭐ " if ok else "⚠️ ") + f"primary = {chosen.get('label') or chosen.get('group_id')}"
    except Exception as exc:
        return f"⚠️ switch failed: {exc}"


async def _watch_cmd(arg: str, rest: str) -> str:
    from . import watchlist

    if arg == "add" and rest:
        res = await watchlist.add(rest)
    elif arg == "del" and rest:
        res = await watchlist.remove(rest)
    elif arg in ("list", ""):
        items = await watchlist.list_watch()
        if not items:
            return "👀 no active watches. Add one: /watch add Leo"
        lines = ["Watching:"]
        lines += [f"  • {w['title']}  (since {_fmt_age(w.get('created_at'))})" for w in items[:40]]
        res = {"ok": True, "detail": "\n".join(lines)}
    else:
        res = {"ok": False, "detail": "Usage: /watch add <title> | /watch del <title> | /watch list"}
    return ("✅ " if res.get("ok") else "⚠️ ") + str(res.get("detail") or res.get("ok"))


def install(client) -> None:
    """Attach command handlers to the userbot client (idempotent)."""
    global _installed
    if _installed or client is None:
        return
    try:
        from pyrogram import filters, handlers

        cmd_filter = filters.me & filters.command(
            ["stats", "heal", "feed", "primary", "watch", "start"], prefixes="/"
        )
        client.add_handler(handlers.MessageHandler(handle_command, cmd_filter))
        _installed = True
        LOGGER.info("[CMDS] userbot slash-command handlers installed")
    except Exception as exc:
        LOGGER.warning("[CMDS] command handlers not installed: %s", exc)
