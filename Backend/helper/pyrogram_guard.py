import asyncio
from functools import wraps

_GUARD_MARKER = "__telegram_stremio_restart_guard__"
_LOCK_ATTRIBUTE = "_telegram_stremio_restart_lock"


def install_session_restart_guard(session_class: type) -> bool:
    """Serialize ``Session.restart()`` for each Pyrogram session instance.

    Pyrofork can schedule a restart from both its ping worker and receive worker
    when one transport fails. Its unguarded restart mutates ``connection`` and
    starts a new receive worker, so overlapping restarts can make two workers
    read the same ``asyncio.StreamReader``. Install this once, before clients
    start, to keep separate sessions concurrent while preventing overlap within
    one session.

    Returns ``True`` when the class was patched and ``False`` when the guard was
    already installed.
    """
    current_restart = session_class.restart
    if getattr(current_restart, _GUARD_MARKER, False):
        return False

    @wraps(current_restart)
    async def guarded_restart(self, *args, **kwargs):
        restart_lock = getattr(self, _LOCK_ATTRIBUTE, None)
        if restart_lock is None:
            # There is no await between lookup and assignment, so event-loop
            # tasks cannot race while lazily creating this per-instance lock.
            restart_lock = asyncio.Lock()
            setattr(self, _LOCK_ATTRIBUTE, restart_lock)

        async with restart_lock:
            return await current_restart(self, *args, **kwargs)

    setattr(guarded_restart, _GUARD_MARKER, True)
    session_class.restart = guarded_restart
    return True
