"""The ntfy delivery edge: one POST, an error description or None back.

An ntfy topic URL is a write capability — anyone holding it can push to the
user's phone — so it never appears in logs, in the database, or in a return
value. Failures come back as the exception type name only, because a
stringified connection error embeds the host being connected to.
"""

import aiohttp

_TIMEOUT = aiohttp.ClientTimeout(total=10)


async def post_ntfy(url: str, title: str, message: str) -> str | None:
    """POST one notification; returns an error description, or None on
    success. Total: never raises, never returns the URL."""
    try:
        async with (
            aiohttp.ClientSession(timeout=_TIMEOUT) as session,
            session.post(url, data=message.encode(), headers={"Title": title}) as response,
        ):
            if response.status // 100 != 2:
                return f"ntfy returned HTTP {response.status}"
            return None
    except Exception as exc:  # noqa: BLE001 - delivery is best-effort by design
        return type(exc).__name__
