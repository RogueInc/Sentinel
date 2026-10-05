"""sentinel/ingest/suppress.py — maintenance windows and manual mutes."""
from sentinel.db import fetchrow


async def suppressed(alert: dict) -> bool:
    labels = alert.get("labels", {})
    row = await fetchrow(
        """
        SELECT 1 FROM suppressions
        WHERE until > now()
          AND match @> $1::jsonb
        LIMIT 1
        """,
        __import__("json").dumps({"service": labels.get("service")}),
    )
    return row is not None