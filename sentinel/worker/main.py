"""sentinel/worker/main.py — entrypoint: `python -m sentinel.worker.main`"""
import asyncio
import logging
import sys

# psycopg's async mode (used by langgraph's AsyncPostgresSaver) cannot run
# under Windows' default ProactorEventLoop — it requires SelectorEventLoop.
# This must be set before any event loop is created, so it goes here,
# ahead of the asyncio.run() call, and only applies on Windows.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from sentinel.worker.runner import run_worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

if __name__ == "__main__":
    asyncio.run(run_worker())