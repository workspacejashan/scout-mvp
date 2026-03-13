from __future__ import annotations

import os
import sys


def _exec(cmd: list[str]) -> None:
    # Replace the current process (so signals work correctly on Railway)
    os.execvp(cmd[0], cmd)


def main() -> None:
    """
    Railway (Railpack) needs an explicit entrypoint in the project root.
    We use one file for both services and switch behavior via SERVICE_ROLE:
      - SERVICE_ROLE=api    -> uvicorn app.main:app
      - SERVICE_ROLE=worker -> celery worker
    """
    role = (os.getenv("SERVICE_ROLE", "api") or "api").strip().lower()

    if role == "worker":
        # Use `python -m` to avoid relying on entrypoint scripts with brittle shebangs.
        _exec(["python", "-m", "celery", "-A", "app.worker.celery_app", "worker", "-l", "info"])

    port = str(os.getenv("PORT", "8000"))
    _exec(["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", port])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
