from __future__ import annotations

import os
import sys


def _exec(cmd: list[str]) -> None:
    # Replace the current process (so signals work correctly on Railway)
    os.execvp(cmd[0], cmd)


def main() -> None:
    """
    Repo-root entrypoint for Railway monorepo builds.

    Some Railway builders (Railpack) try to detect how to build from the repo root.
    Our actual python app lives in ./backend, so we:
    - ensure ./backend is on PYTHONPATH
    - chdir into ./backend (so relative config like alembic.ini works)
    - exec the backend's entrypoint (backend/main.py)

    backend/main.py will then switch on SERVICE_ROLE:
      - SERVICE_ROLE=api    -> uvicorn app.main:app
      - SERVICE_ROLE=worker -> celery worker
    """

    repo_root = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.join(repo_root, "backend")
    if not os.path.isdir(backend_dir):
        raise RuntimeError(f"backend directory not found at {backend_dir}")

    # Prepend backend/ to PYTHONPATH so "import app" works when running under repo root.
    existing = os.getenv("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = backend_dir if not existing else f"{backend_dir}:{existing}"

    os.chdir(backend_dir)
    _exec(["python", "main.py"])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

