"""Apply sql/views/*.sql in filename order (SPEC §11). Idempotent: each file
drops and recreates its view.

Usage: python -m scripts.apply_views
"""
from pathlib import Path

from sqlalchemy import text

from core.db import engine

VIEWS_DIR = Path(__file__).resolve().parent.parent / "sql" / "views"


def main() -> None:
    files = sorted(VIEWS_DIR.glob("*.sql"))
    if not files:
        print(f"no .sql files found in {VIEWS_DIR}")
        return

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))
        for path in files:
            print(f"applying {path.name}...")
            conn.execute(text(path.read_text()))

    print(f"applied {len(files)} view file(s)")


if __name__ == "__main__":
    main()
