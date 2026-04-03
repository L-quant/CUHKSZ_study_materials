from __future__ import annotations

from pathlib import Path

from src.pipeline import run_pipeline


def main() -> None:
    project_root = Path(__file__).resolve().parent
    run_pipeline(project_root)


if __name__ == "__main__":
    main()
