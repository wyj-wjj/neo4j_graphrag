"""Export the backend contract consumed by generated frontend types."""

from __future__ import annotations

import json
from pathlib import Path

from graphrag.api.app import create_app
from graphrag.config import AppEnvironment, Settings


def main() -> None:
    app = create_app(Settings(app_env=AppEnvironment.TEST, use_fake_external_clients=True))
    target = Path(__file__).resolve().parents[1] / "openapi.json"
    target.write_text(
        json.dumps(app.openapi(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
