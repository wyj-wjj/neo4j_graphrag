"""Create a secret-bearing local development .env without printing secrets."""

from __future__ import annotations

import argparse
import secrets
from pathlib import Path


def _secret(bytes_count: int = 24) -> str:
    return secrets.token_hex(bytes_count)


def _render(template: str) -> str:
    mysql_app_password = _secret()
    redis_password = _secret()
    object_store_access_key = f"local{secrets.token_hex(8)}"
    object_store_secret_key = _secret()
    replacements = {
        "USE_FAKE_EXTERNAL_CLIENTS": "false",
        "JWT_DEV_SECRET": _secret(32),
        "DATABASE_URL": (
            f"mysql+aiomysql://app_user:{mysql_app_password}@127.0.0.1:3306/enterprise_agent_db"
        ),
        "MYSQL_APP_PASSWORD": mysql_app_password,
        "MYSQL_ROOT_PASSWORD": _secret(),
        "REDIS_PASSWORD": redis_password,
        "MINIO_ROOT_USER": f"milvus{secrets.token_hex(6)}",
        "MINIO_ROOT_PASSWORD": _secret(),
        "OBJECT_STORE_ACCESS_KEY": object_store_access_key,
        "OBJECT_STORE_SECRET_KEY": object_store_secret_key,
        "REDIS_URL": f"redis://:{redis_password}@127.0.0.1:6379/0",
        "NEO4J_PASSWORD": _secret(),
        "KAFKA_ENABLED": "true",
        "OUTBOX_RELAY_ENABLED": "true",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
        "OBJECT_STORE_BACKEND": "s3",
        "S3_ENDPOINT_URL": "http://127.0.0.1:9002",
        "S3_BUCKET": "knowledge-originals",
        "S3_ACCESS_KEY_ID": object_store_access_key,
        "S3_SECRET_ACCESS_KEY": object_store_secret_key,
        "S3_FORCE_PATH_STYLE": "true",
        "S3_VERIFY_TLS": "false",
    }
    rendered: list[str] = []
    seen: set[str] = set()
    for line in template.splitlines():
        key, separator, _value = line.partition("=")
        if separator and key in replacements:
            rendered.append(f"{key}={replacements[key]}")
            seen.add(key)
        else:
            rendered.append(line)
    missing = sorted(replacements.keys() - seen)
    if missing:
        raise RuntimeError(f"environment template is missing keys: {', '.join(missing)}")
    return "\n".join(rendered) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a local phase-two .env with generated development secrets"
    )
    parser.add_argument("--output", type=Path, default=Path(".env"))
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    template_path = root / ".env.example"
    output = args.output if args.output.is_absolute() else root / args.output
    if output.exists():
        raise SystemExit(f"refusing to overwrite existing environment file: {output}")
    rendered = _render(template_path.read_text(encoding="utf-8"))
    output.write_text(rendered, encoding="utf-8")
    output.chmod(0o600)
    print(f"created {output}")
    print("next: set LLM_API_KEY in that file; generated secrets were not printed")


if __name__ == "__main__":
    main()
