# Synthetic commerce v1 data release

This immutable release contains the two fully materialized, independently validated large profiles produced by
`graphrag-data-factory-v3`. All records are fictional and explicitly marked Synthetic/Fake. These artifacts are for
development, integration, failure recovery, and evaluation; they are not production data or production-SLO evidence.

## Assets

| Profile | Records | Uncompressed bytes | Archive bytes | Manifest SHA-256 |
| --- | ---: | ---: | ---: | --- |
| `dev-standard` | 3,372,108 | 2,842,845,571 | 445,969,397 | `5bbea39339499703796fe5d9d01fdc8541f4414d11ae3b6f3ad65f30baef7fe2` |
| `failure-lab` | 1,380,226 | 1,155,538,493 | 197,895,809 | `2c64f88dc89625a4b44f504e62c7730cfe6caad003a365c19ac34f1b55d98a60` |

Archive SHA-256 values are provided in `SHA256SUMS`. The workflow refuses to publish if a regenerated Manifest or
archive differs from these pinned values.

## Download and validate

From the repository's `synthetic-data/` directory:

```bash
uv sync --frozen --group dev
uv run graphrag-data fetch-release --profile all
```

The command downloads pinned public assets, verifies archive size and SHA-256 before extraction, rejects unsafe tar
members, atomically installs each profile under `generated/synthetic-commerce-v1/`, and runs the full independent
dataset validator. Use `--profile dev-standard` or `--profile failure-lab` to download only one profile.
