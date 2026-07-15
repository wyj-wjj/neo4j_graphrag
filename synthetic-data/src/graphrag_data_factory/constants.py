"""Version and safety constants for the synthetic data contract."""

from __future__ import annotations

from typing import Final

SPEC_VERSION: Final = "phase2-data-spec-v1"
DATASET_VERSION: Final = "synthetic-commerce-v1"
GENERATOR_VERSION: Final = "graphrag-data-factory-v3"
SUPPORTED_GENERATOR_VERSIONS: Final = frozenset({"graphrag-data-factory-v2", GENERATOR_VERSION})
SCHEMA_VERSION: Final = "synthetic-record-v1"
RULES_VERSION: Final = "synthetic-commerce-rules-v2"
TEMPLATE_VERSION: Final = "synthetic-templates-v2"
ORACLE_VERSION: Final = "synthetic-oracle-v2"
PROFILE_VERSION: Final = "profile-v1"
DATASET_DOMAIN: Final = "commerce"
SOURCE: Final = "fake"

SUPPORTED_PROFILES: Final = frozenset({"ci-small", "dev-standard", "failure-lab", "staging-large"})
RESERVED_TENANT_PREFIX: Final = "synthetic-"

COMPATIBILITY: Final[dict[str, str]] = {
    "application": "neo4j-graphrag-agent>=0.1,<0.2",
    "event_envelope": "event-envelope-v1",
    "tool_schema": "tool-schema-v1",
    "agent_state": "agent-state-v2",
    "prompt_bundle": "prompt-bundle-v1",
    "context_policy": "context-policy-v1",
}
