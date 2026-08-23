"""Typed managed ``usage_evidence`` V1 contract — parse, validate, canonicalize.

Coordinate authority layer (issue #12). MultiNexus normalizes provider-reported
usage facts into this bounded contract; Coordinate strictly validates it,
canonicalizes it, and never lets a raw provider payload through.

Contract V1 shape (canonical, stored verbatim in ``job_attempt_usage``):

    {
      "contract_version": 1,
      "records": [
        {
          "provider": "qoder",
          "model": "lite",                # non-null string or null
          "input_tokens": 0,              # non-negative int or null (unknown)
          "output_tokens": 0,
          "cache_read_tokens": 0,
          "cache_write_tokens": 0,
          "provider_cost_microusd": 0,    # non-negative int or null
          "source": "provider_reported",  # provider_reported | estimated | unknown
          "completeness": "complete"      # complete | partial | unknown
        }
      ]
    }

Validation rules (authoritative here, mirrors plan §4):

- ``records`` allows 1..8 entries, unique on ``(provider, model or "")`` and
  canonical-sorted by that key; ``model`` may be null (sort key "").
- A missing ``usage_evidence`` key, explicit ``null``, an empty object ``{}``
  or an empty ``records`` list is MISSING evidence (backward compatible: no
  usage row, no failure). Any contentful block must be fully valid or the
  terminal report fails closed.
- The five numeric fields are non-negative integers or ``null``; ``null`` means
  unknown and is never replaced with 0. Token fields are capped at ``2**53-1``,
  ``provider_cost_microusd`` at signed 64-bit; out-of-range values are rejected,
  never truncated.
- ``source=unknown`` requires every numeric field to be ``null``.
- ``completeness`` is NOT a free producer label. Coordinate re-derives it from
  the null pattern and requires an exact match:
    * all five numeric fields non-null            -> ``complete``
    * ``source=unknown`` and all five null        -> ``unknown``
    * any other legal combination                 -> ``partial``
- Unknown fields anywhere are rejected (strict contract).

Aggregation (attempt level, derived by Coordinate):

- ``observed_tokens`` = sum of all non-null input/output/cache buckets, or
  ``None`` when every bucket is null (nothing observed; never 0 on unknown).
- ``provider_cost_microusd`` = sum only when EVERY record provides a cost,
  else ``None`` (partial cost is never presented as total); a known aggregate
  must also fit the signed 64-bit storage bound.
- ``completeness`` = ``complete`` when all records are complete, ``unknown``
  when all records are unknown, otherwise ``partial``.

``split_usage_evidence`` is the single entry point the runtime calls before any
terminal state branch: it validates, then strips the full evidence out of the
submitted result and replaces it with a bounded digest/summary so running,
replay and late-result spreads never duplicate the full evidence.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

CONTRACT_VERSION = 1
MAX_RECORDS = 8

MAX_TOKEN_VALUE = 2**53 - 1
MAX_COST_MICROUSD = 2**63 - 1

SOURCES = frozenset({"provider_reported", "estimated", "unknown"})
COMPLETENESS = frozenset({"complete", "partial", "unknown"})

NUMERIC_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "provider_cost_microusd",
)
_TOKEN_FIELDS = frozenset(
    {"input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens"}
)

_BLOCK_FIELDS = frozenset({"contract_version", "records"})
_RECORD_FIELDS = frozenset(
    {
        "provider",
        "model",
        *NUMERIC_FIELDS,
        "source",
        "completeness",
    }
)

_MAX_PROVIDER_LEN = 128
_MAX_MODEL_LEN = 256


class UsageEvidenceError(ValueError):
    """Raised when a contentful ``usage_evidence`` block violates the V1 contract.

    The runtime treats this as a fail-closed terminal report: no job mutation,
    no lease release, no terminal event, no partial usage write.
    """


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _validate_provider(value: Any) -> str:
    if not isinstance(value, str) or isinstance(value, bool):
        raise UsageEvidenceError("record provider must be a string")
    if value != value.strip() or not value:
        raise UsageEvidenceError("record provider must be non-blank without surrounding whitespace")
    if len(value) > _MAX_PROVIDER_LEN:
        raise UsageEvidenceError(f"record provider exceeds {_MAX_PROVIDER_LEN} characters")
    if any(ord(ch) < 32 for ch in value):
        raise UsageEvidenceError("record provider contains control characters")
    return value


def _validate_model(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or isinstance(value, bool):
        raise UsageEvidenceError("record model must be a string or null")
    if value != value.strip() or not value:
        raise UsageEvidenceError("record model must be a non-empty string or null")
    if len(value) > _MAX_MODEL_LEN:
        raise UsageEvidenceError(f"record model exceeds {_MAX_MODEL_LEN} characters")
    if any(ord(ch) < 32 for ch in value):
        raise UsageEvidenceError("record model contains control characters")
    return value


def _validate_numeric(value: Any, field: str, index: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise UsageEvidenceError(
            f"record {index} field {field!r} must be a non-negative integer or null"
        )
    if value < 0:
        raise UsageEvidenceError(
            f"record {index} field {field!r} must be non-negative, got {value}"
        )
    limit = MAX_TOKEN_VALUE if field in _TOKEN_FIELDS else MAX_COST_MICROUSD
    if value > limit:
        raise UsageEvidenceError(
            f"record {index} field {field!r} exceeds limit {limit}; "
            "producer must degrade, Coordinate never truncates"
        )
    return value


def _derive_completeness(
    *, source: str, numerics: dict[str, int | None]
) -> str:
    """Coordinate's authoritative completeness derivation from the null pattern."""
    all_known = all(value is not None for value in numerics.values())
    all_unknown = all(value is None for value in numerics.values())
    if all_known:
        return "complete"
    if source == "unknown" and all_unknown:
        return "unknown"
    return "partial"


def _parse_record(index: int, raw: Any) -> "UsageRecord":
    if not isinstance(raw, dict):
        raise UsageEvidenceError(f"record {index} must be an object")
    extra = set(raw) - _RECORD_FIELDS
    if extra:
        raise UsageEvidenceError(f"record {index} has unknown fields: {sorted(extra)}")
    missing = _RECORD_FIELDS - set(raw)
    if missing:
        raise UsageEvidenceError(f"record {index} is missing fields: {sorted(missing)}")

    provider = _validate_provider(raw["provider"])
    model = _validate_model(raw["model"])
    numerics: dict[str, int | None] = {
        field: _validate_numeric(raw[field], field, index) for field in NUMERIC_FIELDS
    }
    source = raw["source"]
    if not isinstance(source, str) or source not in SOURCES:
        raise UsageEvidenceError(
            f"record {index} source must be one of {sorted(SOURCES)}, got {source!r}"
        )
    if source == "unknown" and any(value is not None for value in numerics.values()):
        raise UsageEvidenceError(
            f"record {index} source=unknown requires all numeric fields to be null"
        )
    declared = raw["completeness"]
    if not isinstance(declared, str) or declared not in COMPLETENESS:
        raise UsageEvidenceError(
            f"record {index} completeness must be one of {sorted(COMPLETENESS)}, got {declared!r}"
        )
    derived = _derive_completeness(source=source, numerics=numerics)
    if declared != derived:
        raise UsageEvidenceError(
            f"record {index} declares completeness={declared!r} but the null pattern "
            f"derives {derived!r}"
        )
    return UsageRecord(
        provider=provider,
        model=model,
        input_tokens=numerics["input_tokens"],
        output_tokens=numerics["output_tokens"],
        cache_read_tokens=numerics["cache_read_tokens"],
        cache_write_tokens=numerics["cache_write_tokens"],
        provider_cost_microusd=numerics["provider_cost_microusd"],
        source=source,
        completeness=declared,
    )


@dataclass(frozen=True)
class UsageRecord:
    provider: str
    model: str | None
    input_tokens: int | None
    output_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    provider_cost_microusd: int | None
    source: str
    completeness: str

    @property
    def unique_key(self) -> tuple[str, str]:
        """Canonical uniqueness/sort key: ``(provider, model or "")``."""
        return (self.provider, self.model or "")

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "provider_cost_microusd": self.provider_cost_microusd,
            "source": self.source,
            "completeness": self.completeness,
        }

    def to_summary_dict(self) -> dict[str, Any]:
        """Bounded per-record summary (no numeric buckets) for result/event payloads."""
        return {
            "provider": self.provider,
            "model": self.model,
            "source": self.source,
            "completeness": self.completeness,
        }


@dataclass(frozen=True)
class UsageEvidence:
    """Canonical, validated V1 usage evidence for one attempt.

    ``digest`` is sha256 of the canonical JSON; the canonical block is
    re-derivable from ``records`` so it is not stored as a mutable field.
    """

    contract_version: int
    records: tuple[UsageRecord, ...]
    observed_tokens: int | None
    provider_cost_microusd: int | None
    completeness: str
    digest: str

    @property
    def canonical_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "records": [record.to_dict() for record in self.records],
        }

    def to_summary_dict(self) -> dict[str, Any]:
        """Bounded locator/summary stored in ``result_json`` and event payloads.

        Never replicates the full evidence: only the digest, aggregate values
        and per-record provider/model/source/completeness labels.
        """
        return {
            "contract_version": self.contract_version,
            "digest": self.digest,
            "completeness": self.completeness,
            "observed_tokens": self.observed_tokens,
            "provider_cost_microusd": self.provider_cost_microusd,
            "record_count": len(self.records),
            "records": [record.to_summary_dict() for record in self.records],
        }


def _aggregate(records: tuple[UsageRecord, ...]) -> tuple[int | None, int | None, str]:
    token_sum = 0
    has_token = False
    cost_sum = 0
    all_cost_known = True
    for record in records:
        for field in _TOKEN_FIELDS:
            value = getattr(record, field)
            if value is not None:
                has_token = True
                token_sum += value
        if record.provider_cost_microusd is None:
            all_cost_known = False
        else:
            cost_sum += record.provider_cost_microusd
            if cost_sum > MAX_COST_MICROUSD:
                raise UsageEvidenceError(
                    "aggregate provider_cost_microusd exceeds signed 64-bit limit; "
                    "producer must degrade one or more costs to null"
                )
    observed = token_sum if has_token else None
    cost = cost_sum if all_cost_known else None
    if all(record.completeness == "complete" for record in records):
        completeness = "complete"
    elif all(record.completeness == "unknown" for record in records):
        completeness = "unknown"
    else:
        completeness = "partial"
    return observed, cost, completeness


def parse_usage_evidence(raw: Any) -> UsageEvidence | None:
    """Parse and validate one ``usage_evidence`` block.

    Returns ``None`` for missing evidence (absent key, explicit null, empty
    object, or empty ``records``). Raises ``UsageEvidenceError`` for any
    contentful block that violates the V1 contract.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise UsageEvidenceError("usage_evidence must be an object")
    if not raw:
        return None

    # Compatibility treats only the exact empty-records form as missing.
    # Any other contentful object must pass the strict contract below; an
    # unknown field cannot silently erase evidence merely because ``records``
    # is absent.
    if raw.get("records") == [] and set(raw) <= _BLOCK_FIELDS:
        return None

    contract_version = raw.get("contract_version")
    if (
        isinstance(contract_version, bool)
        or not isinstance(contract_version, int)
        or contract_version != CONTRACT_VERSION
    ):
        raise UsageEvidenceError(
            f"unsupported usage_evidence contract_version: {contract_version!r}"
        )
    extra = set(raw) - _BLOCK_FIELDS
    if extra:
        raise UsageEvidenceError(f"usage_evidence has unknown fields: {sorted(extra)}")

    if "records" not in raw:
        raise UsageEvidenceError("usage_evidence is missing field: 'records'")
    records_raw = raw["records"]
    if not isinstance(records_raw, list):
        raise UsageEvidenceError("usage_evidence.records must be a list")
    if not 1 <= len(records_raw) <= MAX_RECORDS:
        raise UsageEvidenceError(
            f"usage_evidence.records must contain 1..{MAX_RECORDS} records, got {len(records_raw)}"
        )

    records = tuple(_parse_record(index, entry) for index, entry in enumerate(records_raw))
    seen: set[tuple[str, str]] = set()
    for record in records:
        key = record.unique_key
        if key in seen:
            raise UsageEvidenceError(
                f"duplicate (provider, model) record: {key[0]!r}/{key[1]!r}"
            )
        seen.add(key)
    records = tuple(sorted(records, key=lambda record: record.unique_key))

    observed, cost, completeness = _aggregate(records)
    canonical = {
        "contract_version": CONTRACT_VERSION,
        "records": [record.to_dict() for record in records],
    }
    digest = hashlib.sha256(_json_dumps(canonical).encode("utf-8")).hexdigest()
    return UsageEvidence(
        contract_version=CONTRACT_VERSION,
        records=records,
        observed_tokens=observed,
        provider_cost_microusd=cost,
        completeness=completeness,
        digest=digest,
    )


def split_usage_evidence(
    result: dict[str, Any],
) -> tuple[dict[str, Any], UsageEvidence | None]:
    """Centralized terminal-report split: validate, then strip full evidence.

    Called before ANY terminal state branch. Returns ``(sanitized_result,
    evidence)`` where ``sanitized_result`` replaces ``usage_evidence`` with the
    bounded summary; with no evidence the input dict is returned unchanged so
    missing-evidence behavior stays byte-identical. Invalid evidence raises
    ``UsageEvidenceError`` BEFORE any write, lease release or event.
    """
    raw = result.get("usage_evidence")
    evidence = parse_usage_evidence(raw)
    if evidence is None:
        return result, None
    sanitized = dict(result)
    sanitized["usage_evidence"] = evidence.to_summary_dict()
    return sanitized, evidence
