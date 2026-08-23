"""Unit tests for the typed usage_evidence V1 contract (issue #12)."""
from __future__ import annotations

import unittest

from coordinate.usage_evidence import (
    CONTRACT_VERSION,
    MAX_COST_MICROUSD,
    MAX_RECORDS,
    MAX_TOKEN_VALUE,
    UsageEvidenceError,
    parse_usage_evidence,
    split_usage_evidence,
)


def record(**overrides):
    base = {
        "provider": "qoder",
        "model": "lite",
        "input_tokens": 100,
        "output_tokens": 50,
        "cache_read_tokens": 10,
        "cache_write_tokens": 0,
        "provider_cost_microusd": 1234,
        "source": "provider_reported",
        "completeness": "complete",
    }
    base.update(overrides)
    return base


def block(records=None, **overrides):
    value = {"contract_version": CONTRACT_VERSION, "records": records or [record()]}
    value.update(overrides)
    return value


class MissingEvidenceTests(unittest.TestCase):
    def test_absent_key_is_missing(self):
        self.assertIsNone(parse_usage_evidence(None))

    def test_explicit_null_is_missing(self):
        self.assertIsNone(parse_usage_evidence(None))

    def test_empty_object_is_missing(self):
        self.assertIsNone(parse_usage_evidence({}))

    def test_empty_records_is_missing(self):
        self.assertIsNone(parse_usage_evidence({"contract_version": 1, "records": []}))

    def test_contentful_object_missing_records_fails_closed(self):
        with self.assertRaisesRegex(UsageEvidenceError, "missing field: 'records'"):
            parse_usage_evidence({"contract_version": 1})

    def test_unknown_content_without_records_fails_closed(self):
        with self.assertRaises(UsageEvidenceError):
            parse_usage_evidence({"records_missing": True})

    def test_split_missing_returns_identity(self):
        result = {"response_text": "hi", "usage_evidence": None}
        sanitized, evidence = split_usage_evidence(result)
        self.assertIs(sanitized, result)
        self.assertIsNone(evidence)

    def test_split_no_key_returns_identity(self):
        result = {"response_text": "hi"}
        sanitized, evidence = split_usage_evidence(result)
        self.assertIs(sanitized, result)
        self.assertIsNone(evidence)


class InvalidEvidenceTests(unittest.TestCase):
    def test_non_object_block(self):
        with self.assertRaises(UsageEvidenceError):
            parse_usage_evidence("usage")

    def test_unsupported_contract_version(self):
        with self.assertRaisesRegex(UsageEvidenceError, "contract_version"):
            parse_usage_evidence(block(contract_version=2))
        with self.assertRaisesRegex(UsageEvidenceError, "contract_version"):
            parse_usage_evidence(block(contract_version="1"))
        with self.assertRaisesRegex(UsageEvidenceError, "contract_version"):
            parse_usage_evidence(block(contract_version=True))

    def test_unknown_block_fields(self):
        with self.assertRaisesRegex(UsageEvidenceError, "unknown fields"):
            parse_usage_evidence(block(extra_field=1))

    def test_records_not_a_list(self):
        with self.assertRaisesRegex(UsageEvidenceError, "records must be a list"):
            parse_usage_evidence(block(records={"provider": "qoder"}))

    def test_more_than_eight_records(self):
        records = [record(provider=f"p{i}") for i in range(MAX_RECORDS + 1)]
        with self.assertRaisesRegex(UsageEvidenceError, "1..8"):
            parse_usage_evidence(block(records=records))

    def test_duplicate_provider_model(self):
        with self.assertRaisesRegex(UsageEvidenceError, "duplicate"):
            parse_usage_evidence(block(records=[record(), record(provider="qoder", model="lite")]))
        with self.assertRaisesRegex(UsageEvidenceError, "duplicate"):
            parse_usage_evidence(
                block(records=[record(model=None), record(provider="qoder", model=None)])
            )

    def test_record_not_an_object(self):
        with self.assertRaisesRegex(UsageEvidenceError, "record 0 must be an object"):
            parse_usage_evidence(block(records=["qoder"]))

    def test_missing_record_field(self):
        bad = record()
        del bad["output_tokens"]
        with self.assertRaisesRegex(UsageEvidenceError, "missing fields"):
            parse_usage_evidence(block(records=[bad]))

    def test_unknown_record_field(self):
        with self.assertRaisesRegex(UsageEvidenceError, "unknown fields"):
            parse_usage_evidence(block(records=[record(extra=1)]))

    def test_negative_numeric(self):
        with self.assertRaisesRegex(UsageEvidenceError, "non-negative"):
            parse_usage_evidence(block(records=[record(input_tokens=-1)]))

    def test_float_numeric(self):
        with self.assertRaisesRegex(UsageEvidenceError, "integer"):
            parse_usage_evidence(block(records=[record(input_tokens=1.5)]))

    def test_bool_numeric(self):
        with self.assertRaisesRegex(UsageEvidenceError, "integer"):
            parse_usage_evidence(block(records=[record(input_tokens=True)]))

    def test_token_upper_bound(self):
        ok = parse_usage_evidence(block(records=[record(input_tokens=MAX_TOKEN_VALUE)]))
        self.assertEqual(ok.records[0].input_tokens, MAX_TOKEN_VALUE)
        with self.assertRaisesRegex(UsageEvidenceError, "exceeds limit"):
            parse_usage_evidence(block(records=[record(input_tokens=MAX_TOKEN_VALUE + 1)]))

    def test_cost_upper_bound(self):
        ok = parse_usage_evidence(
            block(records=[record(provider_cost_microusd=MAX_COST_MICROUSD)])
        )
        self.assertEqual(ok.records[0].provider_cost_microusd, MAX_COST_MICROUSD)
        with self.assertRaisesRegex(UsageEvidenceError, "exceeds limit"):
            parse_usage_evidence(
                block(records=[record(provider_cost_microusd=MAX_COST_MICROUSD + 1)])
            )

    def test_aggregate_cost_upper_bound(self):
        with self.assertRaisesRegex(UsageEvidenceError, "aggregate.*signed 64-bit"):
            parse_usage_evidence(
                block(
                    records=[
                        record(provider="qoder", provider_cost_microusd=MAX_COST_MICROUSD),
                        record(provider="grok", provider_cost_microusd=1),
                    ]
                )
            )

    def test_invalid_source(self):
        with self.assertRaisesRegex(UsageEvidenceError, "source"):
            parse_usage_evidence(block(records=[record(source="guessed")]))
        with self.assertRaisesRegex(UsageEvidenceError, "source"):
            parse_usage_evidence(block(records=[record(source=[])]))

    def test_invalid_completeness_label(self):
        with self.assertRaisesRegex(UsageEvidenceError, "completeness"):
            parse_usage_evidence(block(records=[record(completeness="mostly")]))
        with self.assertRaisesRegex(UsageEvidenceError, "completeness"):
            parse_usage_evidence(block(records=[record(completeness=[])]))

    def test_source_unknown_with_numbers(self):
        with self.assertRaisesRegex(UsageEvidenceError, "source=unknown"):
            parse_usage_evidence(
                block(
                    records=[
                        record(
                            source="unknown",
                            input_tokens=1,
                            output_tokens=None,
                            cache_read_tokens=None,
                            cache_write_tokens=None,
                            provider_cost_microusd=None,
                            completeness="partial",
                        )
                    ]
                )
            )

    def test_declared_completeness_mismatch(self):
        # Complete numbers but declared partial.
        with self.assertRaisesRegex(UsageEvidenceError, "derives"):
            parse_usage_evidence(block(records=[record(completeness="partial")]))
        # All-null numbers but declared complete (source not unknown).
        with self.assertRaisesRegex(UsageEvidenceError, "derives"):
            parse_usage_evidence(
                block(
                    records=[
                        record(
                            input_tokens=None,
                            output_tokens=None,
                            cache_read_tokens=None,
                            cache_write_tokens=None,
                            provider_cost_microusd=None,
                            completeness="complete",
                        )
                    ]
                )
            )

    def test_empty_model_string_rejected(self):
        with self.assertRaisesRegex(UsageEvidenceError, "model"):
            parse_usage_evidence(block(records=[record(model="")]))

    def test_blank_provider_rejected(self):
        with self.assertRaisesRegex(UsageEvidenceError, "provider"):
            parse_usage_evidence(block(records=[record(provider="  ")]))


class ValidEvidenceTests(unittest.TestCase):
    def test_complete_round_trip(self):
        evidence = parse_usage_evidence(block())
        self.assertEqual(evidence.contract_version, 1)
        self.assertEqual(len(evidence.records), 1)
        self.assertEqual(evidence.observed_tokens, 160)
        self.assertEqual(evidence.provider_cost_microusd, 1234)
        self.assertEqual(evidence.completeness, "complete")
        self.assertEqual(len(evidence.digest), 64)

    def test_partial_derivation(self):
        evidence = parse_usage_evidence(
            block(records=[record(input_tokens=None, completeness="partial")])
        )
        self.assertEqual(evidence.completeness, "partial")
        self.assertEqual(evidence.observed_tokens, 60)
        self.assertEqual(evidence.provider_cost_microusd, 1234)

    def test_unknown_derivation(self):
        evidence = parse_usage_evidence(
            block(
                records=[
                    record(
                        input_tokens=None,
                        output_tokens=None,
                        cache_read_tokens=None,
                        cache_write_tokens=None,
                        provider_cost_microusd=None,
                        source="unknown",
                        completeness="unknown",
                    )
                ]
            )
        )
        self.assertEqual(evidence.completeness, "unknown")
        self.assertIsNone(evidence.observed_tokens)
        self.assertIsNone(evidence.provider_cost_microusd)

    def test_estimated_is_honest_enum(self):
        evidence = parse_usage_evidence(
            block(records=[record(source="estimated", completeness="complete")])
        )
        self.assertEqual(evidence.records[0].source, "estimated")

    def test_null_model_canonical_sort_key(self):
        evidence = parse_usage_evidence(
            block(
                records=[
                    record(provider="grok", model=None, completeness="complete"),
                    record(provider="grok", model="kimi", completeness="complete"),
                    record(provider="qoder", model=None, completeness="complete"),
                ]
            )
        )
        keys = [r.unique_key for r in evidence.records]
        self.assertEqual(
            keys,
            [("grok", ""), ("grok", "kimi"), ("qoder", "")],
        )

    def test_records_sorted_canonically(self):
        evidence = parse_usage_evidence(
            block(
                records=[
                    record(provider="zcode", model="m", completeness="complete"),
                    record(provider="qoder", model="b", completeness="complete"),
                    record(provider="qoder", model="a", completeness="complete"),
                ]
            )
        )
        self.assertEqual(
            [r.unique_key for r in evidence.records],
            [("qoder", "a"), ("qoder", "b"), ("zcode", "m")],
        )

    def test_aggregate_cost_null_when_any_record_cost_null(self):
        evidence = parse_usage_evidence(
            block(
                records=[
                    record(provider="qoder", completeness="complete"),
                    record(
                        provider="grok",
                        model=None,
                        input_tokens=1,
                        output_tokens=1,
                        cache_read_tokens=1,
                        cache_write_tokens=1,
                        provider_cost_microusd=None,
                        source="provider_reported",
                        completeness="partial",
                    ),
                ]
            )
        )
        self.assertIsNone(evidence.provider_cost_microusd)
        self.assertEqual(evidence.observed_tokens, 164)
        self.assertEqual(evidence.completeness, "partial")

    def test_aggregate_completeness_all_unknown(self):
        unknown = {
            "input_tokens": None,
            "output_tokens": None,
            "cache_read_tokens": None,
            "cache_write_tokens": None,
            "provider_cost_microusd": None,
            "source": "unknown",
            "completeness": "unknown",
        }
        evidence = parse_usage_evidence(
            block(records=[record(provider="a", **unknown), record(provider="b", **unknown)])
        )
        self.assertEqual(evidence.completeness, "unknown")

    def test_aggregate_completeness_mixed(self):
        evidence = parse_usage_evidence(
            block(
                records=[
                    record(provider="a", completeness="complete"),
                    record(
                        provider="b",
                        input_tokens=None,
                        completeness="partial",
                    ),
                ]
            )
        )
        self.assertEqual(evidence.completeness, "partial")

    def test_digest_deterministic_and_order_insensitive(self):
        first = parse_usage_evidence(
            block(
                records=[
                    record(provider="b", completeness="complete"),
                    record(provider="a", completeness="complete"),
                ]
            )
        )
        second = parse_usage_evidence(
            block(
                records=[
                    record(provider="a", completeness="complete"),
                    record(provider="b", completeness="complete"),
                ]
            )
        )
        self.assertEqual(first.digest, second.digest)
        self.assertEqual(first.canonical_dict, second.canonical_dict)

    def test_canonical_dict_stored_shape(self):
        evidence = parse_usage_evidence(block())
        canonical = evidence.canonical_dict
        self.assertEqual(set(canonical), {"contract_version", "records"})
        self.assertEqual(
            set(canonical["records"][0]),
            {
                "provider",
                "model",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "provider_cost_microusd",
                "source",
                "completeness",
            },
        )


class SplitEvidenceTests(unittest.TestCase):
    def test_split_replaces_full_evidence_with_bounded_summary(self):
        result = {"response_text": "hi", "usage_evidence": block()}
        sanitized, evidence = split_usage_evidence(result)
        self.assertIsNot(sanitized, result)
        summary = sanitized["usage_evidence"]
        self.assertEqual(
            set(summary),
            {
                "contract_version",
                "digest",
                "completeness",
                "observed_tokens",
                "provider_cost_microusd",
                "record_count",
                "records",
            },
        )
        # Numeric buckets must never leak into the summary.
        record_summary = summary["records"][0]
        self.assertEqual(
            set(record_summary), {"provider", "model", "source", "completeness"}
        )
        self.assertEqual(summary["digest"], evidence.digest)
        self.assertEqual(summary["observed_tokens"], 160)

    def test_split_invalid_raises(self):
        with self.assertRaises(UsageEvidenceError):
            split_usage_evidence({"usage_evidence": {"contract_version": 2, "records": [record()]}})

    def test_split_original_dict_untouched(self):
        result = {"usage_evidence": block()}
        split_usage_evidence(result)
        # The caller's dict is not mutated in place.
        self.assertIn("records", result["usage_evidence"])


if __name__ == "__main__":
    unittest.main()
