from __future__ import annotations

import unittest

from coordinate.runtime_contract import (
    RUNTIME_CAPABILITIES,
    RUNTIME_CONTRACT_VERSION,
    build_runtime_contract,
)


class RuntimeContractTests(unittest.TestCase):
    def test_cli_contract_advertises_claim_recovery_surface(self):
        contract = build_runtime_contract(transport="cli")
        self.assertEqual(contract["contract_version"], RUNTIME_CONTRACT_VERSION)
        self.assertEqual(contract["transport"], "cli")
        self.assertEqual(contract["capabilities"], RUNTIME_CAPABILITIES)

    def test_http_contract_excludes_operator_only_recovery(self):
        contract = build_runtime_contract(transport="http")
        self.assertEqual(contract["contract_version"], RUNTIME_CONTRACT_VERSION)
        self.assertFalse(contract["capabilities"]["recoverable_claim"])
        for name in (
            "claim_fencing",
            "agent_reconcile",
            "managed_lease",
            "terminal_report",
        ):
            self.assertTrue(contract["capabilities"][name])

    def test_unknown_transport_fails_closed(self):
        with self.assertRaises(ValueError):
            build_runtime_contract(transport="legacy")
