"""Versioned read-only contract for the Coordinate runtime data plane."""

from __future__ import annotations

from . import __version__

RUNTIME_CONTRACT_VERSION = 1

# These names describe wire behavior consumed by agentd clients.  They are
# capabilities rather than product features: the contract is deliberately
# small and does not become a second control plane.
RUNTIME_CAPABILITIES = {
    "claim_fencing": True,
    "agent_reconcile": True,
    "recoverable_claim": True,
    "managed_lease": True,
    "terminal_report": True,
}


def build_runtime_contract(*, transport: str) -> dict[str, object]:
    """Return the stable runtime contract advertised by this Coordinate build."""
    if transport not in {"cli", "http"}:
        raise ValueError("transport must be cli or http")
    capabilities = dict(RUNTIME_CAPABILITIES)
    if transport == "http":
        # HTTP intentionally exposes normal claims and agent reconciliation;
        # operator-only recoverable claims remain CLI/SSH operations.
        capabilities["recoverable_claim"] = False
    return {
        "contract_version": RUNTIME_CONTRACT_VERSION,
        "coordinate_version": __version__,
        "transport": transport,
        "capabilities": capabilities,
    }
