"""Same-chain NEAR + MCA direct to/from Lending (no DEX / no 1Click swap leg)."""

from __future__ import annotations

from typing import Any, Dict, Optional


def _addr_equal(a, b) -> bool:
    return str(a or "").strip() == str(b or "").strip()


def is_near_chain_id(chain: str) -> bool:
    return str(chain or "").strip().lower() in frozenset({"near", "near-mainnet"})


def near_same_chain_mca_applies(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """
    True when: NEAR↔NEAR same chain, same NEP-141 address, mca.flow in deposit|withdraw.

    Deposit/withdraw here means wallet <-> Lending on NEAR without bridging other chains.
    """
    if not mca or not isinstance(mca, dict):
        return False
    if not is_near_chain_id(from_chain) or not is_near_chain_id(to_chain):
        return False
    if str(from_chain).strip().lower() != str(to_chain).strip().lower():
        return False
    a = (token_in or {}).get("address")
    b = (token_out or {}).get("address")
    if not a or not _addr_equal(a, b):
        return False
    flow = str(mca.get("flow") or mca.get("mcaFlow") or "").strip().lower()
    return flow in ("deposit", "withdraw")


def resolve_near_mca_deposit_receiver(mca: Dict[str, Any], recipient: str) -> str:
    r = (
        mca.get("nearDepositReceiver")
        or mca.get("near_deposit_receiver")
        or mca.get("mcaAccountId")
        or mca.get("mca_id")
        or recipient
    )
    return str(r or "").strip()
