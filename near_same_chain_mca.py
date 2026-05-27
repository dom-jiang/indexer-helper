"""Same-chain NEAR + MCA: deposit via Near Intents (1Click); withdraw direct to MCA."""

from __future__ import annotations

from typing import Any, Dict, Optional


def _addr_equal(a, b) -> bool:
    return str(a or "").strip() == str(b or "").strip()


def is_near_chain_id(chain: str) -> bool:
    return str(chain or "").strip().lower() in frozenset({"near", "near-mainnet"})


def _near_same_chain_mca_base(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """NEAR↔NEAR, same NEP-141, with an mca block (flow checked by callers)."""
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
    return True


def near_same_chain_mca_deposit_intents_applies(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """
    True when same-chain NEAR wallet → Lending deposit should use 1Click (+ optional Ref preswap).

    Replaces the legacy ``near-mca-deposit`` direct ``ft_transfer_call`` to MCA.

    ``tokenIn`` and ``tokenOut`` may differ (e.g. FLX → USDT): Stage A Ref/SmartX swap must
    deliver to 1Click ``depositAddress`` (``swap_out_recipient``), not ``mcaAccountId``.
    """
    if not mca or not isinstance(mca, dict):
        return False
    if not is_near_chain_id(from_chain) or not is_near_chain_id(to_chain):
        return False
    if str(from_chain).strip().lower() != str(to_chain).strip().lower():
        return False
    flow = str(mca.get("flow") or mca.get("mcaFlow") or "").strip().lower()
    if flow != "deposit":
        return False
    if not (token_in or {}).get("address") or not (token_out or {}).get("address"):
        return False
    return True


def near_same_chain_mca_withdraw_applies(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """True when same-chain NEAR Lending → wallet withdraw (MCA exec, no 1Click)."""
    if not _near_same_chain_mca_base(from_chain, to_chain, token_in, token_out, mca):
        return False
    flow = str(mca.get("flow") or mca.get("mcaFlow") or "").strip().lower()
    return flow == "withdraw"


def near_same_chain_mca_withdraw_intents_applies(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """
    True when same-chain NEAR Lending withdraw + different destination NEP-141
    (e.g. USDT from Burrow → USDC to recipient via 1Click after MCA exec).
    """
    if not mca or not isinstance(mca, dict):
        return False
    if not is_near_chain_id(from_chain) or not is_near_chain_id(to_chain):
        return False
    if str(from_chain).strip().lower() != str(to_chain).strip().lower():
        return False
    flow = str(mca.get("flow") or mca.get("mcaFlow") or "").strip().lower()
    if flow != "withdraw":
        return False
    a = (token_in or {}).get("address")
    b = (token_out or {}).get("address")
    if not a or not b:
        return False
    return not _addr_equal(a, b)


def near_same_chain_mca_applies(
    from_chain: str,
    to_chain: str,
    token_in: Dict[str, Any],
    token_out: Dict[str, Any],
    mca: Optional[Dict[str, Any]],
) -> bool:
    """Alias for withdraw-only direct MCA path (kept for call-site clarity)."""
    return near_same_chain_mca_withdraw_applies(
        from_chain, to_chain, token_in, token_out, mca,
    )


def resolve_near_mca_deposit_receiver(mca: Dict[str, Any], recipient: str) -> str:
    r = (
        mca.get("nearDepositReceiver")
        or mca.get("near_deposit_receiver")
        or mca.get("mcaAccountId")
        or mca.get("mca_id")
        or recipient
    )
    return str(r or "").strip()
