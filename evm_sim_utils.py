"""
Best-effort EVM swap executability simulation via `eth_call` state override.

Why this exists
---------------
At /quote time the user has NOT approved the DEX router yet, so a plain
`eth_call` of the swap reverts at the token-pull step (`TF` / `STF` /
`SafeERC20`) for EVERY router — that tells us nothing about route quality.
To test whether a route can ACTUALLY execute on-chain, we inject (override)
the sender's tokenIn balance and allowance-to-spender into the call state,
then run `eth_call`. If it still reverts, the route itself is unexecutable
(e.g. a thin-liquidity aggregator path) and the caller can demote it.

This is BEST-EFFORT and never raises: if no RPC supports state override, or
the token's storage slots cannot be detected, it returns ``skipped=True`` so
callers keep the route rather than wrongly demoting it.
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ERC20 selectors
_SEL_BALANCE_OF = "0x70a08231"   # balanceOf(address)
_SEL_ALLOWANCE = "0xdd62ed3e"    # allowance(address,address)

# Probe constants used during storage-slot detection.
_PROBE_OWNER = "0x000000000000000000000000000000000000bEEF"
_PROBE_SPENDER = "0x000000000000000000000000000000000000cAFE"
_SENTINEL = 0x1234567890ABCDEF  # arbitrary non-trivial value written into a slot

# How many base slots to scan when probing a token's mapping layout.
_MAX_SLOT_SCAN = 40

# Injected funding (well above any realistic swap amount).
_HUGE = (1 << 128)
_HUGE_WEI = hex(10 * (10 ** 18))  # 10 ETH for gas

# Cache detected slots per (chain_id, token_addr_lower): {"bal": int|None, "alw": int|None}
_SLOT_CACHE: Dict[str, Dict[str, Optional[int]]] = {}

# Cache whether an RPC URL honors eth_call state override. This is the critical
# latency guard: probing storage slots on an override-incapable RPC would grind
# dozens of eth_calls before failing, so we gate slot detection behind one cheap
# capability probe (cached for the process lifetime).
_OVERRIDE_CACHE: Dict[str, bool] = {}

# Minimal runtime bytecode: `return sload(0)`. Used to verify that the RPC
# applies BOTH code and stateDiff overrides (exactly what funded sim relies on).
_PROBE_CODE = "0x60005460005260206000f3"
_PROBE_ADDR = "0x000000000000000000000000000000000000dEaD"


def _keccak(data: bytes) -> bytes:
    """keccak256 — prefer web3, fall back to pycryptodome."""
    try:
        from web3 import Web3
        return bytes(Web3.keccak(data))
    except Exception:
        from Crypto.Hash import keccak as _k  # pycryptodome
        h = _k.new(digest_bits=256)
        h.update(data)
        return h.digest()


def _pad32_addr(addr: str) -> bytes:
    return bytes.fromhex(addr.lower().replace("0x", "").zfill(64))


def _pad32_int(n: int) -> bytes:
    return n.to_bytes(32, "big")


def _slot_single(key_addr: str, base_slot: int) -> str:
    """Storage key for mapping(address => X) at base_slot: keccak(key . slot)."""
    return "0x" + _keccak(_pad32_addr(key_addr) + _pad32_int(base_slot)).hex()


def _slot_double(owner: str, spender: str, base_slot: int) -> str:
    """Storage key for mapping(address => mapping(address => X)) at base_slot."""
    inner = _keccak(_pad32_addr(owner) + _pad32_int(base_slot))
    return "0x" + _keccak(_pad32_addr(spender) + inner).hex()


def _value32(n: int) -> str:
    return "0x" + _pad32_int(n).hex()


def _eth_call(url: str, call_obj: Dict, overrides: Optional[Dict] = None, timeout: int = 12) -> Dict:
    """Single eth_call. Returns {"result": hex} or {"error": msg}."""
    params = [call_obj, "latest"]
    if overrides is not None:
        params.append(overrides)
    try:
        resp = requests.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": params},
            timeout=timeout,
        )
        body = resp.json()
        if "result" in body and body["result"] is not None:
            return {"result": body["result"]}
        err = body.get("error") or {}
        return {"error": err.get("message") or str(err) or "no result"}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def _rpc_supports_override(url: str, timeout: int = 4) -> bool:
    """Cheap, cached probe: does this RPC apply eth_call state override?

    One eth_call against a dummy address whose code (`return sload(0)`) and slot 0
    are both injected via override. If the readback equals the sentinel, the RPC
    honors overrides. Cached per URL so we never re-probe a known-bad endpoint.
    """
    cached = _OVERRIDE_CACHE.get(url)
    if cached is not None:
        return cached
    ov = {_PROBE_ADDR: {"code": _PROBE_CODE, "stateDiff": {_value32(0): _value32(_SENTINEL)}}}
    r = _eth_call(url, {"to": _PROBE_ADDR, "data": "0x"}, ov, timeout=timeout)
    ok = False
    if "result" in r:
        try:
            ok = int(r["result"], 16) == _SENTINEL
        except (ValueError, TypeError):
            ok = False
    _OVERRIDE_CACHE[url] = ok
    return ok


def _detect_slots_on_rpc(url: str, token: str) -> Optional[Dict[str, Optional[int]]]:
    """Detect balanceOf / allowance base slots on one RPC (must support override).

    Returns {"bal": int|None, "alw": int|None} or None if this RPC does not
    support state override at all (so the caller can try the next RPC).
    """
    # balanceOf(probe_owner)
    bal_call = {"to": token, "data": _SEL_BALANCE_OF + _pad32_addr(_PROBE_OWNER).hex()}
    bal_slot: Optional[int] = None
    override_supported = False
    for base in range(_MAX_SLOT_SCAN):
        key = _slot_single(_PROBE_OWNER, base)
        ov = {token: {"stateDiff": {key: _value32(_SENTINEL)}}}
        r = _eth_call(url, bal_call, ov)
        if "error" in r:
            # If the very first probe errors with an override-related message,
            # this RPC likely does not support state override -> bail to next RPC.
            return None
        try:
            val = int(r["result"], 16)
        except (ValueError, TypeError):
            return None
        if val == _SENTINEL:
            bal_slot = base
            override_supported = True
            break
    if not override_supported:
        # Reads succeeded but sentinel never stuck -> override unsupported here.
        return None

    # allowance(probe_owner, probe_spender)
    alw_call = {
        "to": token,
        "data": _SEL_ALLOWANCE + _pad32_addr(_PROBE_OWNER).hex() + _pad32_addr(_PROBE_SPENDER).hex(),
    }
    alw_slot: Optional[int] = None
    for base in range(_MAX_SLOT_SCAN):
        key = _slot_double(_PROBE_OWNER, _PROBE_SPENDER, base)
        ov = {token: {"stateDiff": {key: _value32(_SENTINEL)}}}
        r = _eth_call(url, alw_call, ov)
        if "error" in r:
            break
        try:
            val = int(r["result"], 16)
        except (ValueError, TypeError):
            break
        if val == _SENTINEL:
            alw_slot = base
            break

    return {"bal": bal_slot, "alw": alw_slot, "_rpc": url}


def simulate_swap_funded(
    chain_id: int,
    rpc_urls,
    sender: str,
    token_in_address: str,
    spender: str,
    amount_in: str,
    tx: Dict,
) -> Dict:
    """Simulate the swap `tx` with sender's tokenIn balance + allowance injected.

    Returns one of:
      {"success": True}                      route executes on-chain
      {"success": False, "error": msg}       route reverts even when funded/approved
      {"skipped": True, "reason": "..."}     could not verify (no RPC / no override /
                                             slots undetected) -> caller keeps route
    """
    to_addr = (tx or {}).get("to") or ""
    data = (tx or {}).get("data") or ""
    if not to_addr or not data:
        return {"skipped": True, "reason": "no_tx"}
    if not rpc_urls:
        return {"skipped": True, "reason": "no_rpc"}

    # Latency guard: only keep RPCs that actually honor state override. On
    # override-incapable endpoints this fails fast (one cached probe) instead of
    # grinding dozens of slot-detection eth_calls before giving up.
    capable = [u for u in rpc_urls if _rpc_supports_override(u)]
    if not capable:
        return {"skipped": True, "reason": "no_override_support"}
    rpc_urls = capable

    token = token_in_address.lower()
    cache_key = f"{int(chain_id)}:{token}"
    slots = _SLOT_CACHE.get(cache_key)
    rpc_for_override = None
    if slots and slots.get("bal") is not None:
        rpc_for_override = slots.get("_rpc")

    # Detect slots (and an override-capable RPC) if not cached.
    if not slots or slots.get("bal") is None:
        for url in rpc_urls:
            detected = _detect_slots_on_rpc(url, token_in_address)
            if detected and detected.get("bal") is not None:
                slots = detected
                _SLOT_CACHE[cache_key] = detected
                rpc_for_override = url
                break
        if not slots or slots.get("bal") is None:
            return {"skipped": True, "reason": "slots_undetected_or_no_override"}

    bal_slot = slots["bal"]
    alw_slot = slots.get("alw")

    try:
        need = int(str(amount_in))
    except (TypeError, ValueError):
        need = 0
    inject = max(need, _HUGE)

    state_diff = {_slot_single(sender, bal_slot): _value32(inject)}
    if alw_slot is not None and spender:
        state_diff[_slot_double(sender, spender, alw_slot)] = _value32(inject)

    overrides = {
        token_in_address: {"stateDiff": state_diff},
        sender: {"balance": _HUGE_WEI},
    }

    value = (tx or {}).get("value") or "0"
    if isinstance(value, int):
        value = hex(value)
    elif isinstance(value, str) and value and not value.startswith("0x"):
        try:
            value = hex(int(value))
        except ValueError:
            value = "0x0"

    gas = (tx or {}).get("gasLimit") or (tx or {}).get("gas") or "0xc3500"
    if isinstance(gas, int):
        gas = hex(gas)
    elif isinstance(gas, str) and gas and not gas.startswith("0x"):
        try:
            gas = hex(int(gas))
        except ValueError:
            gas = "0xc3500"

    call_obj = {"from": sender, "to": to_addr, "data": data, "value": value, "gas": gas}

    # Prefer the RPC that we already know supports override.
    ordered = ([rpc_for_override] if rpc_for_override else []) + [u for u in rpc_urls if u != rpc_for_override]
    last_err = ""
    for url in ordered:
        r = _eth_call(url, call_obj, overrides)
        if "result" in r:
            return {"success": True, "rpc": url}
        last_err = r.get("error", "")
        # An override-unsupported RPC may now error; try the next one.
    return {"success": False, "error": last_err or "simulation reverted"}
