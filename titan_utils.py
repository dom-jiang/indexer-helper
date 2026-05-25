"""
Titan Direct WebSocket API integration for Solana swaps.

Protocol: MessagePack over WebSocket (optionally zstd-compressed).
Docs: https://titan-exchange.gitbook.io/titan/developer-doc/swap-api

Used by `swap_utils.aggregate_solana_quote` alongside Jupiter and OKX.
"""

from __future__ import annotations

import base64
import ssl
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import base58
import msgpack
import requests
from loguru import logger

from config import Cfg

try:
    import websocket  # websocket-client
except ImportError:  # pragma: no cover
    websocket = None  # type: ignore

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover
    zstd = None  # type: ignore

try:
    from db_info import (
        TITAN_WS_ENDPOINT,
        TITAN_API_JWT,
        TITAN_API_ORIGIN,
        TITAN_QUOTE_TIMEOUT_SEC,
    )
except ImportError:
    TITAN_WS_ENDPOINT = ""
    TITAN_API_JWT = ""
    TITAN_API_ORIGIN = "https://titan.exchange"
    TITAN_QUOTE_TIMEOUT_SEC = 15

_TITAN_SUBPROTOCOLS = ["v1.api.titan.ag+zstd", "v1.api.titan.ag"]
_jwt_cache: Dict[str, Tuple[str, float]] = {}
_jwt_cache_lock = threading.Lock()
_JWT_EXPIRY_BUFFER_SEC = 60


def _cfg(name: str, default: str = "") -> str:
    return str(getattr(Cfg, name, None) or globals().get(name) or default).strip()


def _mint_bytes(mint: str) -> bytes:
    raw = base58.b58decode(mint)
    if len(raw) != 32:
        raise ValueError(f"invalid Solana mint pubkey: {mint!r}")
    return raw


def _pubkey_b58(raw: bytes) -> str:
    return base58.b58encode(raw).decode("ascii")


def _fetch_apollo_jwt(user_pubkey: str) -> str:
    """Fetch a per-wallet Apollo JWT from Titan (same as frontend `/api/titan/apollo-jwt`)."""
    origin = _cfg("TITAN_API_ORIGIN", TITAN_API_ORIGIN).rstrip("/")
    url = f"{origin}/api/apollo-jwt"
    resp = requests.get(url, params={"address": user_pubkey}, timeout=10)
    resp.raise_for_status()
    data = resp.json() or {}
    token = str(data.get("token") or "").strip()
    if not token:
        raise ValueError("Titan apollo-jwt response missing token")
    expires_at = data.get("expires_at")
    expires_in = data.get("expires_in")
    ttl = 3600
    if expires_in is not None:
        try:
            ttl = max(60, int(expires_in) - _JWT_EXPIRY_BUFFER_SEC)
        except (TypeError, ValueError):
            pass
    elif expires_at:
        try:
            from datetime import datetime, timezone

            exp_dt = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
            ttl = max(60, int(exp_dt.timestamp() - time.time()) - _JWT_EXPIRY_BUFFER_SEC)
        except Exception:
            pass
    with _jwt_cache_lock:
        _jwt_cache[user_pubkey] = (token, time.time() + ttl)
    return token


def _resolve_auth_token(user_pubkey: str) -> str:
    static = _cfg("TITAN_API_JWT", TITAN_API_JWT)
    if static:
        return static
    now = time.time()
    with _jwt_cache_lock:
        cached = _jwt_cache.get(user_pubkey)
        if cached and cached[1] > now + _JWT_EXPIRY_BUFFER_SEC:
            return cached[0]
    return _fetch_apollo_jwt(user_pubkey)


def _ws_url(auth_token: str) -> str:
    endpoint = _cfg("TITAN_WS_ENDPOINT", TITAN_WS_ENDPOINT)
    if not endpoint:
        endpoint = "wss://api.titan.exchange/api/v1/ws"
    sep = "&" if "?" in endpoint else "?"
    return f"{endpoint}{sep}auth={auth_token}"


class _TitanCodec:
    def __init__(self, use_compression: bool):
        self.use_compression = use_compression
        self._packer = msgpack.Packer(use_bin_type=True)

    def encode(self, obj: Any) -> bytes:
        payload = self._packer.pack(obj)
        if self.use_compression:
            if zstd is None:
                raise RuntimeError("zstandard package required for Titan zstd sub-protocol")
            return zstd.ZstdCompressor().compress(payload)
        return payload

    def decode(self, raw: bytes) -> Any:
        data = raw
        if self.use_compression:
            if zstd is None:
                raise RuntimeError("zstandard package required for Titan zstd sub-protocol")
            data = zstd.ZstdDecompressor().decompress(raw)
        return msgpack.unpackb(data, raw=False)


def _pick_best_route(quotes: Dict[str, Any], metadata: Optional[Dict]) -> Tuple[str, Dict]:
    if not quotes:
        raise ValueError("empty Titan quotes map")
    winner = None
    if isinstance(metadata, dict):
        winner = metadata.get("ExpectedWinner")
    if winner and winner in quotes:
        return str(winner), quotes[winner]
    best_id = max(quotes.keys(), key=lambda k: int(quotes[k].get("outAmount") or 0))
    return str(best_id), quotes[best_id]


def _normalize_route(route: Dict[str, Any]) -> Dict[str, Any]:
    """Convert binary Titan fields into JSON-friendly structures for downstream assembly."""
    out = dict(route)
    alts = []
    for alt in route.get("addressLookupTables") or []:
        if isinstance(alt, (bytes, bytearray)):
            alts.append(_pubkey_b58(bytes(alt)))
        else:
            alts.append(str(alt))
    out["addressLookupTables"] = alts

    instructions = []
    for ix in route.get("instructions") or []:
        if not isinstance(ix, dict):
            continue
        prog = ix.get("p") or ix.get("programId")
        if isinstance(prog, (bytes, bytearray)):
            prog_b58 = _pubkey_b58(bytes(prog))
        else:
            prog_b58 = str(prog)
        accounts = []
        for acc in ix.get("a") or ix.get("accounts") or []:
            if not isinstance(acc, dict):
                continue
            pk = acc.get("p") or acc.get("pubkey")
            if isinstance(pk, (bytes, bytearray)):
                pk_b58 = _pubkey_b58(bytes(pk))
            else:
                pk_b58 = str(pk)
            accounts.append({
                "pubkey": pk_b58,
                "isSigner": bool(acc.get("s", acc.get("isSigner", False))),
                "isWritable": bool(acc.get("w", acc.get("isWritable", False))),
            })
        data = ix.get("d") or ix.get("data") or b""
        if isinstance(data, (bytes, bytearray)):
            data_b64 = base64.b64encode(bytes(data)).decode("ascii")
        else:
            data_b64 = str(data)
        instructions.append({"programId": prog_b58, "accounts": accounts, "data": data_b64})
    out["instructions"] = instructions

    tx = route.get("transaction")
    if isinstance(tx, (bytes, bytearray)) and tx:
        out["swapTransaction"] = base64.b64encode(bytes(tx)).decode("ascii")
    return out


def titan_order(
    input_mint: str,
    output_mint: str,
    amount: str,
    slippage_bps: int,
    taker: str,
    swap_mode: str = "ExactIn",
    destination_token_account: Optional[str] = None,
) -> Dict:
    """
    Request a Titan swap quote with executable instructions.

    Returns the same envelope as `jupiter_order`:
    ``{"success": True/False, "router": "titan", "data": {...}, "error": "..."}``
    """
    if websocket is None:
        return {
            "success": False,
            "router": "titan",
            "error": "websocket-client package not installed",
        }
    try:
        amount_int = int(str(amount))
    except (TypeError, ValueError):
        return {"success": False, "router": "titan", "error": f"invalid amount: {amount!r}"}
    if amount_int <= 0:
        return {"success": False, "router": "titan", "error": "amount must be positive"}

    timeout_sec = float(_cfg("TITAN_QUOTE_TIMEOUT_SEC", str(TITAN_QUOTE_TIMEOUT_SEC)) or 15)
    ws = None
    stream_id: Optional[int] = None
    request_id = 1

    try:
        auth = _resolve_auth_token(taker)
        url = _ws_url(auth)
        ws = websocket.create_connection(
            url,
            timeout=timeout_sec,
            subprotocols=_TITAN_SUBPROTOCOLS,
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
        )
        negotiated = ws.subprotocol or "v1.api.titan.ag"
        use_compression = negotiated != "v1.api.titan.ag"
        codec = _TitanCodec(use_compression)

        tx_params: Dict[str, Any] = {
            "userPublicKey": _mint_bytes(taker),
            "createOutputTokenAccount": True,
            "closeInputTokenAccount": False,
        }
        if destination_token_account:
            tx_params["outputAccount"] = _mint_bytes(destination_token_account)

        req_body = {
            "NewSwapQuoteStream": {
                "swap": {
                    "inputMint": _mint_bytes(input_mint),
                    "outputMint": _mint_bytes(output_mint),
                    "amount": amount_int,
                    "swapMode": swap_mode,
                    "slippageBps": int(slippage_bps),
                },
                "transaction": tx_params,
                "update": {"numQuotes": 1, "intervalMs": 5000},
            }
        }
        ws.send(codec.encode({"id": request_id, "data": req_body}), opcode=websocket.ABNF.OPCODE_BINARY)
        request_id += 1

        deadline = time.time() + timeout_sec
        quotes_payload = None
        while time.time() < deadline:
            ws.settimeout(max(0.5, deadline - time.time()))
            try:
                frame = ws.recv()
            except websocket.WebSocketTimeoutException:
                break
            if not frame or isinstance(frame, str):
                continue
            msg = codec.decode(frame)

            if isinstance(msg, dict) and "Error" in msg:
                err = msg["Error"] or {}
                return {
                    "success": False,
                    "router": "titan",
                    "error": f"Titan error {err.get('code')}: {err.get('message')}",
                }

            if isinstance(msg, dict) and "Response" in msg:
                resp = msg["Response"] or {}
                stream = resp.get("stream") or {}
                if stream.get("id") is not None:
                    stream_id = int(stream["id"])

            if isinstance(msg, dict) and "StreamData" in msg:
                payload = (msg["StreamData"] or {}).get("payload") or {}
                if "SwapQuotes" in payload:
                    quotes_payload = payload["SwapQuotes"]
                    break

        if not quotes_payload:
            return {"success": False, "router": "titan", "error": "No quotes returned from Titan"}

        quotes = quotes_payload.get("quotes") or {}
        provider_id, best = _pick_best_route(quotes, quotes_payload.get("metadata"))
        normalized = _normalize_route(best)
        out_amount = str(normalized.get("outAmount") or "")
        if not out_amount or int(out_amount) <= 0:
            return {"success": False, "router": "titan", "error": "Titan quote missing outAmount"}

        data = {
            "outAmount": out_amount,
            "inAmount": str(normalized.get("inAmount") or amount_int),
            "providerId": provider_id,
            "instructions": normalized.get("instructions") or [],
            "addressLookupTables": normalized.get("addressLookupTables") or [],
            "swapTransaction": normalized.get("swapTransaction", ""),
            "slippageBps": normalized.get("slippageBps"),
            "quoteId": quotes_payload.get("id", ""),
        }
        return {"success": True, "router": "titan", "data": data}
    except Exception as e:
        logger.error(f"titan_order error: {e}")
        return {"success": False, "router": "titan", "error": str(e)}
    finally:
        try:
            if ws is not None and stream_id is not None:
                negotiated = ws.subprotocol or "v1.api.titan.ag"
                codec = _TitanCodec(negotiated != "v1.api.titan.ag")
                ws.send(
                    codec.encode({"id": request_id, "data": {"StopStream": {"id": stream_id}}}),
                    opcode=websocket.ABNF.OPCODE_BINARY,
                )
        except Exception:
            pass
        try:
            if ws is not None:
                ws.close()
        except Exception:
            pass
