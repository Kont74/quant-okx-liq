"""
Module: okx_liq_collector.py
Purpose: Record OKX USDT-swap liquidation events (public liquidation-orders
         channel — one subscription covers ALL swaps). Liquidations are NOT
         backfillable; this collector IS the record. Venue-2 beside Bybit.
Design:  Fleet pattern: runs LIQ_RUN_SECONDS (default 3600), exits so the
         wrapper commits, systemd restarts. Reconnects inside the window.
         OKX requires a text "ping" during idle; handled on recv timeout.
Probe:   --probe subscribes to the BTC-USDT-SWAP tickers channel (pushes
         continuously) for 10s and reports frame count — proves this IP
         actually RECEIVES push (the Binance lesson: handshake+ack != data).
Output:  data/okx_liq_YYYY-MM-DD.csv
         recv_ts_utc, exch_ts_ms, inst_id, side, pos_side, sz, bk_px, raw_json
         side/posSide stored verbatim (OKX: side=buy fills a SHORT's
         liquidation, posSide names the liquidated side); interpret at
         analysis time; raw preserved.
Deps:    websocket-client. Python 3.10+.
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import websocket
except ImportError:
    sys.exit("Missing dependency: pip install websocket-client")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("okxliq")

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
RUN_SECONDS = int(os.environ.get("LIQ_RUN_SECONDS", "3600"))
DATA_DIR = Path("data")
COLS = ["recv_ts_utc", "exch_ts_ms", "inst_id", "side", "pos_side", "sz",
        "bk_px", "raw_json"]
rows_written = 0


def csv_path() -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR / f"okx_liq_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"


def append_rows(rows: list) -> None:
    global rows_written
    path = csv_path()
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(COLS)
        w.writerows(rows)
    rows_written += len(rows)


def on_message(message: str) -> None:
    try:
        msg = json.loads(message)
    except json.JSONDecodeError:
        return
    if msg.get("arg", {}).get("channel") != "liquidation-orders":
        return
    recv = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    out = []
    for item in msg.get("data", []):
        inst = item.get("instId", "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        for d in item.get("details", []):
            out.append([recv, d.get("ts", ""), inst, d.get("side", ""),
                        d.get("posSide", ""), d.get("sz", ""), d.get("bkPx", ""),
                        json.dumps(d, separators=(",", ":"))])
    if out:
        append_rows(out)


def connect(sub: dict):
    ws = websocket.WebSocket()
    ws.connect(WS_URL, timeout=15)
    ws.send(json.dumps(sub))
    ws.settimeout(25)
    return ws


def probe() -> None:
    """Prove push works from this IP: tickers channel must stream frames."""
    ws = connect({"op": "subscribe",
                  "args": [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]})
    frames, deadline = 0, time.time() + 10
    while time.time() < deadline:
        try:
            m = ws.recv()
            if m and "tickers" in m:
                frames += 1
        except websocket.WebSocketTimeoutException:
            break
    ws.close()
    print(f"PROBE: {frames} ticker frames in 10s")
    sys.exit(0 if frames >= 5 else 1)


def run_session(deadline: float) -> None:
    ws = connect({"op": "subscribe",
                  "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]})
    logger.info("connected; listening (all-swap liquidation-orders channel)")
    while time.time() < deadline:
        try:
            m = ws.recv()
            if isinstance(m, bytes):
                m = m.decode("utf-8", errors="ignore")
            if m == "pong":
                continue
            if m:
                on_message(m)
        except websocket.WebSocketTimeoutException:
            ws.send("ping")          # OKX idle keepalive
    ws.close()


def main() -> None:
    if "--probe" in sys.argv:
        probe()
    deadline = time.time() + RUN_SECONDS
    session = 0
    while time.time() < deadline - 10:
        session += 1
        try:
            run_session(deadline)
        except Exception as exc:  # noqa: BLE001 — reconnect until window ends
            remaining = int(deadline - time.time())
            logger.warning(f"session {session} dropped ({exc}); {remaining}s left")
            time.sleep(min(5, max(1, remaining)))
    logger.info(f"window complete: {rows_written} liquidation rows "
                f"across {session} session(s)")
    if rows_written == 0:
        logger.warning("ZERO liquidations this window — check push health if repeated")


if __name__ == "__main__":
    main()
