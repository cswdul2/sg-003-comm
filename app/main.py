"""
Run (시리얼 사용 시 reload 비권장 — 포트 점유 충돌 가능):
    python -m uvicorn app.main:app --host 127.0.0.1 --port 8765
Open: http://127.0.0.1:8765/

통신이 확인된 기본 조합(환경변수 미설정 시):
  - slave=5, baud=9600, crc_order=hi-lo, read_frame=with-count
  - FC 0x64/0x66 + 주소 뒤 개수(00 01 / 00 02), 레지스터는 40001~40014 순차 개별 읽기
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from serial.tools import list_ports

from app.sg003a import SG003AClient


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


SERIAL_PORT = os.environ.get("SG003A_PORT", "COM3")
SLAVE_ID = env_int("SG003A_SLAVE", 1)
BAUDRATE = env_int("SG003A_BAUD", 9600)
POLL_INTERVAL = float(os.environ.get("SG003A_POLL_MS", "1000")) / 1000.0
CRC_ORDER = os.environ.get("SG003A_CRC_ORDER", "lo-hi").strip().lower()
CRC_HIGH_FIRST = CRC_ORDER not in ("lo-hi", "low-high", "modbus")
READ_FRAME_MODE = os.environ.get("SG003A_READ_FRAME", "with-count").strip().lower()
READ_WITH_COUNT = READ_FRAME_MODE not in ("addr-only", "legacy")


def _is_serial_permission_denied(exc: BaseException) -> bool:
    s = str(exc)
    if "PermissionError" in s or "액세스가 거부" in s or "Access is denied" in s:
        return True
    e: Optional[BaseException] = exc
    while e is not None:
        if isinstance(e, PermissionError):
            return True
        if getattr(e, "winerror", None) == 5:
            return True
        if getattr(e, "errno", None) in (13, 5):
            return True
        e = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
    return False


def serial_open_error_with_hints(exc: Exception) -> str:
    msg = str(exc)
    if not _is_serial_permission_denied(exc):
        return msg
    return (
        msg
        + "\n\n— COM 접근 거부면 보통 「이미 다른 프로그램이 COM을 열고 있음」 입니다.\n"
        + "• 동일 포트를 쓰는 설정 프로그램, PuTTY/Tera Term, 다른 Python/노드 터미널 종료.\n"
        + "• 백그라운드에 남은 이전 uvicorn/python 프로세스 종료.\n"
        + "• 가능하면 「--reload 없이」 실행: python -m uvicorn app.main:app --host 127.0.0.1 --port 8765\n"
        + "• USB-시리얼을 뽑았다 다시 연결 또는 PC 재부팅."
    )


class PollerState:
    def __init__(self) -> None:
        self.snapshot: Optional[dict[str, Any]] = None
        self.running = False
        self.last_write: Optional[dict[str, Any]] = None
        self.version = 0
        self.cond = threading.Condition()
        self.connected = False
        self.connected_port: Optional[str] = None
        self.io_log: list[dict[str, Any]] = []


state = PollerState()
poll_thread: Optional[threading.Thread] = None
_client: Optional[SG003AClient] = None
_client_lock = threading.Lock()


class ConnectRequest(BaseModel):
    port: str


def _push_log(row: dict[str, Any]) -> None:
    state.io_log.append(row)
    if len(state.io_log) > 300:
        state.io_log = state.io_log[-300:]


def poll_loop_actual() -> None:
    import time as _time

    while state.running:
        with _client_lock:
            client = _client
        if client is None:
            _time.sleep(0.2)
            continue
        snap = client.read_full_map_safe()
        d = asdict(snap)
        d["_connection"] = {
            "port": state.connected_port or SERIAL_PORT,
            "slave_id": SLAVE_ID,
            "baudrate": BAUDRATE,
        }
        with state.cond:
            state.snapshot = d
            for t in d.get("rtu_trace", []):
                _push_log(
                    {
                        "ts": d.get("ts"),
                        "kind": t.get("kind"),
                        "plc": t.get("plc"),
                        "tx": t.get("tx_spaced"),
                        "rx": t.get("rx_spaced"),
                        "ok": t.get("parse_ok"),
                    }
                )
            state.version += 1
            state.cond.notify_all()
        _time.sleep(POLL_INTERVAL)


app = FastAPI(title="SG-003A viewer")


@app.on_event("startup")
async def startup() -> None:
    global poll_thread, _client

    baud_src = "SG003A_BAUD 환경 변수" if (os.environ.get("SG003A_BAUD", "").strip() != "") else "코드 기본값"
    print(
        f"[SG-003A] 시리얼: {SERIAL_PORT} @ {BAUDRATE} baud, slave={SLAVE_ID}, crc={'hi-lo' if CRC_HIGH_FIRST else 'lo-hi'}, read_frame={'with-count' if READ_WITH_COUNT else 'addr-only'} ({baud_src})",
        flush=True,
    )

    def connect_default_port() -> Optional[str]:
        global _client
        try:
            c = SG003AClient(
                port=SERIAL_PORT,
                slave_id=SLAVE_ID,
                baudrate=BAUDRATE,
                crc_high_first=CRC_HIGH_FIRST,
                read_with_count=READ_WITH_COUNT,
            )
            c.open()
            with _client_lock:
                _client = c
            state.connected = True
            state.connected_port = SERIAL_PORT
            return None
        except Exception as e:
            with _client_lock:
                _client = None
            state.connected = False
            state.connected_port = None
            return serial_open_error_with_hints(e)

    state.running = True
    open_err = connect_default_port()
    with state.cond:
        state.snapshot = {
            "ts": None,
            "status": "connected" if open_err is None else "failed_to_open_serial",
            "error": open_err,
            "_connection": {
                "port": state.connected_port,
                "slave_id": SLAVE_ID,
                "baudrate": BAUDRATE,
            },
        }
        state.version += 1
        state.cond.notify_all()

    if open_err is None:

        def first_read_once() -> None:
            with _client_lock:
                c = _client
            if c is None:
                return
            snap = c.read_full_map_safe()
            d = asdict(snap)
            d["_connection"] = {
                "port": state.connected_port or SERIAL_PORT,
                "slave_id": SLAVE_ID,
                "baudrate": BAUDRATE,
            }
            with state.cond:
                state.snapshot = d
                for trow in d.get("rtu_trace", []):
                    _push_log(
                        {
                            "ts": d.get("ts"),
                            "kind": trow.get("kind"),
                            "plc": trow.get("plc"),
                            "tx": trow.get("tx_spaced"),
                            "rx": trow.get("rx_spaced"),
                            "ok": trow.get("parse_ok"),
                        }
                    )
                state.version += 1
                state.cond.notify_all()

        threading.Thread(target=first_read_once, daemon=True).start()

    t = threading.Thread(target=poll_loop_actual, daemon=True)
    poll_thread = t
    t.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    state.running = False
    with _client_lock:
        c = _client
    if c is not None:
        try:
            c.close()
        except Exception:
            pass


@app.get("/api/config")
async def api_config():
    with _client_lock:
        connected = _client is not None
    return {
        "port": SERIAL_PORT,
        "slave_id": SLAVE_ID,
        "baudrate": BAUDRATE,
        "crc_order": "hi-lo" if CRC_HIGH_FIRST else "lo-hi",
        "read_frame": "with-count" if READ_WITH_COUNT else "addr-only",
        "poll_interval_s": POLL_INTERVAL,
        "connected": connected,
        "connected_port": state.connected_port,
    }


@app.get("/api/snapshot")
async def api_snapshot():
    snap = state.snapshot
    if not snap:
        return {"error": "not_ready", "status": "initializing", "last_write": state.last_write}
    out = dict(snap)
    out["last_write"] = state.last_write
    out["io_log"] = state.io_log[-120:]
    return out


@app.get("/api/ports")
async def api_ports():
    ports = [p.device for p in list_ports.comports()]
    return {"ports": ports}


@app.post("/api/connect")
async def api_connect(req: ConnectRequest):
    global _client
    port = req.port.strip()
    if not port:
        return {"ok": False, "error": "empty_port"}
    try:
        c = SG003AClient(
            port=port,
            slave_id=SLAVE_ID,
            baudrate=BAUDRATE,
            crc_high_first=CRC_HIGH_FIRST,
            read_with_count=READ_WITH_COUNT,
        )
        c.open()
        with _client_lock:
            if _client is not None:
                try:
                    _client.close()
                except Exception:
                    pass
            _client = c
        with state.cond:
            state.connected = True
            state.connected_port = port
            state.version += 1
            state.cond.notify_all()
        return {"ok": True, "port": port}
    except Exception as e:
        return {"ok": False, "error": serial_open_error_with_hints(e)}


@app.post("/api/disconnect")
async def api_disconnect():
    global _client
    with _client_lock:
        c = _client
        _client = None
    if c is not None:
        try:
            c.close()
        except Exception:
            pass
    with state.cond:
        state.connected = False
        state.connected_port = None
        state.version += 1
        state.cond.notify_all()
    return {"ok": True}


@app.get("/api/stream")
async def api_stream():
    def gen():
        last_ver = -1
        while True:
            with state.cond:
                if state.version == last_ver:
                    state.cond.wait(timeout=15.0)
                current_ver = state.version
                snap = state.snapshot
                lw = state.last_write
            payload = {"version": current_ver, "snapshot": snap, "last_write": lw}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            last_ver = current_ver

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/api/write-example")
async def api_write_example():
    """
    Manual write example:
    - register: 40002 (0x9C42)
    - value: 0x0462  (mV + J thermocouple type in manual illustration)
    """
    with _client_lock:
        c = _client
    if c is None:
        return {"ok": False, "error": "client_not_initialized"}
    try:
        res = c.write_u16(40002, 0x0462)
    except Exception as e:
        res = {"ok": False, "error": str(e)}
    with state.cond:
        state.last_write = res
        _push_log({"ts": None, "kind": "write_u16", "plc": 40002, "tx": res.get("tx_spaced"), "rx": res.get("rx_spaced"), "ok": res.get("ok")})
        state.version += 1
        state.cond.notify_all()
    return res


static_dir = Path(__file__).resolve().parent.parent / "static"


@app.get("/")
async def serve_index():
    return FileResponse(static_dir / "index.html")


app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
