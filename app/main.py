"""
Run (시리얼 사용 시 reload 비권장 — 포트 점유 충돌 가능):
    python -m uvicorn app.main:app --host 127.0.0.1 --port 8765
Open: http://127.0.0.1:8765/

기본 조합(환경변수 미설정 시, 장비 매뉴얼과 맞게 조정):
  - slave=1, baud=115200, crc_order=lo-hi(표준 Modbus 기본), read_frame=with-count
  - 읽기: 순차 RTU. SG003A_INTER_READ_MS(ms)→번지 간 sleep; 미설정 100ms. 값이 250ms 초과면(또는 1000 근처) 기본적으로 100ms로 줄임 → 그대로 쓰려면 SG003A_INTER_READ_FORCE_EXACT=1.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from serial.tools import list_ports

from app.sg003a import DeviceSnapshot, SG003AClient


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


SERIAL_PORT = os.environ.get("SG003A_PORT", "COM3")
SLAVE_ID = env_int("SG003A_SLAVE", 1)
BAUDRATE = env_int("SG003A_BAUD", 115200)
POLL_INTERVAL = float(os.environ.get("SG003A_POLL_MS", "0")) / 1000.0
CRC_ORDER = os.environ.get("SG003A_CRC_ORDER", "lo-hi").strip().lower()
CRC_HIGH_FIRST = CRC_ORDER not in ("lo-hi", "low-high", "modbus")
READ_FRAME_MODE = os.environ.get("SG003A_READ_FRAME", "with-count").strip().lower()
READ_WITH_COUNT = READ_FRAME_MODE not in ("addr-only", "legacy")
BLOCK_READ_ENV = os.environ.get("SG003A_BLOCK_READ", "0").strip().lower()
BLOCK_READ_ENABLED = BLOCK_READ_ENV not in ("0", "false", "no", "off")
# 순차 RTU: 번지 RX 끝난 뒤 다음 TX까지 대기(ms). 코드·콘솔·/api/config에 적용값 표시.
# SG003A_INTER_READ_MS=1000 은 「번지마다 1초」(로그 줄 간격도 ~1s). 빠르게 하려면 100(0.1s) 전후.
def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _inter_read_ms_env() -> tuple[float, Optional[str]]:
    raw = os.environ.get("SG003A_INTER_READ_MS")
    unset = raw is None or str(raw).strip() == ""
    s = ("100" if unset else str(raw).strip()) or "100"
    try:
        val = float(s)
    except ValueError:
        return 100.0, None
    val = max(0.0, val)
    # 진짜로 긴 간격(ms) 그대로 쓰려면 SG003A_INTER_READ_FORCE_EXACT=1 (또는 1초 대기 명시 확인)
    force_exact = _truthy_env("SG003A_INTER_READ_FORCE_EXACT") or _truthy_env(
        "SG003A_INTER_READ_CONFIRM_1S"
    )
    note: Optional[str] = None
    if not unset and not force_exact:
        # 990~1010 등 흔한 오타 외에, 250ms 넘으면 로그 줄 간격이 체감상 병목이 되기 쉬움 → 기본은 100ms 로 맞춤
        if 990.0 <= val <= 1010.0:
            note = "remapped_1000ms_to_100ms"
            val = 100.0
        elif val >= 250.0:
            note = "clamped_inter_read_to_100ms"
            val = 100.0
    return val, note


_INTER_READ_MS_EFFECTIVE, _INTER_GAP_REMAP_NOTE = _inter_read_ms_env()
INTER_REQUEST_DELAY_S = max(0.0, _INTER_READ_MS_EFFECTIVE / 1000.0)


def _serial_timeout_ms_env() -> float:
    raw = os.environ.get("SG003A_SERIAL_TIMEOUT_MS")
    if raw is None or str(raw).strip() == "":
        return 350.0
    try:
        return float(str(raw).strip())
    except ValueError:
        return 350.0


_SERIAL_TIMEOUT_MS_EFFECTIVE = _serial_timeout_ms_env()
SERIAL_TIMEOUT_S = max(0.05, min(3.0, _SERIAL_TIMEOUT_MS_EFFECTIVE / 1000.0))


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
_map_read_guard = threading.Lock()


class ConnectRequest(BaseModel):
    port: str


def _push_log(row: dict[str, Any]) -> None:
    entry = dict(row)
    entry.setdefault("logged_at", time.time())
    state.io_log.append(entry)
    if len(state.io_log) > 300:
        state.io_log = state.io_log[-300:]


_MAP_STEP_TOP_KEYS: tuple[tuple[str, ...], ...] = (
    ("firmware_u16",),
    ("input_signal",),
    ("output_signal",),
    ("input_value",),
    ("output_value",),
    ("soft_mode_u16",),
    ("active_io_user_hi",),
    ("active_io_user_lo",),
    ("volt_user_hi",),
    ("volt_user_lo",),
    ("passive_io_user_hi",),
    ("passive_io_user_lo",),
)

_MAP_STEP_HEX_U16: tuple[Optional[str], ...] = (
    "40001",
    "40002",
    "40003",
    None,
    None,
    "40008",
    "40009",
    "40010",
    "40011",
    "40012",
    "40013",
    "40014",
)

_MAP_STEP_HEX_FLOAT: tuple[Optional[str], ...] = (
    None,
    None,
    None,
    "40004",
    "40006",
    None,
    None,
    None,
    None,
    None,
    None,
    None,
)


def _merge_rtu_snapshot(
    prev: Optional[dict[str, Any]], new: dict[str, Any], completed_steps: int
) -> dict[str, Any]:
    """이번 스윕에서 completed_steps(1~12)까지 읽은 필드만 덮어쓰고, 나머지 카드 값은 prev 유지."""
    merged: dict[str, Any] = dict(prev) if prev else {}
    n = min(max(completed_steps, 0), len(_MAP_STEP_TOP_KEYS))
    for s in range(n):
        for key in _MAP_STEP_TOP_KEYS[s]:
            merged[key] = new[key]
    h16: dict[str, Any] = dict(merged.get("hex_u16") or {})
    hf: dict[str, Any] = dict(merged.get("hex_float_ieee754_be") or {})
    nu16: dict[str, Any] = new.get("hex_u16") or {}
    nf: dict[str, Any] = new.get("hex_float_ieee754_be") or {}
    for s in range(n):
        ku = _MAP_STEP_HEX_U16[s]
        if ku is not None and ku in nu16:
            h16[ku] = nu16[ku]
        elif ku is not None and ku not in nu16:
            h16.pop(ku, None)
        kf = _MAP_STEP_HEX_FLOAT[s]
        if kf is not None and kf in nf:
            hf[kf] = nf[kf]
        elif kf is not None and kf not in nf:
            hf.pop(kf, None)
    merged["hex_u16"] = h16
    merged["hex_float_ieee754_be"] = hf
    merged["rtu_trace"] = new.get("rtu_trace") or []
    merged["ts"] = new.get("ts")
    merged["error"] = new.get("error")
    if "_connection" in new:
        merged["_connection"] = new["_connection"]
    return merged


def _publish_map_read_partial(
    snap: DeviceSnapshot, log_tail_from: dict[str, int], completed_steps: int
) -> None:
    """순차 읽기마다 반영. 미읽은 번지는 기존 스냅샷 값 유지. io_log는 새 trace 행만."""
    d = asdict(snap)
    d["_connection"] = {
        "port": state.connected_port or SERIAL_PORT,
        "slave_id": SLAVE_ID,
        "baudrate": BAUDRATE,
    }
    with state.cond:
        tr = d.get("rtu_trace") or []
        start_i = log_tail_from.get("i", 0)
        for trow in tr[start_i:]:
            _push_log(
                {
                    "ts": d.get("ts"),
                    "logged_at": float(trow["rx_done_at"])
                    if trow.get("rx_done_at") is not None
                    else time.time(),
                    "kind": trow.get("kind"),
                    "plc": trow.get("plc"),
                    "tx": trow.get("tx_spaced"),
                    "rx": trow.get("rx_spaced"),
                    "ok": trow.get("parse_ok"),
                }
            )
        log_tail_from["i"] = len(tr)
        state.snapshot = _merge_rtu_snapshot(state.snapshot, d, completed_steps)
        state.version += 1
        state.cond.notify_all()


def poll_loop_actual() -> None:
    import time as _time

    while state.running:
        with _client_lock:
            client = _client
        if client is None:
            _time.sleep(0.2)
            continue
        t0 = _time.monotonic()
        with _map_read_guard:
            log_tail_from = {"i": 0}

            def on_partial(s: DeviceSnapshot, done_steps: int) -> None:
                _publish_map_read_partial(s, log_tail_from, done_steps)

            client.read_full_map_safe(on_partial=on_partial)
        elapsed = _time.monotonic() - t0
        remainder = POLL_INTERVAL - elapsed
        if remainder > 0:
            _time.sleep(remainder)


app = FastAPI(title="SG-003A viewer")


@app.on_event("startup")
async def startup() -> None:
    global poll_thread, _client

    baud_src = "SG003A_BAUD 환경 변수" if (os.environ.get("SG003A_BAUD", "").strip() != "") else "코드 기본값"
    print(
        f"[SG-003A] 시리얼: {SERIAL_PORT} @ {BAUDRATE} baud, slave={SLAVE_ID}, crc={'hi-lo' if CRC_HIGH_FIRST else 'lo-hi'}, read_frame={'with-count' if READ_WITH_COUNT else 'addr-only'} ({baud_src})",
        flush=True,
    )
    ie = os.environ.get("SG003A_INTER_READ_MS")
    ie_src = "환경변수" if ie is not None and str(ie).strip() != "" else "기본 100"
    print(
        f"[SG-003A] RTU 번지 간격: {_INTER_READ_MS_EFFECTIVE:g} ms → 다음 TX까지 {INTER_REQUEST_DELAY_S:g} s 대기 ({ie_src}; 100=0.1s)",
        flush=True,
    )
    if _INTER_GAP_REMAP_NOTE == "remapped_1000ms_to_100ms":
        print(
            "[SG-003A] ※ SG003A_INTER_READ_MS≈1000 은 「1초/번지」입니다. 빠르게 하려던 경우 자동으로 100ms 로 보정했습니다. 정말 그대로 쓰려면 SG003A_INTER_READ_FORCE_EXACT=1",
            flush=True,
        )
    elif _INTER_GAP_REMAP_NOTE == "clamped_inter_read_to_100ms":
        print(
            "[SG003A] ※ SG003A_INTER_READ_MS 가 250ms 초과였습니다 → 100ms 로 맞췄습니다. 환경값 그대로 쓰려면 SG003A_INTER_READ_FORCE_EXACT=1",
            flush=True,
        )
    st = os.environ.get("SG003A_SERIAL_TIMEOUT_MS")
    st_src = "환경변수" if st is not None and str(st).strip() != "" else "기본 350"
    print(
        f"[SG-003A] 시리얼 read 타임아웃: {_SERIAL_TIMEOUT_MS_EFFECTIVE:g} ms → pyserial timeout {SERIAL_TIMEOUT_S:g} s ({st_src}, SG003A_SERIAL_TIMEOUT_MS)",
        flush=True,
    )

    def connect_default_port() -> Optional[str]:
        global _client
        try:
            c = SG003AClient(
                port=SERIAL_PORT,
                slave_id=SLAVE_ID,
                baudrate=BAUDRATE,
                timeout_s=SERIAL_TIMEOUT_S,
                crc_high_first=CRC_HIGH_FIRST,
                read_with_count=READ_WITH_COUNT,
                block_read_first=BLOCK_READ_ENABLED,
                inter_request_delay_s=INTER_REQUEST_DELAY_S,
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
            with _map_read_guard:
                log_tail_from = {"i": 0}

                def on_partial(s: DeviceSnapshot, done_steps: int) -> None:
                    _publish_map_read_partial(s, log_tail_from, done_steps)

                c.read_full_map_safe(on_partial=on_partial)

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
        "block_read_first": BLOCK_READ_ENABLED,
        "inter_request_delay_s": INTER_REQUEST_DELAY_S,
        "inter_request_ms_effective": _INTER_READ_MS_EFFECTIVE,
        "inter_read_gap_note": _INTER_GAP_REMAP_NOTE,
        "serial_timeout_ms_effective": _SERIAL_TIMEOUT_MS_EFFECTIVE,
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
            timeout_s=SERIAL_TIMEOUT_S,
            crc_high_first=CRC_HIGH_FIRST,
            read_with_count=READ_WITH_COUNT,
            block_read_first=BLOCK_READ_ENABLED,
            inter_request_delay_s=INTER_REQUEST_DELAY_S,
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
                log_tail = state.io_log[-120:]
            snap_out = None
            if snap is not None:
                snap_out = dict(snap)
                snap_out["io_log"] = list(log_tail)
            payload = {"version": current_ver, "snapshot": snap_out, "last_write": lw}
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
