"""
Run (시리얼 사용 시 reload 비권장 — 포트 점유 충돌 가능):
    python -m uvicorn app.main:app --host 127.0.0.1 --port 8765
Open: http://127.0.0.1:8765/
"""

from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.sg003a import SG003AClient


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


SERIAL_PORT = os.environ.get("SG003A_PORT", "COM3")
SLAVE_ID = env_int("SG003A_SLAVE", 1)
BAUDRATE = env_int("SG003A_BAUD", 19200)
POLL_INTERVAL = float(os.environ.get("SG003A_POLL_MS", "1000")) / 1000.0


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


state = PollerState()
poll_thread: Optional[threading.Thread] = None
_client: Optional[SG003AClient] = None


def poll_loop_actual(client: SG003AClient) -> None:
    import time as _time

    while state.running:
        snap = client.read_full_map_safe()
        d = asdict(snap)
        d["_connection"] = {
            "port": SERIAL_PORT,
            "slave_id": SLAVE_ID,
            "baudrate": BAUDRATE,
        }
        state.snapshot = d
        _time.sleep(POLL_INTERVAL)


app = FastAPI(title="SG-003A viewer")


@app.on_event("startup")
async def startup() -> None:
    global poll_thread, _client

    baud_src = "SG003A_BAUD 환경 변수" if (os.environ.get("SG003A_BAUD", "").strip() != "") else "코드 기본값"
    print(
        f"[SG-003A] 시리얼: {SERIAL_PORT} @ {BAUDRATE} baud, slave={SLAVE_ID} ({baud_src})",
        flush=True,
    )

    client = SG003AClient(port=SERIAL_PORT, slave_id=SLAVE_ID, baudrate=BAUDRATE)
    _client = client

    async def opener() -> None:
        loop = asyncio.get_running_loop()

        def o() -> None:
            client.open()

        await loop.run_in_executor(None, o)

    async def opener_with_retry() -> None:
        for _ in range(3):
            try:
                await opener()

                def first_read() -> None:
                    snap = client.read_full_map_safe()
                    d = asdict(snap)
                    d["_connection"] = {
                        "port": SERIAL_PORT,
                        "slave_id": SLAVE_ID,
                        "baudrate": BAUDRATE,
                    }
                    state.snapshot = d

                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, first_read)

                state.running = True
                t = threading.Thread(target=poll_loop_actual, args=(client,), daemon=True)
                poll_thread = t
                t.start()
                return
            except Exception as e:
                state.snapshot = {
                    "ts": None,
                    "error": serial_open_error_with_hints(e),
                    "status": "failed_to_open_serial",
                    "port": SERIAL_PORT,
                    "slave_id": SLAVE_ID,
                    "baudrate": BAUDRATE,
                }
                await asyncio.sleep(1)

    asyncio.create_task(opener_with_retry())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    state.running = False
    if _client is not None:
        try:
            _client.close()
        except Exception:
            pass


@app.get("/api/config")
async def api_config():
    return {
        "port": SERIAL_PORT,
        "slave_id": SLAVE_ID,
        "baudrate": BAUDRATE,
        "poll_interval_s": POLL_INTERVAL,
    }


@app.get("/api/snapshot")
async def api_snapshot():
    snap = state.snapshot
    return snap or {"error": "not_ready", "status": "initializing"}


static_dir = Path(__file__).resolve().parent.parent / "static"


@app.get("/")
async def serve_index():
    return FileResponse(static_dir / "index.html")


app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
