"""
SG-003A Modbus RTU with vendor function codes.

FC 100 (0x64): read uint16
FC 102 (0x66): read float (32-bit, two PLC addresses)
Register addresses use full 4xxxx value in PDU (see manual examples).
"""

from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import serial


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def append_crc(payload: bytes, *, high_first: bool = False) -> bytes:
    c = crc16_modbus(payload)
    lo = c & 0xFF
    hi = (c >> 8) & 0xFF
    if high_first:
        return payload + bytes([hi, lo])
    return payload + bytes([lo, hi])


FC_READ_U16 = 0x64
FC_READ_FLOAT = 0x66
FC_WRITE_U16 = 0x65

# PLC addresses from manual (holding register map).
REG_FIRMWARE = 40001
REG_INPUT_SIGNAL = 40002
REG_OUTPUT_SIGNAL = 40003
REG_INPUT_VALUE = 40004
REG_OUTPUT_VALUE = 40006
REG_SOFT_MODE = 40008
REG_ACTIVE_IO_USER_HI = 40009
REG_ACTIVE_IO_USER_LO = 40010
REG_VOLT_USER_HI = 40011
REG_VOLT_USER_LO = 40012
REG_PASSIVE_IO_USER_HI = 40013
REG_PASSIVE_IO_USER_LO = 40014


def plc_addr_to_pdu(addr: int) -> bytes:
    return struct.pack(">H", addr & 0xFFFF)


TC_TYPES = ["—", "S", "B", "E", "K", "R", "J", "T", "N"]
SIG_TYPES = ["—", "current", "voltage", "frequency", "millivolt", "resistance"]
MODES = ["—", "mV", "thermocouple", "WR thermocouple"]


def decode_signal_word(value: int) -> dict:
    hi = (value >> 8) & 0xFF
    lo = value & 0xFF
    sig_code = hi
    tc_nibble = (lo >> 4) & 0x0F
    mode_nibble = lo & 0x0F
    return {
        "raw_u16": value,
        "raw_hex": f"0x{value:04X}",
        "signal_type_code": sig_code,
        "signal_type": SIG_TYPES[sig_code] if 0 <= sig_code < len(SIG_TYPES) else f"code_{sig_code}",
        "tc_type": TC_TYPES[tc_nibble] if 1 <= tc_nibble <= 8 else f"nibble_{tc_nibble}",
        "mode_code": mode_nibble,
        "mode": MODES[mode_nibble] if 1 <= mode_nibble < len(MODES) else f"code_{mode_nibble}",
    }


@dataclass
class DeviceSnapshot:
    ts: float
    firmware_u16: Optional[int]
    input_signal: Optional[dict]
    output_signal: Optional[dict]
    input_value: Optional[float]
    output_value: Optional[float]
    soft_mode_u16: Optional[int]
    active_io_user_hi: Optional[int]
    active_io_user_lo: Optional[int]
    volt_user_hi: Optional[int]
    volt_user_lo: Optional[int]
    passive_io_user_hi: Optional[int]
    passive_io_user_lo: Optional[int]
    error: Optional[str]
    #: uint16 레지스터 PLC 주소 문자열 → "0xABCD"
    hex_u16: dict[str, str] = field(default_factory=dict)
    #: float 시작 주소 문자열 → IEEE754 big-endian 8 hex chars
    hex_float_ieee754_be: dict[str, str] = field(default_factory=dict)
    #: 한 번의 폴링 동안 순서대로 수집된 RTU 프레임 (TX 전체 · RX 원시)
    rtu_trace: list[dict[str, Any]] = field(default_factory=list)


class SG003AClient:
    def __init__(
        self,
        port: str,
        slave_id: int = 1,
        baudrate: int = 115200,
        bytesize: int = serial.EIGHTBITS,
        parity: str = serial.PARITY_NONE,
        stopbits: int = serial.STOPBITS_ONE,
        timeout_s: float = 0.35,
        crc_high_first: bool = True,
        read_with_count: bool = True,
        block_read_first: bool = False,
        inter_request_delay_s: float = 0.1,
    ):
        self._lock = threading.Lock()
        self._ser: Optional[serial.Serial] = None
        self.port = port
        self.slave_id = slave_id & 0xFF
        self.baudrate = baudrate
        self.bytesize = bytesize
        self.parity = parity
        self.stopbits = stopbits
        self.timeout_s = timeout_s
        self.crc_high_first = crc_high_first
        self.read_with_count = read_with_count
        self.block_read_first = block_read_first
        self.inter_request_delay_s = max(0.0, float(inter_request_delay_s))

    def _build_read_u16_body(self, plc_address: int) -> bytes:
        body = bytes([FC_READ_U16]) + plc_addr_to_pdu(plc_address)
        if self.read_with_count:
            body += b"\x00\x01"
        return body

    def _build_read_float_body(self, plc_address: int) -> bytes:
        body = bytes([FC_READ_FLOAT]) + plc_addr_to_pdu(plc_address)
        if self.read_with_count:
            # one float is two 16-bit registers
            body += b"\x00\x02"
        return body

    def _build_read_u16_block_body(self, start_plc_address: int, word_count: int) -> bytes:
        """Vendor FC 0x64 여러 워드 — 표준 Holding 다건과 같이 시작주소+N."""
        body = bytes([FC_READ_U16]) + plc_addr_to_pdu(start_plc_address)
        if self.read_with_count:
            body += struct.pack(">H", word_count & 0xFFFF)
        return body

    def _parse_read_u16_block_response(self, raw: bytes, word_count: int) -> list[int]:
        """응답: addr, fc, byte_count=N*2, 데이터..., CRC."""
        if len(raw) < 5:
            raise IOError(f"Short response ({len(raw)} bytes): {raw.hex()}")
        if raw[0] != self.slave_id:
            raise IOError(f"Unexpected slave in response: {raw.hex()}")
        if raw[1] & 0x80:
            raise IOError(f"Modbus exception: {raw.hex()}")
        if raw[1] != FC_READ_U16:
            raise IOError(f"Unexpected function code: {raw.hex()}")
        expected_bc = word_count * 2
        bc = raw[2]
        if bc != expected_bc:
            raise IOError(f"Unexpected byte count {bc} expected {expected_bc}: {raw.hex()}")
        need = 3 + bc + 2
        if len(raw) < need:
            raise IOError(f"Truncated block payload ({len(raw)} < {need}): {raw.hex()}")
        payload = raw[3 : 3 + bc]
        crc_calc, crc_rx = self._split_rx_crc(raw[: 3 + bc], raw[3 + bc : need])
        if crc_rx != crc_calc:
            raise IOError(f"CRC mismatch block (calc {crc_calc:04X} rx {crc_rx:04X}): {raw.hex()}")
        out: list[int] = []
        for i in range(0, len(payload), 2):
            out.append((payload[i] << 8) | payload[i + 1])
        if len(out) != word_count:
            raise IOError(f"Word unpack length {len(out)} != {word_count}: {raw.hex()}")
        return out

    def open(self) -> None:
        with self._lock:
            if self._ser and self._ser.is_open:
                return
            self._ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=self.bytesize,
                parity=self.parity,
                stopbits=self.stopbits,
                timeout=self.timeout_s,
            )

    def close(self) -> None:
        with self._lock:
            if self._ser and self._ser.is_open:
                self._ser.close()
            self._ser = None

    def _transceive(self, pdu_body: bytes) -> bytes:
        if not self._ser or not self._ser.is_open:
            raise RuntimeError("Serial port is not open")
        frame = append_crc(bytes([self.slave_id]) + pdu_body, high_first=self.crc_high_first)
        self._ser.reset_input_buffer()
        self._ser.write(frame)
        self._ser.flush()
        # 최소 5바이트(addr+fc+…) 확보 후 CRC 등 나머지 수신. 상한은 pyserial timeout 기준(0.5s 최소 대기 제거).
        deadline = time.monotonic() + max(0.08, self.timeout_s) + 0.04
        buf = bytearray()
        while time.monotonic() < deadline:
            chunk = self._ser.read(256)
            if chunk:
                buf.extend(chunk)
                if len(buf) >= 5:
                    break
            else:
                time.sleep(0.005)
        time.sleep(0.005)
        buf.extend(self._ser.read(256))
        return bytes(buf)

    def _exchange_traced(
        self,
        pdu_body: bytes,
        *,
        plc: int,
        kind: str,
        fc_label: str,
        trace_out: list[dict[str, Any]],
    ) -> bytes:
        """Send PDU (without CRC/slave) and append one trace row with spaced hex."""
        if not self._ser or not self._ser.is_open:
            raise RuntimeError("Serial port is not open")
        tx = append_crc(bytes([self.slave_id]) + pdu_body, high_first=self.crc_high_first)
        self._ser.reset_input_buffer()
        self._ser.write(tx)
        self._ser.flush()
        deadline = time.monotonic() + max(0.08, self.timeout_s) + 0.04
        buf = bytearray()
        while time.monotonic() < deadline:
            chunk = self._ser.read(256)
            if chunk:
                buf.extend(chunk)
                if len(buf) >= 5:
                    break
            else:
                time.sleep(0.005)
        time.sleep(0.005)
        buf.extend(self._ser.read(256))
        rx = bytes(buf)
        rx_done_wall = time.time()

        def _spaced(data: bytes) -> str:
            return " ".join(f"{b:02X}" for b in data)

        row: dict[str, Any] = {
            "plc": plc,
            "kind": kind,
            "fc_tx": fc_label,
            "tx_hex": tx.hex().upper(),
            "tx_spaced": _spaced(tx),
            "rx_hex": rx.hex().upper(),
            "rx_spaced": _spaced(rx),
            "rx_len": len(rx),
            "crc_order": "hi-lo" if self.crc_high_first else "lo-hi",
            "rx_done_at": rx_done_wall,
        }
        trace_out.append(row)
        return rx

    def _split_rx_crc(self, data_without_crc: bytes, raw_crc2: bytes) -> tuple[int, int]:
        crc_calc = crc16_modbus(data_without_crc)
        if len(raw_crc2) != 2:
            raise IOError("CRC length is not 2 bytes")
        if self.crc_high_first:
            crc_rx = (raw_crc2[0] << 8) | raw_crc2[1]
        else:
            crc_rx = raw_crc2[0] | (raw_crc2[1] << 8)
        return crc_calc, crc_rx

    def read_u16(self, plc_address: int) -> int:
        """Read one uint16 from holding map address (4xxxx style)."""
        body = self._build_read_u16_body(plc_address)
        with self._lock:
            raw = self._transceive(body)
        return self._parse_read_u16_response(raw)

    def read_float32(self, plc_address: int) -> float:
        """Read float starting at given address (occupies two addresses)."""
        body = self._build_read_float_body(plc_address)
        with self._lock:
            raw = self._transceive(body)
        return self._parse_read_float_response(raw)

    def write_u16(self, plc_address: int, value: int) -> dict[str, Any]:
        """
        Write one uint16 using vendor FC 0x65.

        Manual example:
            01 65 9C 42 04 62 CRC(2B)
        """
        v = value & 0xFFFF
        body = bytes([FC_WRITE_U16]) + plc_addr_to_pdu(plc_address) + struct.pack(">H", v)
        trace: list[dict[str, Any]] = []
        with self._lock:
            raw = self._exchange_traced(body, plc=plc_address, kind="write_u16", fc_label="0x65", trace_out=trace)

        row = trace[0] if trace else {}
        result: dict[str, Any] = {
            "ok": False,
            "address": plc_address,
            "value_u16": v,
            "value_hex": f"0x{v:04X}",
            "tx_spaced": row.get("tx_spaced", ""),
            "rx_spaced": row.get("rx_spaced", ""),
            "rx_len": row.get("rx_len", 0),
            "crc_order": row.get("crc_order"),
        }

        # Keep validation loose for diagnostics: many devices echo request,
        # some reply with a shortened ACK frame, some are silent.
        if len(raw) == 0:
            result["error"] = "No response bytes"
            return result
        if raw[0] != self.slave_id:
            result["error"] = f"Unexpected slave byte: 0x{raw[0]:02X}"
            return result
        if len(raw) >= 2 and (raw[1] & 0x80):
            result["error"] = f"Modbus exception frame: {raw.hex().upper()}"
            return result
        if len(raw) >= 2 and raw[1] != FC_WRITE_U16:
            result["error"] = f"Unexpected function code: 0x{raw[1]:02X}"
            return result

        result["ok"] = True
        return result

    def _parse_read_u16_response(self, raw: bytes) -> int:
        if len(raw) < 5:
            raise IOError(f"Short response ({len(raw)} bytes): {raw.hex()}")
        if raw[0] != self.slave_id:
            raise IOError(f"Unexpected slave in response: {raw.hex()}")
        if raw[1] & 0x80:
            raise IOError(f"Modbus exception: {raw.hex()}")
        if raw[1] != FC_READ_U16:
            raise IOError(f"Unexpected function code: {raw.hex()}")
        # Expect: id, fc, byte_count=2, hi, lo, crc_lo, crc_hi
        if len(raw) < 7:
            raise IOError(f"Truncated u16 payload: {raw.hex()}")
        bc = raw[2]
        if bc != 2:
            raise IOError(f"Unexpected byte count {bc}: {raw.hex()}")
        value = (raw[3] << 8) | raw[4]
        crc_calc, crc_rx = self._split_rx_crc(raw[:5], raw[5:7])
        if crc_rx != crc_calc:
            raise IOError(f"CRC mismatch (calc {crc_calc:04X} rx {crc_rx:04X}): {raw.hex()}")
        return value & 0xFFFF

    def _parse_read_float_response(self, raw: bytes) -> float:
        if len(raw) < 5:
            raise IOError(f"Short response ({len(raw)} bytes): {raw.hex()}")
        if raw[0] != self.slave_id:
            raise IOError(f"Unexpected slave in response: {raw.hex()}")
        if raw[1] & 0x80:
            raise IOError(f"Modbus exception: {raw.hex()}")
        if raw[1] != FC_READ_FLOAT:
            raise IOError(f"Unexpected function code: {raw.hex()}")
        if len(raw) < 9:
            raise IOError(f"Truncated float payload: {raw.hex()}")
        bc = raw[2]
        if bc != 4:
            raise IOError(f"Unexpected float byte count {bc}: {raw.hex()}")
        # Big-endian float (Modbus/register order hi word first typical)
        fbytes = raw[3:7]
        crc_calc, crc_rx = self._split_rx_crc(raw[:7], raw[7:9])
        if crc_rx != crc_calc:
            raise IOError(f"CRC mismatch float (calc {crc_calc:04X} rx {crc_rx:04X}): {raw.hex()}")
        return struct.unpack(">f", fbytes)[0]

    def read_full_map_safe(
        self,
        *,
        on_partial: Optional[Callable[[DeviceSnapshot, int], None]] = None,
    ) -> DeviceSnapshot:
        ts = time.time()
        err: Optional[str] = None
        trace: list[dict[str, Any]] = []

        def sig_dict(v: Optional[int]) -> Optional[dict]:
            if v is None:
                return None
            return decode_signal_word(v)

        def u16_hex(a: Optional[int]) -> Optional[str]:
            if a is None:
                return None
            return f"0x{a:04X}"

        def f32_hex(f: Optional[float]) -> Optional[str]:
            if f is None:
                return None
            return struct.pack(">f", f).hex().upper()

        def snapshot_from_regs(
            *,
            fw: Optional[int],
            ins: Optional[int],
            outs: Optional[int],
            inv: Optional[float],
            outv: Optional[float],
            sm: Optional[int],
            aio_hi: Optional[int],
            aio_lo: Optional[int],
            v_hi: Optional[int],
            v_lo: Optional[int],
            p_hi: Optional[int],
            p_lo: Optional[int],
            snap_err: Optional[str],
        ) -> DeviceSnapshot:
            hex_u16: dict[str, str] = {}
            for key, val in [
                ("40001", fw),
                ("40008", sm),
                ("40009", aio_hi),
                ("40010", aio_lo),
                ("40011", v_hi),
                ("40012", v_lo),
                ("40013", p_hi),
                ("40014", p_lo),
            ]:
                h = u16_hex(val)
                if h is not None:
                    hex_u16[key] = h
            for key, val in [("40002", ins), ("40003", outs)]:
                h = u16_hex(val)
                if h is not None:
                    hex_u16[key] = h
            hex_f: dict[str, str] = {}
            ih = f32_hex(inv)
            if ih is not None:
                hex_f["40004"] = ih
            oh = f32_hex(outv)
            if oh is not None:
                hex_f["40006"] = oh
            return DeviceSnapshot(
                ts=ts,
                firmware_u16=fw,
                input_signal=sig_dict(ins),
                output_signal=sig_dict(outs),
                input_value=inv,
                output_value=outv,
                soft_mode_u16=sm,
                active_io_user_hi=aio_hi,
                active_io_user_lo=aio_lo,
                volt_user_hi=v_hi,
                volt_user_lo=v_lo,
                passive_io_user_hi=p_hi,
                passive_io_user_lo=p_lo,
                error=snap_err,
                hex_u16=hex_u16,
                hex_float_ieee754_be=hex_f,
                rtu_trace=trace,
            )

        if self.block_read_first and self.read_with_count:
            try:
                nw = 14
                body = self._build_read_u16_block_body(REG_FIRMWARE, nw)
                with self._lock:
                    raw = self._exchange_traced(
                        body,
                        plc=REG_FIRMWARE,
                        kind="u16_block",
                        fc_label="0x64",
                        trace_out=trace,
                    )
                w = self._parse_read_u16_block_response(raw, nw)
                trace[-1]["parse_ok"] = True
                snap = snapshot_from_regs(
                    fw=w[0],
                    ins=w[1],
                    outs=w[2],
                    inv=struct.unpack(">f", struct.pack(">HH", w[3], w[4]))[0],
                    outv=struct.unpack(">f", struct.pack(">HH", w[5], w[6]))[0],
                    sm=w[7],
                    aio_hi=w[8],
                    aio_lo=w[9],
                    v_hi=w[10],
                    v_lo=w[11],
                    p_hi=w[12],
                    p_lo=w[13],
                    snap_err=None,
                )
                if on_partial is not None:
                    on_partial(snap, 12)
                return snap
            except Exception as e:
                if trace:
                    trace[-1]["parse_ok"] = False
                    trace[-1]["parse_err"] = str(e)

        def read_u16_one(addr: int) -> Optional[int]:
            nonlocal err
            body = self._build_read_u16_body(addr)
            try:
                with self._lock:
                    raw = self._exchange_traced(
                        body, plc=addr, kind="u16", fc_label="0x64", trace_out=trace
                    )
                v = self._parse_read_u16_response(raw)
                trace[-1]["parse_ok"] = True
                return v
            except Exception as e:
                if trace:
                    trace[-1]["parse_ok"] = False
                    trace[-1]["parse_err"] = str(e)
                if err is None:
                    err = str(e)
                return None

        def read_f32_one(addr: int) -> Optional[float]:
            nonlocal err
            body = self._build_read_float_body(addr)
            try:
                with self._lock:
                    raw = self._exchange_traced(
                        body, plc=addr, kind="float", fc_label="0x66", trace_out=trace
                    )
                v = self._parse_read_float_response(raw)
                trace[-1]["parse_ok"] = True
                return v
            except Exception as e:
                if trace:
                    trace[-1]["parse_ok"] = False
                    trace[-1]["parse_err"] = str(e)
                if err is None:
                    err = str(e)
                return None

        # 12 RTU round-trips (40001~40014 맵: float 2회 + u16 10회). 요청 사이 간격만 적용(마지막 뒤는 sleep 없음).
        steps: list[tuple[str, int]] = [
            ("u16", REG_FIRMWARE),
            ("u16", REG_INPUT_SIGNAL),
            ("u16", REG_OUTPUT_SIGNAL),
            ("f32", REG_INPUT_VALUE),
            ("f32", REG_OUTPUT_VALUE),
            ("u16", REG_SOFT_MODE),
            ("u16", REG_ACTIVE_IO_USER_HI),
            ("u16", REG_ACTIVE_IO_USER_LO),
            ("u16", REG_VOLT_USER_HI),
            ("u16", REG_VOLT_USER_LO),
            ("u16", REG_PASSIVE_IO_USER_HI),
            ("u16", REG_PASSIVE_IO_USER_LO),
        ]
        def unpack12(vals: list[Optional[Any]]) -> tuple:
            pad_n: list[Optional[Any]] = [None] * 12
            for j in range(min(len(vals), 12)):
                pad_n[j] = vals[j]
            return tuple(pad_n)

        seq: list[Optional[Any]] = []
        d = self.inter_request_delay_s
        n = len(steps)
        fw: Optional[int]
        ins: Optional[int]
        outs: Optional[int]
        inv: Optional[float]
        outv: Optional[float]
        sm: Optional[int]
        aio_hi: Optional[int]
        aio_lo: Optional[int]
        v_hi: Optional[int]
        v_lo: Optional[int]
        p_hi: Optional[int]
        p_lo: Optional[int]

        for i, (kind, addr) in enumerate(steps):
            if kind == "u16":
                seq.append(read_u16_one(addr))
            else:
                seq.append(read_f32_one(addr))
            (
                fw,
                ins,
                outs,
                inv,
                outv,
                sm,
                aio_hi,
                aio_lo,
                v_hi,
                v_lo,
                p_hi,
                p_lo,
            ) = unpack12(seq)
            if on_partial is not None:
                on_partial(
                    snapshot_from_regs(
                        fw=fw,
                        ins=ins,
                        outs=outs,
                        inv=inv,
                        outv=outv,
                        sm=sm,
                        aio_hi=aio_hi,
                        aio_lo=aio_lo,
                        v_hi=v_hi,
                        v_lo=v_lo,
                        p_hi=p_hi,
                        p_lo=p_lo,
                        snap_err=err,
                    ),
                    i + 1,
                )
            if d > 0 and i < n - 1:
                time.sleep(d)

        return snapshot_from_regs(
            fw=fw,
            ins=ins,
            outs=outs,
            inv=inv,
            outv=outv,
            sm=sm,
            aio_hi=aio_hi,
            aio_lo=aio_lo,
            v_hi=v_hi,
            v_lo=v_lo,
            p_hi=p_hi,
            p_lo=p_lo,
            snap_err=err,
        )
