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
from typing import Any, Optional

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


def append_crc(payload: bytes) -> bytes:
    c = crc16_modbus(payload)
    return payload + bytes([c & 0xFF, (c >> 8) & 0xFF])


FC_READ_U16 = 0x64
FC_READ_FLOAT = 0x66

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
        baudrate: int = 19200,
        bytesize: int = serial.EIGHTBITS,
        parity: str = serial.PARITY_NONE,
        stopbits: int = serial.STOPBITS_ONE,
        timeout_s: float = 0.35,
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
        frame = append_crc(bytes([self.slave_id]) + pdu_body)
        self._ser.reset_input_buffer()
        self._ser.write(frame)
        self._ser.flush()
        # Read full response: variable length; wait for at least 5 bytes then drain
        deadline = time.monotonic() + max(self.timeout_s, 0.5)
        buf = bytearray()
        while time.monotonic() < deadline:
            chunk = self._ser.read(256)
            if chunk:
                buf.extend(chunk)
                if len(buf) >= 5:
                    # minimal response: addr fc ... crc2
                    break
            else:
                time.sleep(0.01)
        # brief trailing read for slow links
        time.sleep(0.02)
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
        tx = append_crc(bytes([self.slave_id]) + pdu_body)
        self._ser.reset_input_buffer()
        self._ser.write(tx)
        self._ser.flush()
        deadline = time.monotonic() + max(self.timeout_s, 0.5)
        buf = bytearray()
        while time.monotonic() < deadline:
            chunk = self._ser.read(256)
            if chunk:
                buf.extend(chunk)
                if len(buf) >= 5:
                    break
            else:
                time.sleep(0.01)
        time.sleep(0.02)
        buf.extend(self._ser.read(256))
        rx = bytes(buf)

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
        }
        trace_out.append(row)
        return rx

    def read_u16(self, plc_address: int) -> int:
        """Read one uint16 from holding map address (4xxxx style)."""
        body = bytes([FC_READ_U16]) + plc_addr_to_pdu(plc_address)
        with self._lock:
            raw = self._transceive(body)
        return self._parse_read_u16_response(raw)

    def read_float32(self, plc_address: int) -> float:
        """Read float starting at given address (occupies two addresses)."""
        body = bytes([FC_READ_FLOAT]) + plc_addr_to_pdu(plc_address)
        with self._lock:
            raw = self._transceive(body)
        return self._parse_read_float_response(raw)

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
        crc_rx = raw[5] | (raw[6] << 8)
        crc_calc = crc16_modbus(raw[:5])
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
        crc_rx = raw[7] | (raw[8] << 8)
        crc_calc = crc16_modbus(raw[:7])
        if crc_rx != crc_calc:
            raise IOError(f"CRC mismatch float (calc {crc_calc:04X} rx {crc_rx:04X}): {raw.hex()}")
        return struct.unpack(">f", fbytes)[0]

    def read_full_map_safe(self) -> DeviceSnapshot:
        ts = time.time()
        err: Optional[str] = None
        trace: list[dict[str, Any]] = []

        def g_u16(addr: int) -> Optional[int]:
            nonlocal err
            body = bytes([FC_READ_U16]) + plc_addr_to_pdu(addr)
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

        def g_f32(addr: int) -> Optional[float]:
            nonlocal err
            body = bytes([FC_READ_FLOAT]) + plc_addr_to_pdu(addr)
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

        fw = g_u16(REG_FIRMWARE)
        ins = g_u16(REG_INPUT_SIGNAL)
        outs = g_u16(REG_OUTPUT_SIGNAL)
        inv = g_f32(REG_INPUT_VALUE)
        outv = g_f32(REG_OUTPUT_VALUE)
        sm = g_u16(REG_SOFT_MODE)
        aio_hi = g_u16(REG_ACTIVE_IO_USER_HI)
        aio_lo = g_u16(REG_ACTIVE_IO_USER_LO)
        v_hi = g_u16(REG_VOLT_USER_HI)
        v_lo = g_u16(REG_VOLT_USER_LO)
        p_hi = g_u16(REG_PASSIVE_IO_USER_HI)
        p_lo = g_u16(REG_PASSIVE_IO_USER_LO)

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
            error=err,
            hex_u16=hex_u16,
            hex_float_ieee754_be=hex_f,
            rtu_trace=trace,
        )
