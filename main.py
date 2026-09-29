"""
I2S DAQ Web-Interface (FastAPI + echte Hardware)
================================================
Steuert die I2S-DAQ über /dev/mem (mmap), liest Live-Daten aus dem DMA-Ring
und stellt sie per WebSocket + REST bereit.

Hardware (siehe i2s_ctrl.py / record_i2s.py):
  I2S-Register  0x43C00000 (4 KB)
  DMA-Register  0x43C10000 (64 KB)
  Datenpuffer   0x1F000000 (256 MiB, reserved memory)

Run:
    python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import base64
import json
import math
import mmap
import os
import queue
import struct
import threading
import time
from collections import deque
from typing import Dict, List, Optional

import numpy as np
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Hardware-Konstanten
# ---------------------------------------------------------------------------

I2S_BASE = 0x43C00000
DMA_BASE = 0x43C10000
DMA_BUF = 0x1F000000
DMA_BUF_SZ = 0x10000000          # 256 MiB

# I2S-Register-Offsets
OFF = {
    "CONTROL": 0x00, "STATUS": 0x04, "NUM_ACTIVE": 0x08, "VERSION": 0x0C,
    "CH_EN0": 0x10, "CH_EN1": 0x14, "CH_EN2": 0x18,
    "WARMUP": 0x1C, "FRAME_COUNT": 0x20, "OVERRUN_COUNT": 0x24,
    "FRAME_BYTES": 0x28,
}

CTRL_PREPARE = 1 << 0
CTRL_RECORD = 1 << 1
CTRL_STOP = 1 << 2
CTRL_SRESET = 1 << 3

# DMA (S2MM-Kanal-Offset 0x30)
DMACR = 0x30
DMASR = 0x34
CURDESC = 0x38
TAILDESC = 0x40

DMACR_RS = 1 << 0
DMACR_RESET = 1 << 2
DMACR_CYCLIC = 1 << 4
DMACR_IOC_IrqEn = 1 << 12
DMACR_Err_IrqEn = 1 << 14

DESC_SZ = 64
DESC_NEXT = 0x00
DESC_BUF = 0x08
DESC_CONTROL = 0x18
DESC_STATUS = 0x1C

STATE_NAMES = {0: "IDLE", 1: "PREPARED", 2: "RECORDING", 3: "STOPPING"}

NUM_CHANNELS = 80
SAMPLE_RATE = 96000

# Angezeigte App-/Firmware-Version (Software-Release, unabhängig vom HW-Register)
APP_VERSION = "0.0.2b"


def ring_stride(frame_bytes: int) -> int:
    """Slot-Größe = kgV(frame_bytes, 64).

    Jeder DMA-Deskriptor-Slot fasst damit ganze Frames und ist 64-Byte-aligned.
    Folge: Ab jeder Descriptor-Buffer-Adresse (= Frame-Grenze) kann sauber
    frame_bytes-weise dekodiert werden, kein Versatz mehr über Slot-Grenzen
    (stride-Fix, Commit 112f0ee).
    """
    return frame_bytes * 64 // math.gcd(frame_bytes, 64)


# ---------------------------------------------------------------------------
# Hardware-Zugriff (einmalige mmaps, von allen Threads geteilt)
# ---------------------------------------------------------------------------

class Hardware:
    def __init__(self):
        self._lock = threading.Lock()
        self._f = open("/dev/mem", "rb+", buffering=0)
        self.i2s = mmap.mmap(self._f.fileno(), 0x1000, offset=I2S_BASE)
        self.dma = mmap.mmap(self._f.fileno(), 0x1000, offset=DMA_BASE)
        self.buf = mmap.mmap(self._f.fileno(), DMA_BUF_SZ, offset=DMA_BUF)
        # Ring-Konfiguration (wird beim Start gesetzt, hier Defaults)
        self.n_desc = 8192
        self.stride = 64
        self.frame_bytes = 24
        self.data_ring_start = 0     # Byte-Offset des Datenrings im buf
        self.data_ring_size = 0      # Größe des Datenrings in Bytes

    # -- Register IO -----------------------------------------------------
    def rd(self, m, off):
        return struct.unpack_from("<I", m, off)[0]

    def wr(self, m, off, val):
        struct.pack_into("<I", m, off, val & 0xFFFFFFFF)

    # -- Status ----------------------------------------------------------
    def read_status(self):
        with self._lock:
            v = self.rd(self.i2s, OFF["STATUS"])
        st = v & 0x7
        return {
            "state": STATE_NAMES.get(st, "?%d" % st),
            "state_num": st,
            "clk_active": bool(v & 0x08),
            "warmup_done": bool(v & 0x10),
            "stream_stall": bool(v & 0x20),
            "overrun_sticky": bool(v & 0x40),
            "cmd_err": bool(v & 0x80),
            "raw": v,
        }

    def read_counters(self):
        with self._lock:
            na = self.rd(self.i2s, OFF["NUM_ACTIVE"])
            fc = self.rd(self.i2s, OFF["FRAME_COUNT"])
            ov = self.rd(self.i2s, OFF["OVERRUN_COUNT"])
            fb = self.rd(self.i2s, OFF["FRAME_BYTES"])
        return {
            "num_active": na,
            "version": APP_VERSION,
            "frame_count": fc,
            "overrun_count": ov,
            "frame_bytes": fb,
        }

    def read_channel_mask(self):
        with self._lock:
            e0 = self.rd(self.i2s, OFF["CH_EN0"])
            e1 = self.rd(self.i2s, OFF["CH_EN1"])
            e2 = self.rd(self.i2s, OFF["CH_EN2"])
        return e0 | (e1 << 32) | (e2 << 64)

    # -- Control ---------------------------------------------------------
    def set_channels(self, mask: int):
        e0 = mask & 0xFFFFFFFF
        e1 = (mask >> 32) & 0xFFFFFFFF
        e2 = (mask >> 64) & 0xFFFF
        with self._lock:
            self.wr(self.i2s, OFF["CH_EN0"], e0)
            self.wr(self.i2s, OFF["CH_EN1"], e1)
            self.wr(self.i2s, OFF["CH_EN2"], e2)
            # frame_bytes / stride folgen der Kanalzahl
            n = bin(mask).count("1")
            self.frame_bytes = 16 + 8 * n
            self.stride = ring_stride(self.frame_bytes)

    def control(self, cmd: str):
        with self._lock:
            if cmd == "prepare":
                self.wr(self.i2s, OFF["CONTROL"], CTRL_PREPARE)
            elif cmd == "record":
                self.wr(self.i2s, OFF["CONTROL"], CTRL_RECORD)
            elif cmd == "stop":
                self.wr(self.i2s, OFF["CONTROL"], CTRL_STOP)
            elif cmd == "reset":
                self.wr(self.i2s, OFF["CONTROL"], CTRL_SRESET)

    def clear_sticky(self):
        """Loescht die Sticky-Bits OVERRUN_STICKY (Bit6) und CMD_ERR (Bit7).

        Diese Bits sind historische Latches: sie bleiben nach einem einzelnen
        Ereignis fuer immer gesetzt, bis sie explizit geloescht werden (HDL
        i2s_axi_regs.v: Schreiben von Bit6/Bit7 ins STATUS-Register 0x04).
        Ohne Loeschen zeigt die UI dauerhaft "overrun yes", obwohl aktuell
        kein Overrun mehr auftritt (overrun_count bleibt 0).
        """
        with self._lock:
            self.wr(self.i2s, OFF["STATUS"], (1 << 6) | (1 << 7))


hw = Hardware()


def detect_ring_params() -> bool:
    """
    Leitet Datenring-Start und -Größe aus dem bereits programmierten
    DMA-Deskriptor-Ring ab (desc_base = DMA_BUF + 0x100000). Robust gegen
    Server-Neustarts, wo der Ring schon läuft, aber data_ring_size == 0 ist.
    """
    desc_base = DMA_BUF + 0x100000
    d0 = desc_base - DMA_BUF
    # ersten und zweiten Deskriptor-Buffer lesen -> stride
    buf0 = hw.rd(hw.buf, d0 + DESC_BUF)
    buf1 = hw.rd(hw.buf, d0 + DESC_SZ + DESC_BUF)
    stride = buf1 - buf0
    if not (64 <= stride <= 0x100000):
        return False

    # n_desc durch Folgen der next-Pointer ermitteln (Cyclic-Ring),
    # bis wir wieder beim Start-Deskriptor ankommen.
    n = 0
    cur = desc_base
    while n < 100000:
        nxt = hw.rd(hw.buf, (cur - DMA_BUF) + DESC_NEXT)
        n += 1
        cur = nxt
        if cur == desc_base:
            break
        if cur < desc_base or cur >= desc_base + 0x100000:
            n = 0
            break

    if n <= 0:
        return False

    data_base = buf0
    hw.n_desc = n
    hw.stride = stride
    hw.data_ring_start = data_base - DMA_BUF
    hw.data_ring_size = n * stride
    return True


# ---------------------------------------------------------------------------
# DMA-Ring-Programmierung (analog record_i2s.py)
# ---------------------------------------------------------------------------

def program_dma_ring(n_desc: int = 8192) -> None:
    """Programmiert den Cyclic-Ring und startet den DMA-Kanal."""
    channels = bin(hw.read_channel_mask()).count("1") or 1
    frame_bytes = 16 + 8 * channels
    stride = ring_stride(frame_bytes)

    # Deskriptoren liegen fix ab +1 MiB; Datenring danach (64K-aligned).
    # n_desc so begrenzen, dass Deskriptoren + Datenring sicher in den
    # Puffer passen (stride wächst mit kgV(frame_bytes,64) stark an).
    desc_base = DMA_BUF + 0x100000
    overhead = 0x100000 + 0x10000          # +1 MiB Descs + max. 64K-Align
    max_n = (DMA_BUF_SZ - overhead) // (stride + DESC_SZ)
    n_desc = max(1, min(n_desc, max_n))

    hw.n_desc = n_desc
    hw.frame_bytes = frame_bytes
    hw.stride = stride

    data_base = desc_base + n_desc * DESC_SZ
    data_base = (data_base + 0xFFFF) & ~0xFFFF
    # Datenring-Grenzen festhalten (wichtig fürs rückwärts-Wrap-Lesen)
    hw.data_ring_start = data_base - DMA_BUF
    hw.data_ring_size = n_desc * stride

    for i in range(n_desc):
        daddr = desc_base + i * DESC_SZ
        baddr = data_base + i * stride
        nxt = desc_base + ((i + 1) % n_desc) * DESC_SZ
        doff = daddr - DMA_BUF
        hw.wr(hw.buf, doff + DESC_NEXT, nxt)
        hw.wr(hw.buf, doff + DESC_NEXT + 4, 0)
        hw.wr(hw.buf, doff + DESC_BUF, baddr)
        hw.wr(hw.buf, doff + DESC_BUF + 4, 0)
        hw.wr(hw.buf, doff + DESC_CONTROL, stride)
        hw.wr(hw.buf, doff + DESC_STATUS, 0)

    # DMA reset
    hw.wr(hw.dma, DMACR, DMACR_RESET)
    for _ in range(100):
        if not (hw.rd(hw.dma, DMACR) & DMACR_RESET):
            break
        time.sleep(0.001)
    hw.wr(hw.dma, DMACR, 0)
    time.sleep(0.01)

    hw.wr(hw.dma, CURDESC, desc_base)
    hw.wr(hw.dma, CURDESC + 4, 0)
    hw.wr(hw.dma, DMACR,
          DMACR_RS | DMACR_CYCLIC | DMACR_IOC_IrqEn | DMACR_Err_IrqEn)

    dummy = desc_base + n_desc * DESC_SZ
    hw.wr(hw.dma, TAILDESC, dummy)
    hw.wr(hw.dma, TAILDESC + 4, 0)


# ---------------------------------------------------------------------------
# App-/UI-Zustand + Recording
# ---------------------------------------------------------------------------

class AppState:
    def __init__(self):
        self.display_channels: List[int] = [1, 2, 3, 4, 5, 6, 7, 8]  # im Oszilloskop angezeigt
        self.view_frames: int = 400                                   # Anzahl Frames im Live-Fenster
        self.monitor_channel: int = 0                                 # 0 = kein Monitor aktiv
        self.recording: Optional[dict] = None  # {file, stop_event, thread, started, duration}

    def to_dict(self):
        return {
            "display_channels": self.display_channels,
            "view_frames": self.view_frames,
            "monitor_channel": self.monitor_channel,
            "settings": getattr(self, "settings", None),
            "recording": None if not self.recording else {
                "duration": self.recording.get("duration"),
                "started": self.recording.get("started"),
                "filename": self.recording.get("filename"),
            },
        }


appstate = AppState()


# ---------------------------------------------------------------------------
# Einstellungen (Preferences) — JSON-Format
# ---------------------------------------------------------------------------
#
# JSON-Schema (schema = "mooga.multidecoder.settings"):
# {
#   "schema": "mooga.multidecoder.settings",
#   "version": 1,
#   "active_channels": [1, 2, 3, ...],          # Liste aktiver Kanäle (1..80)
#   "sensitivity": {                              # pro Kanal (String-Key "1".."80")
#     "1": { "value": 1.0, "unit": "µV", "name": "Temp 1" },
#     ...
#   }
# }
#
# - `value`  : float, Umrechnungsfaktor Roh-Sample -> physikalische Einheit
#              (wird nur im Live-View zum Skalieren + Y-Achsen-Beschriftung genutzt)
# - `unit`   : String, Einheit für die Y-Achse
# - `name`   : String, Kanalname (max. 10 Zeichen in der UI gekürzt)

SETTINGS_SCHEMA = "mooga.multidecoder.settings"
SETTINGS_FILE = "mooga_settings.json"  # auf dem Gerät (relativ zum Arbeitsverzeichnis)


def _default_settings() -> dict:
    return {
        "schema": SETTINGS_SCHEMA,
        "version": 1,
        "active_channels": [1, 2, 3, 4, 5, 6, 7, 8],
        "sensitivity": {str(c): {"value": 1.0, "unit": "", "name": ""} for c in range(1, NUM_CHANNELS + 1)},
    }


def load_settings_from_file() -> dict:
    """Lädt Einstellungen vom Gerät (SETTINGS_FILE). Bei Fehler Defaults."""
    try:
        with open(SETTINGS_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, dict) and data.get("schema") == SETTINGS_SCHEMA:
            return data
    except Exception:
        pass
    return _default_settings()


def save_settings_to_file(data: dict) -> None:
    """Speichert Einstellungen auf dem Gerät (SETTINGS_FILE)."""
    with open(SETTINGS_FILE, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _validate_settings(data: dict) -> dict:
    """Normalisiert/validiert ein Einstellungs-Dict auf ein sauberes Format."""
    out = _default_settings()
    if not isinstance(data, dict):
        return out
    out["schema"] = SETTINGS_SCHEMA
    out["version"] = int(data.get("version", 1)) or 1

    # active_channels
    ac = data.get("active_channels")
    if isinstance(ac, list):
        chans = [int(c) for c in ac if str(c).lstrip("-").isdigit() and 1 <= int(c) <= NUM_CHANNELS]
        out["active_channels"] = sorted(set(chans))

    # sensitivity
    sens = data.get("sensitivity")
    if isinstance(sens, dict):
        for c in range(1, NUM_CHANNELS + 1):
            key = str(c)
            entry = sens.get(key)
            if isinstance(entry, dict):
                val = entry.get("value", 1.0)
                try:
                    val = float(val)
                except (TypeError, ValueError):
                    val = 1.0
                out["sensitivity"][key] = {
                    "value": val,
                    "unit": str(entry.get("unit", "")),
                    "name": str(entry.get("name", "")),
                }
    return out


# Einstellungen im RAM halten (Session). Beim Start vom Gerät laden.
appstate.settings = load_settings_from_file()


def _ui_state() -> dict:
    """Konsistente UI-Zustands-Sicht: aktive Kanäle (aus Hardware-Maske),
    Anzeige-Kanäle und Monitor-Kanal (aus appstate)."""
    mask = hw.read_channel_mask()
    enabled = [i + 1 for i in range(NUM_CHANNELS) if mask & (1 << i)]
    return {
        "enabled_channels": enabled,
        "channel_mask": mask,
        "display_channels": appstate.display_channels,
        "settings": appstate.settings,
    }


def _frames_to_pcm24(chunk: bytes, nch: int) -> bytes:
    """Wandelt einen Byte-Block ganzer Frames in 24-bit interleaved PCM.

    Frame-Format: Timestamp(16B) + N×(CH_ID 4B + DATA 4B), DATA 24-bit
    signed MSB-aligned. Rückgabe: 3 Bytes pro Sample, little-endian,
    Kanäle verschachtelt.
    """
    frame_bytes = 16 + 8 * nch
    n = len(chunk) // frame_bytes
    if n <= 0:
        return b""
    arr = np.frombuffer(chunk[:n * frame_bytes], dtype="<u4").reshape(n, 4 + 2 * nch)
    v24 = (arr[:, 5::2].astype(np.int32) >> 8).reshape(-1)  # 24-bit signed, interleaved
    v = v24 & 0xFFFFFF
    pcm = np.empty((v.size, 3), dtype=np.uint8)
    pcm[:, 0] = (v & 0xFF).astype(np.uint8)
    pcm[:, 1] = ((v >> 8) & 0xFF).astype(np.uint8)
    pcm[:, 2] = ((v >> 16) & 0xFF).astype(np.uint8)
    return pcm.tobytes()


def _wav_fmt_extensible(nch: int, bits: int = 24) -> bytes:
    """Baut den fmt-Chunk als WAVE_FORMAT_EXTENSIBLE (0xFFFE) mit PCM-SubFormat.

    Standard-PCM (Format-Code 1) wird von den meisten Playern nur als Mono/
    Stereo interpretiert. EXTENSIBLE ist nötig für >2 Kanäle und trägt eine
    explizite Kanal-Maske + wValidBitsPerSample.
    """
    block_align = nch * 3
    byte_rate = SAMPLE_RATE * block_align
    # Kanal-Maske: Bit i = Kanal i+1. Bei >32 Kanälen nicht per uint32 darstellbar
    # -> 0 (Layout undefiniert), die Kanalzahl selbst trägt die Information.
    channel_mask = (1 << nch) - 1 if nch <= 32 else 0
    # KSDATAFORMAT_SUBTYPE_PCM GUID {00000001-0000-0010-8000-00AA00389B71}
    subformat = struct.pack(
        "<IHH8s", 0x00000001, 0x0000, 0x0010,
        bytes([0x80, 0x00, 0x00, 0xAA, 0x00, 0x38, 0x9B, 0x71]),
    )
    payload = struct.pack(
        "<HHIIHHHHI",
        0xFFFE,           # wFormatTag = WAVE_FORMAT_EXTENSIBLE
        nch,              # nChannels
        SAMPLE_RATE,      # nSamplesPerSec
        byte_rate,        # nAvgBytesPerSec
        block_align,      # nBlockAlign
        bits,             # wBitsPerSample
        22,               # cbSize
        bits,             # wValidBitsPerSample
        channel_mask,     # dwChannelMask
    ) + subformat
    return b"fmt " + struct.pack("<I", len(payload)) + payload


def _wav_header(nch: int) -> bytes:
    """WAV-Header (24-bit PCM, WAVE_FORMAT_EXTENSIBLE) mit Platzhalter-Größen.

    Aufbau: RIFF(12) + fmt(8 + 40) + data(8) = 68 Byte Header.
    """
    bits = 24
    fmt = _wav_fmt_extensible(nch, bits)
    return struct.pack("<4sI4s", b"RIFF", 0, b"WAVE") + fmt + struct.pack("<4sI", b"data", 0)


def _wav_header_stream(nch: int) -> bytes:
    """WAV-Header für Live-Streaming: RIFF-/data-Größe = 0xFFFFFFFF (unbekannt).

    Erlaubt Playern das Abspielen, während noch Daten nachkommen (Streaming).
    """
    bits = 24
    fmt = _wav_fmt_extensible(nch, bits)
    return struct.pack("<4sI4s", b"RIFF", 0xFFFFFFFF, b"WAVE") + fmt + struct.pack("<4sI", b"data", 0xFFFFFFFF)


def _recording_worker(duration: float, filename: str, stop_event: threading.Event):
    """
    Nimmt die aktiven Kanäle als 24-bit-PCM-WAV auf. Liest inkrementell aus
    dem Datenring (nur neue Samples seit dem letzten Mal) und schreibt sie in
    verschachtelter Reihenfolge (interleaved) pro Kanal.
    """
    start = time.time()
    # Kanalzahl autoritativ aus der Maske ableiten (Register kann noch leer sein)
    nch = max(1, bin(hw.read_channel_mask()).count("1"))
    frame_bytes = 16 + 8 * nch

    header = _wav_header(nch)
    last_rel = get_write_rel()   # Startposition

    with open(filename, "wb") as f:
        f.write(header)
        while not stop_event.is_set() and (time.time() - start) < duration:
            rel = get_write_rel()
            if rel < 0:
                time.sleep(0.01)
                continue
            size = hw.data_ring_size or DMA_BUF_SZ
            # Anzahl neuer Bytes seit letztem Lauf (mit Wrap)
            new_bytes = (rel - last_rel) % size
            # auf ganze Frames abrunden
            new_bytes = (new_bytes // frame_bytes) * frame_bytes
            if new_bytes <= 0:
                time.sleep(0.005)
                continue
            chunk = _read_ring_from(last_rel, new_bytes)
            f.write(_frames_to_pcm24(chunk, nch))
            last_rel = (last_rel + new_bytes) % size
            time.sleep(0.005)

    # Header-Größen patchen (Header ist jetzt 68 Byte, data-Size-Feld bei Offset 64)
    header_len = len(header)
    data_size = 0
    with open(filename, "rb") as f:
        data_size = f.seek(0, 2) - header_len
    with open(filename, "r+b") as f:
        f.seek(4)
        f.write(struct.pack("<I", (header_len - 8) + data_size))
        f.seek(header_len - 4)
        f.write(struct.pack("<I", data_size))

    appstate.recording = None


def start_recording(duration: float):
    if appstate.recording:
        return None
    filename = "/tmp/i2s_rec_%d.wav" % int(time.time())
    stop_event = threading.Event()
    t = threading.Thread(target=_recording_worker, args=(duration, filename, stop_event), daemon=True)
    appstate.recording = {
        "duration": duration, "started": time.time(),
        "filename": filename, "stop_event": stop_event, "thread": t,
    }
    t.start()
    return filename


# ---------------------------------------------------------------------------
# Live-Daten lesen (Byte-Strom aus dem Ring, rückwärts von CURDESC)
# ---------------------------------------------------------------------------

def get_write_position() -> int:
    """Aktuelle Schreibposition des DMA im Ring (Byte-Offset in buf)."""
    cur = hw.rd(hw.dma, CURDESC)
    if cur < DMA_BUF or cur >= DMA_BUF + DMA_BUF_SZ:
        return -1
    doff = cur - DMA_BUF
    baddr = hw.rd(hw.buf, doff + DESC_BUF)
    return baddr - DMA_BUF if DMA_BUF <= baddr < DMA_BUF + DMA_BUF_SZ else -1


def _read_ring_bytes(n_bytes: int) -> bytes:
    """Liest die letzten `n_bytes` Bytes des Datenrings (korrekt begrenzt auf
    den tatsächlichen Ring, mit Wrap am Ringende)."""
    pos = get_write_position()
    if pos < 0:
        return b""
    start = hw.data_ring_start
    size = hw.data_ring_size
    if size <= 0:
        # Fallback: ganzer 16-MiB-Bereich, falls Ring noch nicht programmiert
        start, size = 0, DMA_BUF_SZ

    n_bytes = min(n_bytes, size)
    # Schreibposition innerhalb des Rings normalisieren
    rel = (pos - start) % size
    begin = (rel - n_bytes) % size

    abs_begin = start + begin
    abs_end = start + begin + n_bytes
    if abs_end <= start + size:
        return bytes(hw.buf[abs_begin:abs_end])
    else:
        first = (start + size) - abs_begin
        return bytes(hw.buf[abs_begin:start + size]) + bytes(hw.buf[start:start + (n_bytes - first)])


def _read_ring_from(offset: int, n_bytes: int) -> bytes:
    """Liest `n_bytes` Bytes ab einem absoluten Ring-Offset (mit Wrap)."""
    start = hw.data_ring_start
    size = hw.data_ring_size
    if size <= 0:
        start, size = 0, DMA_BUF_SZ
    n_bytes = min(n_bytes, size)
    rel = offset % size
    abs_begin = start + rel
    abs_end = start + rel + n_bytes
    if abs_end <= start + size:
        return bytes(hw.buf[abs_begin:abs_end])
    first = (start + size) - abs_begin
    return bytes(hw.buf[abs_begin:start + size]) + bytes(hw.buf[start:start + (n_bytes - first)])


def get_write_rel() -> int:
    """Schreibposition als relativer Offset innerhalb des Datenrings."""
    pos = get_write_position()
    if pos < 0:
        return -1
    start = hw.data_ring_start
    size = hw.data_ring_size
    if size <= 0:
        return pos
    return (pos - start) % size


def _find_frame_start(raw: bytes, frame_bytes: int, num_active: int) -> int:
    """
    Findet den Byte-Offset innerhalb von `raw`, an dem ein vollständiger Frame
    beginnt. Die gelesenen Bytes sind nicht frame-ausgerichtet (Schreibposition
    ist 64-Byte-aligned, Frames sind z.B. 24 Byte). Wir suchen einen Offset, an
    dem die Kanal-ID-Sequenz gültig (1..80, aufsteigend) und über zwei
    aufeinanderfolgende Frames konsistent ist.
    """
    nch = max(1, num_active)
    for off in range(frame_bytes):
        if off + 16 + 8 * nch > len(raw):
            break
        ids = [raw[off + 16 + k * 8] for k in range(nch)]
        # gültige Kanal-IDs, aufsteigend
        if not all(1 <= x <= 80 for x in ids):
            continue
        if ids != sorted(ids):
            continue
        # Konsistenz über einen zweiten Frame prüfen (falls vorhanden)
        off2 = off + frame_bytes
        if off2 + 16 + 8 * nch <= len(raw):
            ids2 = [raw[off2 + 16 + k * 8] for k in range(nch)]
            if ids2 == ids:
                return off
        else:
            return off
    return 0


def _read_aligned(num_frames: int):
    """Liest einen um eine Frame-Größe erweiterten Block und richtet ihn auf
    eine Frame-Grenze aus. Gibt (raw, num_active, frame_bytes) zurück.

    Kanalzahl wird aus der Hardware-Maske abgeleitet (wie im WAV-Export) statt
    aus dem FRAME_BYTES/NUM_ACTIVE-Register, das erst nach prepare valide ist
    und sonst stale/leer sein kann.
    """
    num_active = max(1, bin(hw.read_channel_mask()).count("1"))
    frame_bytes = 16 + 8 * num_active
    if frame_bytes <= 0:
        return b"", num_active, frame_bytes
    raw = _read_ring_bytes(num_frames * frame_bytes + frame_bytes)
    if len(raw) < frame_bytes:
        return b"", num_active, frame_bytes
    off = _find_frame_start(raw, frame_bytes, num_active)
    return raw[off:], num_active, frame_bytes




def read_recent_frames(num_frames: int) -> List[dict]:
    """
    Liest die letzten `num_frames` Frames aus dem Ring als Byte-Strom.
    Geht von der Schreibposition rückwärts. Kanalzahl aus der Maske.
    Gibt eine Liste von Frame-Dicts zurück (zeitlich aufsteigend).
    """
    num_active = max(1, bin(hw.read_channel_mask()).count("1"))
    frame_bytes = 16 + 8 * num_active
    if frame_bytes <= 0:
        return []

    raw = _read_ring_bytes(num_frames * frame_bytes)
    n = len(raw) // frame_bytes
    if n <= 0:
        return []

    # Frames rückwärts parsen (letzter Frame zuerst, dann zeitlich aufsteigend)
    frames = []
    for i in range(n):
        off = len(raw) - (i + 1) * frame_bytes
        chunk = raw[off:off + frame_bytes]
        ch = []
        for k in range(num_active):
            cid = struct.unpack_from("<I", chunk, 16 + k * 8)[0] & 0xFF
            val = struct.unpack_from("<i", chunk, 20 + k * 8)[0] >> 8
            ch.append({"ch": cid, "v": val})
        frames.append({"channels": ch})
    frames.reverse()
    return frames


MAX_LIVE_POINTS = 1500  # max. Samples pro Kanal pro Update (Dezimierung)


def read_channel_series(wanted: List[int], num_frames: int) -> Dict[int, List[int]]:
    """Schneller numpy-vektorisierter Live-Pfad für die gewünschten Kanäle."""
    raw, na, frame_bytes = _read_aligned(num_frames)
    if frame_bytes <= 0 or na <= 0:
        return {c: [] for c in wanted}

    n = len(raw) // frame_bytes
    if n <= 0:
        return {c: [] for c in wanted}

    arr = np.frombuffer(raw[:n * frame_bytes], dtype="<u4").reshape(n, 4 + 2 * na)
    chid = arr[0, 4::2] & 0xFF
    data = arr[:, 5::2].astype(np.int32) >> 8

    if n > MAX_LIVE_POINTS:
        step = (n + MAX_LIVE_POINTS - 1) // MAX_LIVE_POINTS
        data = data[::step]

    result = {}
    for c in wanted:
        idx = np.where(chid == c)[0]
        if idx.size:
            result[c] = data[:, idx[0]].tolist()
        else:
            result[c] = []
    return result


MONITOR_CHUNK = 960  # Samples pro Audio-Update (~10 ms bei 96 kHz)
MONITOR_GAIN = 150   # Verstärkung (Signal ist nur ~0,4 % vom 24-bit-Vollausschlag)


class MonitorReader:
    """Inkrementeller Leser für den Audio-Monitor.

    Der alte Ansatz las jedes Mal ein festes Fenster der letzten 960 Samples
    (≈10 ms). Da die WebSocket-Schleife aber ≈20 ms pro Durchlauf braucht
    (10 ms receive_timeout + 10 ms sleep + Register-Reads), wurde nur die
    Hälfte der anfallenden Daten geliefert → hörbare Lücken („abgehackt").

    Dieser Reader verfolgt die Ring-Leseposition und liefert lückenlos alle
    seit dem letzten Aufruf neu geschriebenen Samples — unabhängig von der
    Tick-Rate (gleiches Prinzip wie der funktionierende WAV-Export).

    Rückgabe als int16-Array (bereits mit Gain + Clipping skaliert), damit der
    WebSocket-Endpoint die Daten als kompaktes base64 senden kann statt einer
    langsamen JSON-Float-Liste.
    """

    def __init__(self):
        self._cursor = None

    def reset(self):
        self._cursor = None

    def read(self, channel: int) -> np.ndarray:
        rel = get_write_rel()
        if rel < 0:
            return np.empty(0, dtype=np.int16)
        nch = max(1, bin(hw.read_channel_mask()).count("1"))
        frame_bytes = 16 + 8 * nch
        size = hw.data_ring_size or DMA_BUF_SZ

        if self._cursor is None:
            self._cursor = rel
            return np.empty(0, dtype=np.int16)

        new_bytes = (rel - self._cursor) % size
        new_bytes = (new_bytes // frame_bytes) * frame_bytes
        if new_bytes <= 0:
            return np.empty(0, dtype=np.int16)

        chunk = _read_ring_from(self._cursor, new_bytes)
        self._cursor = (self._cursor + new_bytes) % size

        n = len(chunk) // frame_bytes
        if n <= 0:
            return np.empty(0, dtype=np.int16)

        arr = np.frombuffer(chunk[:n * frame_bytes], dtype="<u4").reshape(n, 4 + 2 * nch)
        chid = arr[0, 4::2] & 0xFF
        data = arr[:, 5::2].astype(np.int32) >> 8

        idx = np.where(chid == channel)[0]
        if idx.size == 0:
            return np.empty(0, dtype=np.int16)

        # 96-kHz-Samples unverändert liefern. Das Resampling auf die tatsächliche
        # AudioContext-Rate übernimmt der Client im AudioWorklet (dort kennt er
        # seine echte sampleRate). Eine feste Dezimierung hier führte zu
        # Rate-Mismatch ("eine Spur zu schnell") auf Geräten mit anderer Rate.

        # normalisieren auf -1..1, verstärken, clippen, auf int16 skalieren
        vals = data[:, idx[0]].astype(np.float64) / 8388608.0
        vals *= MONITOR_GAIN
        np.clip(vals, -1.0, 1.0, out=vals)
        return (vals * 32767.0).astype(np.int16)


monitor_reader = MonitorReader()


def read_monitor_audio(channel: int, num_frames: int) -> np.ndarray:
    """Inkrementell: liefert die seit dem letzten Aufruf neuen Samples als int16."""
    return monitor_reader.read(channel)


class MonitorProducer:
    """Liest den Audio-Ring in einem eigenen, schnellen Thread und legt die
    int16-Chunks in einer Queue ab.

    Der Monitor lief bisher im langsamen WebSocket-Loop (~50 Reads/s), der
    zusätzlich die teure Oszilloskop-Berechnung macht. Dadurch kam der Reader
    nicht hinterher und der DMA-Ring lief über -> hörbare Lücken (Knacken).

    Dieser Producer läuft wie der funktionierende WAV-Export in einem eigenen
    Thread mit ~200 Reads/s und ist damit unabhängig von der WebSocket-Tick-Rate.
    Der WebSocket-Endpoint entnimmt die fertigen Chunks nur noch (non-blocking)
    aus der Queue.
    """

    def __init__(self):
        self._channel = 0
        self._reader = MonitorReader()
        self._q = queue.Queue(maxsize=256)
        self._stop = threading.Event()
        self._thread = None

    def start(self, channel: int) -> None:
        self.stop()
        self._channel = channel
        self._reader.reset()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._channel = 0
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.2)
            self._thread = None
        # Queue leeren, damit keine alten Samples weiterlaufen.
        while True:
            try:
                self._q.get_nowait()
            except queue.Empty:
                break

    @property
    def active(self) -> bool:
        return self._channel > 0

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._channel:
                audio = self._reader.read(self._channel)
                if audio.size:
                    try:
                        self._q.put_nowait(audio)
                    except queue.Full:
                        # Queue voll: ältesten Chunk verwerfen statt blockieren.
                        try:
                            self._q.get_nowait()
                        except queue.Empty:
                            pass
                        try:
                            self._q.put_nowait(audio)
                        except queue.Full:
                            pass
            time.sleep(0.005)  # ~200 Hz, wie der WAV-Export

    def drain(self) -> Optional[np.ndarray]:
        """Liefert alle gepufferten Chunks als ein zusammenhängendes int16-Array
        (oder None, wenn nichts vorliegt)."""
        parts = []
        while True:
            try:
                parts.append(self._q.get_nowait())
            except queue.Empty:
                break
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        return np.concatenate(parts)




def decode_to_channel_series(frames: List[dict], wanted: List[int]) -> Dict[int, List[int]]:
    """Wandle Frames in Zeitreihen pro Kanal um (nur `wanted`-Kanäle)."""
    series = {c: [] for c in wanted}
    for fr in frames:
        seen = set()
        for ch in fr["channels"]:
            if ch["ch"] in wanted and ch["ch"] not in seen:
                series[ch["ch"]].append(ch["v"])
                seen.add(ch["ch"])
    return series


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="I2S DAQ Interface")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> FileResponse:
    return FileResponse("static/index.html")


@app.get("/api/status")
async def api_status() -> JSONResponse:
    st = hw.read_status()
    ct = hw.read_counters()
    return JSONResponse({
        **st, **ct,
        **_ui_state(),
        "view_frames": appstate.view_frames,
        "recording": appstate.to_dict()["recording"],
    })


@app.post("/api/channels")
async def api_channels(body: dict) -> JSONResponse:
    """Setzt die Kanal-Maske. body: {"channels": [1,5,20]} oder {"mask": 0x...}"""
    if "mask" in body:
        mask = int(body["mask"])
    else:
        chans = body.get("channels", [])
        mask = 0
        for c in chans:
            c = int(c)
            if 1 <= c <= NUM_CHANNELS:
                mask |= 1 << (c - 1)
    hw.set_channels(mask)
    return JSONResponse({"ok": True, "mask": mask})


@app.post("/api/control")
async def api_control(body: dict) -> JSONResponse:
    """body: {"action": "activate"|"stop"|"reset"}

    - "activate": Gerät scharf schalten (DMA-Ring programmieren + prepare + record),
      damit Live-View & Monitor Daten bekommen. Kein Datei-Export.
    - "stop": Frame fertigstellen, Takte aus -> IDLE.
    - "reset": harter Soft-Reset zurück nach IDLE.
    """
    action = body.get("action")
    if action == "activate":
        try:
            program_dma_ring()
            detect_ring_params()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)})
        hw.control("prepare")
        time.sleep(0.05)
        hw.control("record")
        return JSONResponse({"ok": True, "action": action})
    if action in ("stop", "reset"):
        hw.control(action)
        # Sticky-Bits nach Stop/Reset loeschen, damit die UI den aktuellen
        # Zustand zeigt statt eines historischen "overrun yes".
        hw.clear_sticky()
        return JSONResponse({"ok": True, "action": action})
    # Rückwärtskompatibel: alte Aktionen
    if action == "record":
        try:
            program_dma_ring()
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)})
        hw.control("record")
        return JSONResponse({"ok": True, "action": action})
    if action == "prepare":
        hw.control("prepare")
        return JSONResponse({"ok": True, "action": action})
    return JSONResponse({"ok": False, "error": "unknown action"}, status_code=400)


@app.get("/api/settings")
async def api_settings_get() -> JSONResponse:
    """Aktuelles Einstellungs-Setup (Session + auf dem Gerät)."""
    return JSONResponse(appstate.settings)


@app.post("/api/settings")
async def api_settings_post(body: dict) -> JSONResponse:
    """Nimmt Änderungen an („send changes"). body = komplettes Settings-JSON."""
    data = _validate_settings(body)
    appstate.settings = data

    # Aktiven Kanal-Maskenwechsel setzt einen Neustart der Aufnahme voraus:
    # erst anhalten, dann Kanäle setzen, dann (falls vorher aktiv) wieder starten.
    was_recording = hw.read_status()["state"] == "RECORDING"
    if was_recording:
        hw.control("stop")
        time.sleep(0.05)

    mask = 0
    for c in data.get("active_channels", []):
        mask |= 1 << (int(c) - 1)
    hw.set_channels(mask)

    if was_recording:
        try:
            program_dma_ring()
            detect_ring_params()
        except Exception:
            pass
        hw.control("prepare")
        time.sleep(0.05)
        hw.control("record")

    return JSONResponse({"ok": True, "settings": data})


@app.post("/api/settings/save")
async def api_settings_save(body: dict) -> JSONResponse:
    """Speichert das aktuelle Setup auf dem Gerät (SETTINGS_FILE)."""
    data = _validate_settings(body.get("settings", appstate.settings))
    appstate.settings = data
    save_settings_to_file(data)
    return JSONResponse({"ok": True, "filename": SETTINGS_FILE})


@app.get("/api/settings/download")
async def api_settings_download() -> Response:
    """Lädt das aktuelle Setup als JSON-Datei herunter."""
    data = json.dumps(appstate.settings, indent=2, ensure_ascii=False)
    return Response(
        content=data,
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="mooga_settings.json"'},
    )


@app.post("/api/display")
async def api_display(body: dict) -> JSONResponse:
    chans = body.get("channels", [])
    appstate.display_channels = [int(c) for c in chans if 1 <= int(c) <= NUM_CHANNELS][:16]
    return JSONResponse({"ok": True, "display_channels": appstate.display_channels})


@app.post("/api/record")
async def api_record(body: dict) -> JSONResponse:
    """Startet eine Messung mit gegebener Dauer. body: {"duration": 5.0}
    Programmiert DMA-Ring und führt prepare+record automatisch aus."""
    duration = float(body.get("duration", 5.0))
    duration = max(0.1, min(3600.0, duration))
    # DMA-Ring sicherstellen und Aufnahme starten
    try:
        program_dma_ring()
        detect_ring_params()
    except Exception as e:
        return JSONResponse({"ok": False, "error": "DMA: " + str(e)})
    hw.control("prepare")
    time.sleep(0.05)
    hw.control("record")
    time.sleep(0.05)
    filename = start_recording(duration)
    if filename is None:
        return JSONResponse({"ok": False, "error": "bereits eine Messung aktiv"})
    return JSONResponse({"ok": True, "filename": filename, "duration": duration})


@app.get("/api/record/status")
async def api_record_status() -> JSONResponse:
    rec = appstate.recording
    if not rec:
        return JSONResponse({"recording": False})
    elapsed = time.time() - rec["started"]
    return JSONResponse({
        "recording": True,
        "elapsed": elapsed,
        "duration": rec["duration"],
        "filename": rec["filename"],
    })


@app.get("/api/download")
async def api_download(filename: str) -> Response:
    import os
    base = os.path.basename(filename)
    path = os.path.join("/tmp", base)
    if not os.path.exists(path):
        return JSONResponse({"error": "Datei nicht gefunden"}, status_code=404)
    mime = "audio/wav" if base.endswith(".wav") else "application/octet-stream"
    # Streaming von Disk statt komplettem Einlesen in den RAM.
    return FileResponse(path, media_type=mime,
                        filename=base,
                        headers={"Content-Disposition": f'attachment; filename="{base}"'})


@app.get("/api/record/stream")
async def api_record_stream(duration: float = 5.0) -> Response:
    """Streamt die laufende Messung direkt als WAV (chunked), ohne sie komplett
    im RAM/tmpfs zu puffern. Ideal für viele Kanäle + lange Messzeiten.

    Der Client kann die Antwort während der Messung fortlaufend konsumieren
    (z.B. in eine lokale Datei schreiben oder direkt abspielen).
    """
    duration = max(0.1, min(3600.0, duration))
    nch = max(1, bin(hw.read_channel_mask()).count("1"))
    frame_bytes = 16 + 8 * nch

    # DMA-Ring sicherstellen und Aufnahme starten
    try:
        program_dma_ring()
        detect_ring_params()
    except Exception as e:
        return JSONResponse({"error": "DMA: " + str(e)}, status_code=500)
    hw.control("prepare")
    time.sleep(0.05)
    hw.control("record")
    time.sleep(0.05)

    def gen():
        start = time.time()
        last_rel = get_write_rel()
        size = hw.data_ring_size or DMA_BUF_SZ
        yield _wav_header_stream(nch)
        while (time.time() - start) < duration:
            rel = get_write_rel()
            if rel < 0:
                time.sleep(0.01)
                continue
            new_bytes = (rel - last_rel) % size
            new_bytes = (new_bytes // frame_bytes) * frame_bytes
            if new_bytes <= 0:
                time.sleep(0.005)
                continue
            chunk = _read_ring_from(last_rel, new_bytes)
            last_rel = (last_rel + new_bytes) % size
            yield _frames_to_pcm24(chunk, nch)
            time.sleep(0.005)
        # Aufnahme beenden
        hw.control("stop")

    return StreamingResponse(gen(), media_type="audio/wav",
                             headers={"Content-Disposition": 'attachment; filename="stream.wav"'})


@app.get("/api/record/stream_binary")
async def api_record_stream_binary(duration: float = 5.0) -> Response:
    """Streamt die laufende Messung als rohen Binary-Stream (chunked), ohne
    WAV-Header und ohne PCM-Konvertierung — exakt so, wie die Frames aus dem
    DMA-Ring kommen (Timestamp + Kanal-Samples), ohne Zwischenspeichern."""
    duration = max(0.1, min(3600.0, duration))
    nch = max(1, bin(hw.read_channel_mask()).count("1"))
    frame_bytes = 16 + 8 * nch

    try:
        program_dma_ring()
        detect_ring_params()
    except Exception as e:
        return JSONResponse({"error": "DMA: " + str(e)}, status_code=500)
    hw.control("prepare")
    time.sleep(0.05)
    hw.control("record")
    time.sleep(0.05)

    def gen():
        start = time.time()
        last_rel = get_write_rel()
        size = hw.data_ring_size or DMA_BUF_SZ
        while (time.time() - start) < duration:
            rel = get_write_rel()
            if rel < 0:
                time.sleep(0.01)
                continue
            new_bytes = (rel - last_rel) % size
            new_bytes = (new_bytes // frame_bytes) * frame_bytes
            if new_bytes <= 0:
                time.sleep(0.005)
                continue
            chunk = _read_ring_from(last_rel, new_bytes)
            last_rel = (last_rel + new_bytes) % size
            yield chunk
            time.sleep(0.005)
        hw.control("stop")

    return StreamingResponse(gen(), media_type="application/octet-stream",
                             headers={"Content-Disposition": 'attachment; filename="stream.bin"'})


# ---------------------------------------------------------------------------
# WebSocket (Live-Stream der Oszilloskop-Kanäle + Status)
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)


manager = ConnectionManager()

# Globaler Audio-Owner: Nur EINE Session streamt gleichzeitig Audio. Verhindert
# das "doppelt versetzte" Echo, wenn mehrere Tabs/Fenster denselben Kanal abspielen.
_audio_owner = {"ws": None, "channel": 0}

# Dedizierter Audio-Producer-Thread: liest den Ring unabhängig von der langsamen
# WebSocket-Schleife und verhindert damit Ring-Overruns (Knacken).
monitor_producer = MonitorProducer()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await manager.connect(ws)
    # Lokaler Monitor-Kanal dieser Verbindung (nur gültig, wenn sie der Owner ist).
    monitor_channel = 0
    try:
        tick = 0
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_json(), timeout=0.01)
                t = msg.get("type")
                if t == "display":
                    chans = msg.get("channels", [])
                    appstate.display_channels = [int(c) for c in chans if 1 <= int(c) <= NUM_CHANNELS][:16]
                elif t == "view_frames":
                    v = int(msg.get("value", 400))
                    appstate.view_frames = max(100, min(20000, v))
                elif t == "monitor":
                    ch = int(msg.get("channel", 0))
                    ch = ch if 1 <= ch <= NUM_CHANNELS else 0
                    if ch > 0:
                        # Play anfordern: nur wenn frei (oder schon dieser Client),
                        # sonst ablehnen -> kein zweiter Audiostream.
                        if _audio_owner["ws"] is None or _audio_owner["ws"] is ws:
                            _audio_owner["ws"] = ws
                            _audio_owner["channel"] = ch
                            monitor_channel = ch
                            monitor_producer.start(ch)
                        else:
                            # Stream ist bereits an einen anderen Client vergeben.
                            monitor_channel = 0
                            await ws.send_json({
                                "type": "audio_busy",
                                "message": "Audio stream is already in use by another session.",
                            })
                    else:
                        # Stop: Owner freigeben, falls dieser Client der Owner war.
                        if _audio_owner["ws"] is ws:
                            _audio_owner["ws"] = None
                            _audio_owner["channel"] = 0
                            monitor_producer.stop()
                        monitor_channel = 0
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break

            tick += 1
            st = hw.read_status()
            ct = hw.read_counters()
            ui = _ui_state()
            if st["state"] == "RECORDING":
                payload = {
                    "type": "live",
                    "state": st["state"],
                    "status": st,
                    "counters": ct,
                    **ui,
                }
                # Live-Wellenform nur alle ~10 Updates senden (≈10 fps)
                if tick % 10 == 0:
                    payload["series"] = read_channel_series(
                        appstate.display_channels, appstate.view_frames)
                # Monitor-Audio nur an den Owner (1 Stream / 1 Session).
                if monitor_channel:
                    audio = monitor_producer.drain()
                    if audio is not None and audio.size:
                        payload["audio_b64"] = base64.b64encode(audio.tobytes()).decode("ascii")
                        payload["audio_len"] = int(audio.size)
                        payload["audio_channel"] = monitor_channel
            else:
                payload = {
                    "type": "status",
                    "state": st["state"],
                    "status": st,
                    "counters": ct,
                    **ui,
                }
            await ws.send_json(payload)
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        pass
    finally:
        # Beim Trennen: Owner freigeben, falls dieser Client ihn hielt.
        if _audio_owner["ws"] is ws:
            _audio_owner["ws"] = None
            _audio_owner["channel"] = 0
            monitor_producer.stop()
        manager.disconnect(ws)


# Ring-Geometrie beim Import/Start erkennen (robust gegen Neustarts)
try:
    detect_ring_params()
except Exception:
    pass








