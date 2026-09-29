# Mooga Multidecoder — DAQ Web-Interface

Web-Frontend und Backend zur Steuerung einer I2S-DAQ auf einem Zynq-Board.
Die App greift per `/dev/mem` (mmap) direkt auf die FPGA-Hardware zu, liest
Live-Daten aus dem DMA-Ring und stellt sie über **REST + WebSocket** bereit.

- **Version:** `0.0.2b`
- **Kanäle:** bis zu 80 (Hardware-Maske)
- **Sample-Rate:** 96 kHz
- **Sample-Format:** 24-bit signed, Little-Endian (MSB-aligned im 32-bit-Rohformat)

---

## Inhaltsverzeichnis

1. [Hardware-Adressen](#1-hardware-adressen)
2. [Projektstruktur](#2-projektstruktur)
3. [Voraussetzungen](#3-voraussetzungen)
4. [Starten der App](#4-starten-der-app)
5. [Konfiguration / Settings](#5-konfiguration--settings)
6. [REST-API](#6-rest-api)
7. [WebSocket-API](#7-websocket-api)
8. [Datenformate](#8-datenformate)
9. [Frontend](#9-frontend)

---

## 1. Hardware-Adressen

| Bereich | Adresse | Größe |
|---|---|---|
| I2S-Register | `0x43C00000` | 4 KB |
| DMA-Register | `0x43C10000` | 64 KB |
| **DMA-Datenpuffer** | `0x1F000000` | **256 MiB** |

> Der DMA-Ringpuffer wurde von 16 MiB auf **256 MiB** vergrößert
> (Basis `0x1F000000`, Ende `0x2EFFFFFF`). Bei 80 Kanälen (~63 MB/s)
> läuft der Ring dadurch in ~4 s statt ~0,25 s um.

Frame-Struktur (rohes DMA-Format):
```
Timestamp (16 Byte) + N × (CH_ID 4 Byte + DATA 4 Byte)
```
- `CH_ID`: Kanalnummer (1 Byte genutzt, 3 Byte Padding)
- `DATA`: 24-bit signed, MSB-aligned in 32 bit

DMA-Schreibposition wird über `CURDESC` (`0x43C10038`) → Descriptor ermittelt.

---

## 2. Projektstruktur

```
mooga_webfrontend/
├── main.py                  # Backend (FastAPI + Hardware-Zugriff)
├── mooga_settings.json      # Persistierte Einstellungen (auf dem Gerät)
├── static/
│   ├── index.html           # Hauptseite (Measurement + Preferences)
│   ├── live.html            # Live-View-Popup (Oszilloskop)
│   └── mooga.png            # Logo
└── README.md
```

---

## 3. Voraussetzungen

- Python 3.12+
- Pakete: `fastapi`, `uvicorn`, `numpy`, `websockets`
- Zugriff auf `/dev/mem` (läuft als `root`)

Installation (auf dem Gerät, persistent nach `/data`):
```bash
python3 -m pip install --target /data/python_packages \
    fastapi uvicorn websockets
```

> Hinweis: `numpy` ist üblicherweise bereits im Rootfs vorinstalliert.
> Das Rootfs ist **nicht persistent** — installiere die Zusatzpakete daher
> nach `/data` (persistente Partition), wie oben gezeigt.

---

## 4. Starten der App

```bash
cd /data/webapp
PYTHONPATH=/data/python_packages \
    python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Danach ist die App unter `http://<board-ip>:8000` erreichbar.

> **Deployment-Hinweis:** Die App liegt persistent unter `/data/webapp/`
> (nicht unter `/home/root/webapp/`, das beim Reflash verloren geht).

---

## 5. Konfiguration / Settings

Die Einstellungen liegen im JSON-Format (Datei `mooga_settings.json`):

```json
{
  "schema": "mooga.multidecoder.settings",
  "version": 1,
  "active_channels": [1, 2, 3],
  "sensitivity": {
    "1": { "value": 1.0, "unit": "µV", "name": "Temp 1" }
  }
}
```

| Feld | Bedeutung |
|---|---|
| `active_channels` | Liste der aktiven Kanäle (1..80) |
| `sensitivity[c].value` | Umrechnungsfaktor Roh-Sample → physikalische Einheit |
| `sensitivity[c].unit` | Einheit (für Y-Achse im Live-View) |
| `sensitivity[c].name` | Kanalname (in der UI auf 10 Zeichen gekürzt) |

---

## 6. REST-API

| Methode | Endpoint | Beschreibung |
|---|---|---|
| `GET` | `/` | Hauptseite (HTML) |
| `GET` | `/api/status` | Hardware-Status + Counter + Settings |
| `POST` | `/api/control` | `{"action": "activate" \| "stop" \| "reset"}` |
| `POST` | `/api/channels` | Kanal-Maske setzen (`{"channels": [...]}` oder `{"mask": ...}`) |
| `GET` | `/api/settings` | Aktuelle Einstellungen |
| `POST` | `/api/settings` | Einstellungen übernehmen („Send changes") |
| `POST` | `/api/settings/save` | Einstellungen aufs Gerät speichern |
| `GET` | `/api/settings/download` | Einstellungen als JSON-Datei herunterladen |
| `POST` | `/api/display` | Anzeige-Kanäle für Live-View setzen |
| `POST` | `/api/record` | Messung starten (`{"duration": 5.0}`) |
| `GET` | `/api/record/status` | Status der laufenden Messung |
| `GET` | `/api/download?filename=...` | Aufgenommene Datei herunterladen |
| `GET` | `/api/record/stream?duration=...` | Messung als **WAV** streamen (chunked) |
| `GET` | `/api/record/stream_binary?duration=...` | Messung als **rohes Binary** streamen |
| `POST` | `/api/record/save` | Aufnahme lokal nach `/data/measurements` speichern (`{"duration": 5.0, "format": "wav"\|"binary", "filename": "myrec"}`) |
| `GET` | `/api/measurements` | Liste der lokalen Aufnahmen (mit Metadaten) |
| `GET` | `/api/measurements/download?filename=...` | Lokale Aufnahme herunterladen |
| `GET` | `/api/measurements/info?filename=...` | Metadaten einer Aufnahme (Kanäle, Dauer, …) |
| `POST` | `/api/measurements/rename` | Umbenennen (`{"filename": "...", "new_name": "..."}`) |
| `POST` | `/api/measurements/delete` | Löschen (`{"filename": "..."}`) |

### Aktionen (`/api/control`)

| Aktion | Bedeutung |
|---|---|
| `activate` | Gerät scharf schalten (DMA-Ring programmieren + prepare + record) |
| `stop` | Aufnahme beenden, Takte aus → IDLE |
| `reset` | Harter Soft-Reset nach IDLE |

---

## 7. WebSocket-API

**Endpoint:** `ws://<board-ip>:8000/ws`

Der Server pusht fortlaufend Status und (im RECORDING-Zustand) Live-Daten.

Client → Server (JSON):
```json
{ "type": "display", "channels": [1, 2, 3] }
{ "type": "view_frames", "value": 400 }
{ "type": "monitor", "channel": 2 }
```

Server → Client (JSON):

**Status-Nachricht** (IDLE/PREPARED/STOPPING):
```json
{
  "type": "status",
  "state": "IDLE",
  "status": { "state": "IDLE", "clk_active": true, "warmup_done": true },
  "counters": { "num_active": 2, "frame_count": 123, "version": "0.0.2b" },
  "enabled_channels": [1, 2],
  "display_channels": [1, 2],
  "monitor_channel": 0,
  "settings": {}
}
```

**Live-Nachricht** (RECORDING):
```json
{
  "type": "live",
  "state": "RECORDING",
  "status": {},
  "counters": {},
  "series": { "1": [123, 456], "2": [789, 101] },
  "audio_b64": "...", "audio_channel": 2
}
```

- `series`: Live-Wellenform pro Kanal (alle ~10 Updates ≈ 10 fps)
- `audio_b64`: base64-kodierte int16-Samples des Monitor-Kanals

---

## 8. Datenformate

### WAV-Export (`/api/record/stream`)

- Format: **WAVE_FORMAT_EXTENSIBLE** (`0xFFFE`), 24-bit PCM interleaved
- Unterstützt **mehr als 2 Kanäle** (Standard-PCM wäre auf Mono/Stereo limitiert)
- Header: 68 Byte (RIFF + fmt(8+40) + data)
- Sample-Rate 96 kHz, Little-Endian

### Binary-Export (`/api/record/stream_binary`)

Rohdaten **exakt wie aus dem DMA-Ring** — ohne WAV-Header, ohne Konvertierung:

```
[Timestamp 16B][CH_ID 4B][DATA 4B] × N Kanäle  (pro Frame)
```

Bei 80 Kanälen: `frame_bytes = 16 + 8·80 = 656` Byte, ~63 MB/s.

---

## 9. Frontend

- **`index.html`** — Hauptseite:
  - Tabs `Measurement` / `Preferences`
  - **Control** (Activate / Stop / Reset)
  - **Live Monitor** (Kanal-Dropdown + Play/Stop, Audio über WebAudio)
  - **Status** (State, CLK, Warmup, Overrun, Version, …)
  - **Live preview channels** (nur aktive Kanäle, max. 10 im Plot)
  - **Measurement (WAV)** und **Measurement (binary)**
  - **Preferences** (aktive Kanäle, Sensitivity + Kanalname pro Kanal)

- **`live.html`** — Live-View-Popup:
  - Oszilloskop-Plot der gewählten Kanäle
  - Y-Achse umgerechnet per Sensitivity (Faktor + Einheit des ersten Kanals)
  - Auto-/manueller Y-Bereich, Zeitfenster-Slider


