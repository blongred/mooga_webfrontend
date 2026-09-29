#!/bin/sh
# I2S DAQ Web-App Start/Stop/Status Script
# Nutzung:  ./start.sh [start|stop|restart|status|log]

APP_DIR=/data/webapp
PY_PKGS=/data/python_packages
HOST=0.0.0.0
PORT=8000
LOG=/tmp/webapp.log
PIDFILE=/tmp/webapp.pid

find_pid() {
    ps w 2>/dev/null | grep "python3 -m uvicorn main:app" | grep -v grep | awk '{print $1}' | head -1
}

do_start() {
    PID=$(find_pid)
    if [ -n "$PID" ]; then
        echo "App laeuft bereits (PID $PID). Nichts zu tun."
        exit 0
    fi
    if [ ! -d "$PY_PKGS" ]; then
        echo "FEHLER: $PY_PKGS nicht gefunden."
        exit 1
    fi
    cd "$APP_DIR" || exit 1
    echo "Starte App auf http://$HOST:$PORT ..."
    PYTHONPATH="$PY_PKGS" \
        setsid python3 -m uvicorn main:app --host "$HOST" --port "$PORT" \
        > "$LOG" 2>&1 < /dev/null &
    sleep 2
    PID=$(find_pid)
    if [ -n "$PID" ]; then
        echo "OK - laeuft (PID $PID). Log: $LOG"
    else
        echo "FEHLER beim Start - Log-Ausgabe:"
        tail -20 "$LOG"
        exit 1
    fi
}

do_stop() {
    PID=$(find_pid)
    if [ -z "$PID" ]; then
        echo "App laeuft nicht."
        exit 0
    fi
    echo "Stoppe App (PID $PID) ..."
    kill "$PID" 2>/dev/null
    sleep 1
    PID=$(find_pid)
    if [ -n "$PID" ]; then
        kill -9 "$PID" 2>/dev/null
    fi
    echo "Gestoppt."
}

do_status() {
    PID=$(find_pid)
    if [ -n "$PID" ]; then
        echo "App LAEUFT (PID $PID) auf http://$HOST:$PORT"
    else
        echo "App laeuft NICHT."
    fi
}

do_log() {
    tail -n 40 "$LOG" 2>/dev/null || echo "Kein Log vorhanden."
}

case "$1" in
    start)   do_start ;;
    stop)    do_stop ;;
    restart) do_stop; do_start ;;
    status)  do_status ;;
    log)     do_log ;;
    "")      do_start ;;
    *)       echo "Nutzung: $0 [start|stop|restart|status|log]"; exit 1 ;;
esac
