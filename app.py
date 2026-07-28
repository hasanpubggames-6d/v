"""
PyHost — Telegram Bot Hosting Panel
Flask backend: process management, file handling, live logs, packages
"""

import os
import sys
import json
import time
import uuid
import signal
import shutil
import hashlib
import zipfile
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from functools import wraps
from collections import deque
from flask import send_file
from flask import (
    Flask, render_template, request, jsonify,
    session, redirect, url_for, send_from_directory,
    Response, stream_with_context
)
from werkzeug.utils import secure_filename

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
BOTS_DIR   = BASE_DIR / "bots_data"
BOTS_DIR.mkdir(exist_ok=True)

MAX_UPLOAD_MB   = 50
LOG_BUFFER_SIZE = 500          # lines per bot
WATCHDOG_SLEEP  = 10           # seconds

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "pyhost-secret-2025-change-me")
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024

# ─────────────────────────────────────────────
# IN-MEMORY STORE
# ─────────────────────────────────────────────
USERS: dict[str, str] = {
    os.environ.get("ADMIN_USER", "admin"): hashlib.sha256(
        os.environ.get("ADMIN_PASS", "admin123").encode()
    ).hexdigest()
}

# bot_id → { meta dict }
BOTS: dict[str, dict] = {}

# bot_id → subprocess.Popen
PROCESSES: dict[str, subprocess.Popen] = {}

# bot_id → deque of log lines (strings)
LOG_BUFFERS: dict[str, deque] = {}

# Global lock
LOCK = threading.Lock()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def hash_pw(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def bot_dir(bot_id: str) -> Path:
    d = BOTS_DIR / bot_id
    d.mkdir(exist_ok=True)
    return d

def save_bots_meta():
    meta = {
        bid: {k: v for k, v in b.items() if k != "_start_time"}
        for bid, b in BOTS.items()
    }
    with open(BOTS_DIR / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)

def load_bots_meta():
    p = BOTS_DIR / "meta.json"
    if not p.exists():
        return
    try:
        with open(p) as f:
            data = json.load(f)
        for bid, b in data.items():
            b["status"] = "stopped"   # safe default on restart
            BOTS[bid] = b
            LOG_BUFFERS[bid] = deque(maxlen=LOG_BUFFER_SIZE)
            log_append(bid, "info", "Panel restarted — bot is stopped")
    except Exception:
        pass

def log_append(bot_id: str, level: str, msg: str):
    if bot_id not in LOG_BUFFERS:
        LOG_BUFFERS[bot_id] = deque(maxlen=LOG_BUFFER_SIZE)
    LOG_BUFFERS[bot_id].append({"t": ts(), "lvl": level, "msg": msg})

def get_bot_or_404(bot_id: str):
    b = BOTS.get(bot_id)
    if not b:
        return None, jsonify({"ok": False, "error": "Bot not found"}), 404
    return b, None, None

# ─────────────────────────────────────────────
# AUTH DECORATOR
# ─────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            if request.is_json:
                return jsonify({"ok": False, "error": "Unauthorized"}), 401
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return wrapper

# ─────────────────────────────────────────────
# PROCESS MANAGER
# ─────────────────────────────────────────────
def launch_process(bot_id: str) -> bool:
    """Spawn the bot subprocess and start log-reader thread."""
    b = BOTS.get(bot_id)
    if not b:
        return False

    d = bot_dir(bot_id)
    cmd = b.get("start_cmd", "python bot.py").split()

    # Build env
    env = os.environ.copy()
    for line in b.get("env", "").splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(d),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as e:
        log_append(bot_id, "err", f"Launch failed: {e}")
        return False

    with LOCK:
        PROCESSES[bot_id] = proc
        BOTS[bot_id]["status"] = "running"
        BOTS[bot_id]["pid"]    = proc.pid
        BOTS[bot_id]["_start_time"] = time.time()

    log_append(bot_id, "ok", f"Process started — PID {proc.pid}")
    save_bots_meta()

    # Log reader thread
    def _reader():
        try:
            for line in proc.stdout:
                log_append(bot_id, "info", line.rstrip())
        except Exception:
            pass
        finally:
            rc = proc.wait()
            with LOCK:
                PROCESSES.pop(bot_id, None)
                if BOTS.get(bot_id):
                    BOTS[bot_id]["status"] = "stopped"
                    BOTS[bot_id]["pid"]    = None
            log_append(bot_id, "warn", f"Process exited — code {rc}")
            save_bots_meta()

    threading.Thread(target=_reader, daemon=True).start()
    return True

def kill_process(bot_id: str):
    proc = PROCESSES.get(bot_id)
    if proc and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception:
            pass
    with LOCK:
        PROCESSES.pop(bot_id, None)
        if BOTS.get(bot_id):
            BOTS[bot_id]["status"] = "stopped"
            BOTS[bot_id]["pid"]    = None
    log_append(bot_id, "warn", "Bot stopped by user")
    save_bots_meta()

# ─────────────────────────────────────────────
# WATCHDOG
# ─────────────────────────────────────────────
def watchdog():
    """Auto-restart bots marked auto_restart=True that have crashed."""
    while True:
        time.sleep(WATCHDOG_SLEEP)
        for bot_id, b in list(BOTS.items()):
            if not b.get("auto_restart"):
                continue
            proc = PROCESSES.get(bot_id)
            if proc is None or proc.poll() is not None:
                if b.get("status") == "running":
                    log_append(bot_id, "warn", "Watchdog: crash detected — restarting")
                    launch_process(bot_id)

threading.Thread(target=watchdog, daemon=True).start()

# ─────────────────────────────────────────────
# ROUTES — AUTH
# ─────────────────────────────────────────────
@app.route("/")
def index():
    if session.get("user"):
        return redirect(url_for("dashboard"))
    return send_file(__file__.replace('app.py', 'index.html'))

@app.route("/api/login", methods=["POST"])
def api_login():
    d = request.get_json(force=True)
    u = d.get("username", "").strip()
    p = hash_pw(d.get("password", ""))
    if USERS.get(u) == p:
        session["user"] = u
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Invalid credentials"}), 401

@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/change-password", methods=["POST"])
@login_required
def api_change_password():
    d = request.get_json(force=True)
    old_h = hash_pw(d.get("old", ""))
    u = session["user"]
    if USERS.get(u) != old_h:
        return jsonify({"ok": False, "error": "Wrong current password"}), 400
    USERS[u] = hash_pw(d.get("new", ""))
    return jsonify({"ok": True})

# ─────────────────────────────────────────────
# ROUTES — PAGES
# ─────────────────────────────────────────────
@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("index.html")

# ─────────────────────────────────────────────
# ROUTES — BOTS CRUD
# ─────────────────────────────────────────────
@app.route("/api/bots", methods=["GET"])
@login_required
def api_bots_list():
    out = []
    for bid, b in BOTS.items():
        proc = PROCESSES.get(bid)
        uptime = 0
        if proc and b.get("_start_time"):
            uptime = int(time.time() - b["_start_time"])
        out.append({
            "id":          bid,
            "name":        b.get("name"),
            "file":        b.get("main_file"),
            "status":      b.get("status", "stopped"),
            "pid":         b.get("pid"),
            "uptime":      uptime,
            "auto_restart":b.get("auto_restart", False),
            "start_cmd":   b.get("start_cmd"),
            "packages":    b.get("packages", []),
            "created":     b.get("created"),
        })
    return jsonify({"ok": True, "bots": out})

@app.route("/api/bots/<bot_id>", methods=["GET"])
@login_required
def api_bot_get(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    proc   = PROCESSES.get(bot_id)
    uptime = 0
    if proc and b.get("_start_time"):
        uptime = int(time.time() - b["_start_time"])
    return jsonify({
        "ok": True,
        "bot": {
            "id":          bot_id,
            "name":        b.get("name"),
            "file":        b.get("main_file"),
            "status":      b.get("status", "stopped"),
            "pid":         b.get("pid"),
            "uptime":      uptime,
            "start_cmd":   b.get("start_cmd"),
            "env":         b.get("env", ""),
            "auto_restart":b.get("auto_restart", False),
            "packages":    b.get("packages", []),
            "created":     b.get("created"),
        }
    })

@app.route("/api/bots/<bot_id>", methods=["PATCH"])
@login_required
def api_bot_update(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    d = request.get_json(force=True)
    for field in ("name", "start_cmd", "env", "auto_restart"):
        if field in d:
            b[field] = d[field]
    save_bots_meta()
    return jsonify({"ok": True})

@app.route("/api/bots/<bot_id>", methods=["DELETE"])
@login_required
def api_bot_delete(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    kill_process(bot_id)
    with LOCK:
        BOTS.pop(bot_id, None)
        LOG_BUFFERS.pop(bot_id, None)
    shutil.rmtree(bot_dir(bot_id), ignore_errors=True)
    save_bots_meta()
    return jsonify({"ok": True})

# ─────────────────────────────────────────────
# ROUTES — BOT CONTROL
# ─────────────────────────────────────────────
@app.route("/api/bots/<bot_id>/start", methods=["POST"])
@login_required
def api_bot_start(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    if b.get("status") == "running":
        return jsonify({"ok": False, "error": "Already running"})
    ok = launch_process(bot_id)
    return jsonify({"ok": ok, "error": None if ok else "Failed to start — check logs"})

@app.route("/api/bots/<bot_id>/stop", methods=["POST"])
@login_required
def api_bot_stop(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    kill_process(bot_id)
    return jsonify({"ok": True})

@app.route("/api/bots/<bot_id>/restart", methods=["POST"])
@login_required
def api_bot_restart(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    kill_process(bot_id)
    time.sleep(1)
    ok = launch_process(bot_id)
    return jsonify({"ok": ok})

# ─────────────────────────────────────────────
# ROUTES — UPLOAD
# ─────────────────────────────────────────────
ALLOWED_EXT = {".py", ".zip", ".txt", ".json", ".env", ".cfg", ".ini", ".toml"}

@app.route("/api/bots/upload", methods=["POST"])
@login_required
def api_bot_upload():
    name      = request.form.get("name", "").strip()
    start_cmd = request.form.get("start_cmd", "python bot.py").strip()
    env_raw   = request.form.get("env", "")
    auto_rst  = request.form.get("auto_restart", "false").lower() == "true"

    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400

    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    ext = Path(f.filename).suffix.lower()
    if ext not in ALLOWED_EXT:
        return jsonify({"ok": False, "error": f"File type {ext} not allowed"}), 400

    bot_id   = str(uuid.uuid4())[:8]
    dest_dir = bot_dir(bot_id)
    fname    = secure_filename(f.filename)
    dest     = dest_dir / fname
    f.save(dest)

    # Unzip if zip
    main_file = fname
    if ext == ".zip":
        with zipfile.ZipFile(dest, "r") as z:
            z.extractall(dest_dir)
        dest.unlink()
        py_files = list(dest_dir.glob("**/*.py"))
        main_file = py_files[0].name if py_files else "bot.py"

    with LOCK:
        BOTS[bot_id] = {
            "name":         name,
            "main_file":    main_file,
            "start_cmd":    start_cmd,
            "env":          env_raw,
            "auto_restart": auto_rst,
            "status":       "stopped",
            "pid":          None,
            "packages":     [],
            "created":      datetime.now().isoformat(),
        }
        LOG_BUFFERS[bot_id] = deque(maxlen=LOG_BUFFER_SIZE)

    log_append(bot_id, "ok", f"Bot registered — {main_file}")
    save_bots_meta()
    return jsonify({"ok": True, "bot_id": bot_id})

# ─────────────────────────────────────────────
# ROUTES — FILES
# ─────────────────────────────────────────────
@app.route("/api/bots/<bot_id>/files", methods=["GET"])
@login_required
def api_bot_files(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    d = bot_dir(bot_id)
    files = []
    for p in sorted(d.iterdir()):
        if p.is_file():
            files.append({
                "name":     p.name,
                "size":     p.stat().st_size,
                "modified": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
            })
    return jsonify({"ok": True, "files": files})

@app.route("/api/bots/<bot_id>/files/<filename>", methods=["GET"])
@login_required
def api_bot_file_read(bot_id, filename):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    p = bot_dir(bot_id) / secure_filename(filename)
    if not p.exists() or not p.is_file():
        return jsonify({"ok": False, "error": "File not found"}), 404
    try:
        content = p.read_text(errors="replace")
        return jsonify({"ok": True, "content": content})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/bots/<bot_id>/files/<filename>", methods=["PUT"])
@login_required
def api_bot_file_write(bot_id, filename):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    p = bot_dir(bot_id) / secure_filename(filename)
    d = request.get_json(force=True)
    try:
        p.write_text(d.get("content", ""))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/bots/<bot_id>/files/<filename>", methods=["DELETE"])
@login_required
def api_bot_file_delete(bot_id, filename):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    p = bot_dir(bot_id) / secure_filename(filename)
    if p.exists():
        p.unlink()
    return jsonify({"ok": True})

@app.route("/api/bots/<bot_id>/files/<filename>/download")
@login_required
def api_bot_file_download(bot_id, filename):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    return send_from_directory(bot_dir(bot_id), secure_filename(filename), as_attachment=True)

@app.route("/api/bots/<bot_id>/upload-file", methods=["POST"])
@login_required
def api_bot_upload_file(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No file"}), 400
    fname = secure_filename(f.filename)
    f.save(bot_dir(bot_id) / fname)
    return jsonify({"ok": True, "name": fname})

# ─────────────────────────────────────────────
# ROUTES — PACKAGES
# ─────────────────────────────────────────────
@app.route("/api/bots/<bot_id>/packages/install", methods=["POST"])
@login_required
def api_pkg_install(bot_id):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    d   = request.get_json(force=True)
    pkg = d.get("package", "").strip()
    if not pkg:
        return jsonify({"ok": False, "error": "No package name"}), 400

    def _stream():
        log_append(bot_id, "info", f"pip install {pkg}")
        yield f"data: [INFO] Running: pip install {pkg}\n\n"
        proc = subprocess.Popen(
            [sys.executable, "-m", "pip", "install", pkg, "--target",
             str(bot_dir(bot_id) / "site-packages"), "--quiet", "--progress-bar", "off"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                log_append(bot_id, "info", line)
                yield f"data: {line}\n\n"
        rc = proc.wait()
        if rc == 0:
            pkg_name = pkg.split("==")[0].split(">=")[0]
            if pkg_name not in b.get("packages", []):
                b.setdefault("packages", []).append(pkg_name)
                save_bots_meta()
            log_append(bot_id, "ok", f"Installed {pkg} ✓")
            yield f"data: [OK] Installed {pkg}\n\n"
        else:
            log_append(bot_id, "err", f"pip failed — code {rc}")
            yield f"data: [ERR] pip exited {rc}\n\n"
        yield "data: __DONE__\n\n"

    return Response(stream_with_context(_stream()), mimetype="text/event-stream")

@app.route("/api/bots/<bot_id>/packages/<pkg_name>", methods=["DELETE"])
@login_required
def api_pkg_remove(bot_id, pkg_name):
    b, err, code = get_bot_or_404(bot_id)
    if err:
        return err, code
    b["packages"] = [p for p in b.get("packages", []) if p != pkg_name]
    save_bots_meta()
    return jsonify({"ok": True})

# ─────────────────────────────────────────────
# ROUTES — LOGS
# ─────────────────────────────────────────────
@app.route("/api/bots/<bot_id>/logs", methods=["GET"])
@login_required
def api_bot_logs(bot_id):
    lines = list(LOG_BUFFERS.get(bot_id, []))
    return jsonify({"ok": True, "logs": lines})

@app.route("/api/bots/<bot_id>/logs/stream")
@login_required
def api_bot_logs_stream(bot_id):
    """SSE endpoint — streams new log lines every second."""
    def _gen():
        sent = 0
        while True:
            buf  = list(LOG_BUFFERS.get(bot_id, []))
            new  = buf[sent:]
            sent = len(buf)
            for line in new:
                yield f"data: {json.dumps(line)}\n\n"
            time.sleep(1)
    return Response(stream_with_context(_gen()), mimetype="text/event-stream")

@app.route("/api/bots/<bot_id>/logs", methods=["DELETE"])
@login_required
def api_bot_logs_clear(bot_id):
    LOG_BUFFERS[bot_id] = deque(maxlen=LOG_BUFFER_SIZE)
    return jsonify({"ok": True})

# ─────────────────────────────────────────────
# ROUTES — STATS
# ─────────────────────────────────────────────
@app.route("/api/stats", methods=["GET"])
@login_required
def api_stats():
    total   = len(BOTS)
    running = sum(1 for b in BOTS.values() if b.get("status") == "running")
    return jsonify({
        "ok":      True,
        "total":   total,
        "running": running,
        "stopped": total - running,
    })

# ─────────────────────────────────────────────
# BOOT
# ─────────────────────────────────────────────
load_bots_meta()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
