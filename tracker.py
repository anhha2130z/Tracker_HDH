import threading
import time
import json
import os
import tempfile
from flask import Flask, request, jsonify, abort
from datetime import datetime
from typing import Dict, Any
from datetime import datetime, timezone

# Configurable params (match recommended config)
HEARTBEAT_INTERVAL = 10      # not enforced on tracker side, peers should use it
TIMEOUT = 30                 # seconds - consider peer dead if no heartbeat within this
METADATA_FILE = "metadata.json"
TRACKER_LOG = "tracker.log"
CHECK_INTERVAL = 10          # how often tracker scans for dead peers
REPLICATION_HOOK_MODULE = "replication"  # optional module (Người 2)

app = Flask(__name__)

# In-memory metadata structure
# { "peers": {peer_id: {host, port, last_heartbeat (ts), files: {filename: [chunk_hash...]}, status: "alive"/"dead"} },
#   "files": { filename: { "chunks": {chunk_hash: [peer_id, ...]}, "size": int, "uploaded_by": peer_id } },
#   "created_at": timestamp }
metadata_lock = threading.RLock()
if os.path.exists(METADATA_FILE):
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            METADATA: Dict[str, Any] = json.load(f)
    except Exception as e:
        print(f"[WARN] Could not read {METADATA_FILE}: {e}. Starting with empty metadata.")
        METADATA = {}
else:
    METADATA = {}

# ensure keys exist
METADATA.setdefault("peers", {})
METADATA.setdefault("files", {})
METADATA.setdefault("created_at", time.time())

# Try to import replication hook if exists
_replication_hook = None
try:
    import importlib
    replication_mod = importlib.import_module(REPLICATION_HOOK_MODULE)
    # expected function: on_peer_down(dead_peer_id: str, metadata: dict, tracker: object)
    if hasattr(replication_mod, "on_peer_down"):
        _replication_hook = replication_mod.on_peer_down
        print("[INFO] Replication hook loaded from replication.py")
    else:
        print("[INFO] replication.py present but no on_peer_down() found.")
except Exception:
    # module not present or import error — not fatal
    _replication_hook = None

def log(msg: str):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(TRACKER_LOG, "a", encoding="utf-8") as lf:
            lf.write(line + "\n")
    except Exception:
        pass

def safe_write_metadata():
    """Write METADATA to METADATA_FILE atomically."""
    with metadata_lock:
        tmpfd, tmppath = tempfile.mkstemp(prefix="meta_", suffix=".json", dir=".")
        try:
            with os.fdopen(tmpfd, "w", encoding="utf-8") as f:
                json.dump(METADATA, f, indent=2, ensure_ascii=False)
            os.replace(tmppath, METADATA_FILE)
        except Exception as e:
            log(f"ERROR writing metadata file: {e}")
            try:
                if os.path.exists(tmppath):
                    os.remove(tmppath)
            except Exception:
                pass

def update_peer_heartbeat(peer_id: str, host: str = None, port: int = None, files: Dict[str, Any] = None):
    now = time.time()
    with metadata_lock:
        peer = METADATA["peers"].setdefault(peer_id, {})
        peer["last_heartbeat"] = now
        peer["status"] = "alive"
        if host:
            peer["host"] = host
        if port:
            peer["port"] = int(port)
        if files is not None:
            # files is mapping filename -> list of chunk hashes OR a list of filenames
            # we store as filename -> list
            peer_files = {}
            for fn, chunks in files.items():
                peer_files[fn] = list(chunks) if isinstance(chunks, (list, tuple)) else []
            peer["files"] = peer_files
        METADATA.setdefault("updated_at", now)
        safe_write_metadata()

def mark_peer_dead(peer_id: str):
    with metadata_lock:
        peer = METADATA["peers"].get(peer_id)
        if not peer:
            return
        if peer.get("status") == "dead":
            return
        peer["status"] = "dead"
        peer["dead_since"] = time.time()
        METADATA.setdefault("updated_at", time.time())
        safe_write_metadata()
    log(f"Peer marked dead: {peer_id}")
    # Call replication hook if available
    try:
        if _replication_hook:
            # call in separate thread to avoid blocking
            threading.Thread(target=_replication_hook, args=(peer_id, METADATA, None), daemon=True).start()
        else:
            # If no replication module, just write a task placeholder for manual inspection
            tasks_path = "replication_tasks.json"
            task = {"type": "peer_down", "peer_id": peer_id, "time": time.time()}
            try:
                if os.path.exists(tasks_path):
                    with open(tasks_path, "r", encoding="utf-8") as f:
                        tasks = json.load(f)
                else:
                    tasks = []
            except Exception:
                tasks = []
            tasks.append(task)
            with open(tasks_path, "w", encoding="utf-8") as f:
                json.dump(tasks, f, indent=2)
            log(f"Wrote replication placeholder task for dead peer {peer_id} to {tasks_path}")
    except Exception as e:
        log(f"Error invoking replication hook for {peer_id}: {e}")

def scan_for_dead_peers_loop():
    """Background thread: scans heartbeat timestamps and marks dead peers."""
    while True:
        now = time.time()
        to_mark = []
        with metadata_lock:
            for pid, pinfo in list(METADATA.get("peers", {}).items()):
                last = pinfo.get("last_heartbeat", 0)
                status = pinfo.get("status", "alive")
                if status == "alive" and (now - last) > TIMEOUT:
                    to_mark.append(pid)
        for pid in to_mark:
            mark_peer_dead(pid)
        time.sleep(CHECK_INTERVAL)

# API endpoints

@app.route("/register", methods=["POST"])
def register():
    """
    Register a new peer.
    JSON body: { "id": str, "host": str, "port": int, "files": { filename: [chunk_hash,...] } (optional) }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "invalid json"}), 400
    peer_id = data.get("id")
    host = data.get("host")
    port = data.get("port")
    files = data.get("files", None)
    if not peer_id or not host or not port:
        return jsonify({"error": "id, host and port required"}), 400
    update_peer_heartbeat(peer_id, host=host, port=port, files=files)
    log(f"Registered/Updated peer {peer_id} ({host}:{port})")
    return jsonify({"status": "ok", "peer_id": peer_id}), 200

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """
    Heartbeat from peer.
    JSON body: { "id": str, "host": str (optional), "port": int (optional) }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "invalid json"}), 400
    peer_id = data.get("id")
    host = data.get("host")
    port = data.get("port")
    if not peer_id:
        return jsonify({"error": "id required"}), 400
    update_peer_heartbeat(peer_id, host=host, port=port)
    return jsonify({"status": "ok"}), 200

@app.route("/publish", methods=["POST"])
def publish():
    """
    Peer publishes file metadata to tracker.
    JSON body: {
      "id": str,         # peer id
      "file": str,       # filename
      "size": int,       # optional
      "chunks": [ "hash1", "hash2", ... ]  # list of chunk hashes
    }
    Tracker updates file->chunk->peer mapping and peer.files
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "invalid json"}), 400
    peer_id = data.get("id")
    filename = data.get("file")
    chunks = data.get("chunks")
    size = data.get("size", None)
    if not peer_id or not filename or not isinstance(chunks, list):
        return jsonify({"error": "id, file, chunks (list) required"}), 400

    now = time.time()
    with metadata_lock:
        # update peer's file list
        peer = METADATA["peers"].setdefault(peer_id, {})
        peer.setdefault("files", {})
        peer["files"][filename] = list(chunks)
        peer["last_heartbeat"] = now
        peer["status"] = "alive"
        # update global files map
        file_entry = METADATA["files"].setdefault(filename, {"chunks": {}, "size": size, "uploaded_by": peer_id})
        if size is not None:
            file_entry["size"] = size
        file_entry.setdefault("chunks", {})
        for h in chunks:
            lst = file_entry["chunks"].setdefault(h, [])
            if peer_id not in lst:
                lst.append(peer_id)
        METADATA["updated_at"] = now
        safe_write_metadata()

    log(f"Peer {peer_id} published file {filename} ({len(chunks)} chunks)")
    return jsonify({"status": "ok"}), 200

@app.route("/lookup/<path:filename>", methods=["GET"])
def lookup(filename):
    """
    Return the list of peers that have chunks for the requested file.
    Response:
    {
      "file": filename,
      "chunks": {
         "chunk_hash": [ { "peer_id": id, "host": host, "port": port, "status": "alive" }, ... ]
      }
    }
    """
    with metadata_lock:
        fe = METADATA["files"].get(filename)
        if not fe:
            return jsonify({"error": "file not found"}), 404
        chunks_info = {}
        for chash, peers in fe.get("chunks", {}).items():
            peers_info = []
            for pid in peers:
                pinfo = METADATA["peers"].get(pid, {})
                peers_info.append({
                    "peer_id": pid,
                    "host": pinfo.get("host"),
                    "port": pinfo.get("port"),
                    "status": pinfo.get("status", "unknown")
                })
            chunks_info[chash] = peers_info
    return jsonify({"file": filename, "chunks": chunks_info}), 200

@app.route("/peers", methods=["GET"])
def peers():
    """Return peer registry and statuses."""
    with metadata_lock:
        return jsonify(METADATA.get("peers", {})), 200

@app.route("/files", methods=["GET"])
def files():
    """Return files metadata."""
    with metadata_lock:
        return jsonify(METADATA.get("files", {})), 200

@app.route("/update_file", methods=["POST"])
def update_file():
    """
    Update file metadata manually.
    JSON: { "file": str, "chunks": { "hash": [peer_id,...] }, "size": int (optional) }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "invalid json"}), 400
    filename = data.get("file")
    chunks_map = data.get("chunks")
    size = data.get("size", None)
    if not filename or not isinstance(chunks_map, dict):
        return jsonify({"error": "file and chunks map required"}), 400
    with metadata_lock:
        entry = METADATA["files"].setdefault(filename, {"chunks": {}, "size": size})
        entry["chunks"] = {k: list(v) for k, v in chunks_map.items()}
        if size is not None:
            entry["size"] = size
        METADATA["updated_at"] = time.time()
        safe_write_metadata()
    log(f"File {filename} metadata updated manually")
    return jsonify({"status": "ok"}), 200

@app.route("/summary", methods=["GET"])
def summary():
    """Short summary used by monitoring scripts."""
    with metadata_lock:
        total_peers = len(METADATA.get("peers", {}))
        alive = sum(1 for p in METADATA.get("peers", {}).values() if p.get("status") == "alive")
        dead = total_peers - alive
        total_files = len(METADATA.get("files", {}))
    return jsonify({
        "peers_total": total_peers,
        "peers_alive": alive,
        "peers_dead": dead,
        "files_total": total_files,
        "timestamp": time.time()
    }), 200

# Small health endpoint
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": time.time()}), 200

def start_background_scanner():
    t = threading.Thread(target=scan_for_dead_peers_loop, daemon=True)
    t.start()

if __name__ == "__main__":
    # when running as main: start background scanner and Flask
    start_background_scanner()
    log("Starting Tracker server on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)