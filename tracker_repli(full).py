import threading
import time
import json
import os
import tempfile
from flask import Flask, request, jsonify
from datetime import datetime, timezone
import requests

HEARTBEAT_INTERVAL = 10
TIMEOUT = 30
METADATA_FILE = "metadata.json"
TRACKER_LOG = "tracker.log"
CHECK_INTERVAL = 10
REPLICATION_FACTOR = 2

app = Flask(__name__)

metadata_lock = threading.RLock()
if os.path.exists(METADATA_FILE):
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            METADATA = json.load(f)
    except Exception:
        METADATA = {}
else:
    METADATA = {}

METADATA.setdefault("peers", {})
METADATA.setdefault("files", {})
METADATA.setdefault("created_at", time.time())

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

def update_peer_heartbeat(peer_id: str, host: str = None, port: int = None, files: dict = None):
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

def scan_for_dead_peers_loop():
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

def replication_manager_loop():
    while True:
        time.sleep(CHECK_INTERVAL)
        with metadata_lock:
            for fname, finfo in METADATA.get("files", {}).items():
                for ch, peers_with_chunk in finfo.get("chunks", {}).items():
                    alive_peers = [p for p in peers_with_chunk if METADATA["peers"].get(p, {}).get("status")=="alive"]
                    if len(alive_peers) < REPLICATION_FACTOR and alive_peers:
                        src = alive_peers[0]
                        dst_candidates = [p for p in METADATA["peers"] if METADATA["peers"][p]["status"]=="alive" and p not in peers_with_chunk]
                        if dst_candidates:
                            dst = dst_candidates[0]
                            src_peer = METADATA["peers"][src]
                            dst_peer = METADATA["peers"][dst]
                            try:
                                requests.post(f"http://{src_peer['host']}:{src_peer['port']}/replicate", json={
                                    "file": fname,
                                    "chunk": ch,
                                    "dst_host": dst_peer["host"],
                                    "dst_port": dst_peer["port"],
                                    "dst_id": dst
                                }, timeout=5)
                                log(f"Replication triggered: {fname}/{ch} {src} -> {dst}")
                            except Exception as e:
                                log(f"Replication error: {e}")
        safe_write_metadata()

@app.route("/register", methods=["POST"])
def register():
    data = request.get_json(force=True, silent=True)
    peer_id = data.get("id")
    host = data.get("host")
    port = data.get("port")
    files = data.get("files", None)
    if not peer_id or not host or not port:
        return jsonify({"error": "id, host, port required"}), 400
    update_peer_heartbeat(peer_id, host=host, port=port, files=files)
    log(f"Registered/Updated peer {peer_id} ({host}:{port})")
    return jsonify({"status": "ok", "peer_id": peer_id}), 200

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json(force=True, silent=True)
    peer_id = data.get("id")
    host = data.get("host")
    port = data.get("port")
    if not peer_id:
        return jsonify({"error": "id required"}), 400
    update_peer_heartbeat(peer_id, host=host, port=port)
    return jsonify({"status": "ok"}), 200

@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json(force=True, silent=True)
    peer_id = data.get("id")
    filename = data.get("file")
    chunks = data.get("chunks")
    size = data.get("size", None)
    if not peer_id or not filename or not isinstance(chunks, list):
        return jsonify({"error": "id, file, chunks required"}), 400
    now = time.time()
    with metadata_lock:
        peer = METADATA["peers"].setdefault(peer_id, {})
        peer.setdefault("files", {})
        peer["files"][filename] = list(chunks)
        peer["last_heartbeat"] = now
        peer["status"] = "alive"
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
    with metadata_lock:
        return jsonify(METADATA.get("peers", {})), 200

@app.route("/files", methods=["GET"])
def files():
    with metadata_lock:
        return jsonify(METADATA.get("files", {})), 200

@app.route("/replicate_done", methods=["POST"])
def replicate_done():
    data = request.get_json(force=True, silent=True)
    file_name = data["file"]
    chunk_hash = data["chunk"]
    peer_id = data["peer_id"]
    with metadata_lock:
        if file_name in METADATA["files"]:
            if chunk_hash not in METADATA["files"][file_name]["chunks"]:
                METADATA["files"][file_name]["chunks"][chunk_hash] = []
            if peer_id not in METADATA["files"][file_name]["chunks"][chunk_hash]:
                METADATA["files"][file_name]["chunks"][chunk_hash].append(peer_id)
        METADATA["updated_at"] = time.time()
        safe_write_metadata()
    return jsonify({"status": "ok"})

@app.route("/summary", methods=["GET"])
def summary():
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

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": time.time()}), 200

def start_background_threads():
    t1 = threading.Thread(target=scan_for_dead_peers_loop, daemon=True)
    t1.start()
    t2 = threading.Thread(target=replication_manager_loop, daemon=True)
    t2.start()

if __name__ == "__main__":
    start_background_threads()
    log("Starting Tracker server on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)