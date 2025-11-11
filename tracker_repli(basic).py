# tracker.py
from flask import Flask, request, jsonify
from datetime import datetime, timezone
import json, threading, time, requests

app = Flask(__name__)

METADATA_FILE = "metadata.json"
REPLICATION_FACTOR = 2
HEARTBEAT_TIMEOUT = 30  # giây
CHECK_INTERVAL = 10     # giây, kiểm tra replication

# ---------------------
# Load / Save metadata
# ---------------------
try:
    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)
except:
    metadata = {"peers": {}, "files": {}}

metadata_lock = threading.Lock()

def save_metadata():
    with metadata_lock:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)

# ---------------------
# Timestamp helper
# ---------------------
def ts():
    return datetime.now(timezone.utc).isoformat()

# ---------------------
# Peer APIs
# ---------------------
@app.route("/register", methods=["POST"])
def register():
    data = request.json
    peer_id = data["id"]
    host = data["host"]
    port = data["port"]
    with metadata_lock:
        metadata["peers"][peer_id] = {
            "host": host,
            "port": port,
            "status": "alive",
            "last_heartbeat": time.time(),
            "files": {}
        }
    save_metadata()
    return jsonify({"peer_id": peer_id, "status": "ok"})

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json
    peer_id = data["id"]
    with metadata_lock:
        if peer_id in metadata["peers"]:
            metadata["peers"][peer_id]["last_heartbeat"] = time.time()
            metadata["peers"][peer_id]["status"] = "alive"
    save_metadata()
    return jsonify({"status": "ok"})

@app.route("/publish", methods=["POST"])
def publish():
    data = request.json
    peer_id = data["id"]
    file_name = data["file"]
    size = data.get("size", 0)
    chunks = data.get("chunks", [])
    with metadata_lock:
        # Peer files
        metadata["peers"][peer_id]["files"][file_name] = chunks
        # Global files
        if file_name not in metadata["files"]:
            metadata["files"][file_name] = {"chunks": {}, "size": size, "uploaded_by": peer_id}
        for ch in chunks:
            if ch not in metadata["files"][file_name]["chunks"]:
                metadata["files"][file_name]["chunks"][ch] = []
            if peer_id not in metadata["files"][file_name]["chunks"][ch]:
                metadata["files"][file_name]["chunks"][ch].append(peer_id)
    save_metadata()
    return jsonify({"status": "ok"})

@app.route("/lookup/<file_name>", methods=["GET"])
def lookup(file_name):
    with metadata_lock:
        file_info = metadata["files"].get(file_name, {})
        chunks = {}
        for ch, peers in file_info.get("chunks", {}).items():
            chunks[ch] = []
            for pid in peers:
                peer = metadata["peers"].get(pid)
                if peer:
                    chunks[ch].append({
                        "peer_id": pid,
                        "host": peer["host"],
                        "port": peer["port"],
                        "status": peer["status"]
                    })
    return jsonify({"file": file_name, "chunks": chunks})

@app.route("/peers", methods=["GET"])
def peers():
    with metadata_lock:
        return jsonify(metadata["peers"])

@app.route("/files", methods=["GET"])
def files():
    with metadata_lock:
        return jsonify(metadata["files"])

# ---------------------
# Replication API from peer
# ---------------------
@app.route("/replicate_done", methods=["POST"])
def replicate_done():
    data = request.json
    file_name = data["file"]
    chunk_hash = data["chunk"]
    peer_id = data["peer_id"]
    with metadata_lock:
        if file_name in metadata["files"]:
            if chunk_hash not in metadata["files"][file_name]["chunks"]:
                metadata["files"][file_name]["chunks"][chunk_hash] = []
            if peer_id not in metadata["files"][file_name]["chunks"][chunk_hash]:
                metadata["files"][file_name]["chunks"][chunk_hash].append(peer_id)
    save_metadata()
    return jsonify({"status": "ok"})

# ---------------------
# Replication Manager Thread
# ---------------------
def replication_manager():
    while True:
        time.sleep(CHECK_INTERVAL)
        with metadata_lock:
            now = time.time()
            # update peer status
            for pid, peer in metadata["peers"].items():
                if now - peer["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                    peer["status"] = "dead"
            # check replication
            for fname, finfo in metadata["files"].items():
                for ch, peers_with_chunk in finfo["chunks"].items():
                    alive_peers = [p for p in peers_with_chunk if metadata["peers"].get(p, {}).get("status")=="alive"]
                    if len(alive_peers) < REPLICATION_FACTOR and alive_peers:
                        # select source and destination
                        src = alive_peers[0]
                        # find destination peer that does not have chunk
                        dst_candidates = [p for p in metadata["peers"] if metadata["peers"][p]["status"]=="alive" and p not in peers_with_chunk]
                        if dst_candidates:
                            dst = dst_candidates[0]
                            # send replicate command
                            src_peer = metadata["peers"][src]
                            dst_peer = metadata["peers"][dst]
                            try:
                                requests.post(f"http://{src_peer['host']}:{src_peer['port']}/replicate", json={
                                    "file": fname,
                                    "chunk": ch,
                                    "dst_host": dst_peer["host"],
                                    "dst_port": dst_peer["port"],
                                    "dst_id": dst
                                }, timeout=5)
                                print(f"[{ts()}] Replication triggered: {fname}/{ch} {src} -> {dst}")
                            except Exception as e:
                                print(f"[{ts()}] Replication error: {e}")
        save_metadata()

# ---------------------
# Start thread
# ---------------------
threading.Thread(target=replication_manager, daemon=True).start()

# ---------------------
# Run Flask
# ---------------------
if __name__ == "__main__":
    print(f"[{ts()}] Starting Tracker server on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000)