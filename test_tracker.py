import requests
import time
import json

BASE = "http://127.0.0.1:5000"

def pretty(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False))

def test_register():
    print("\n== Test /register ==")
    r = requests.post(f"{BASE}/register", json={
        "id": "peer1",
        "host": "127.0.0.1",
        "port": 9001
    })
    pretty(r.json())

def test_heartbeat():
    print("\n== Test /heartbeat ==")
    r = requests.post(f"{BASE}/heartbeat", json={"id": "peer1"})
    pretty(r.json())

def test_publish():
    print("\n== Test /publish ==")
    data = {
        "id": "peer1",
        "file": "data.txt",
        "size": 1234,
        "chunks": ["hash1", "hash2", "hash3"]
    }
    r = requests.post(f"{BASE}/publish", json=data)
    pretty(r.json())

def test_lookup():
    print("\n== Test /lookup/data.txt ==")
    r = requests.get(f"{BASE}/lookup/data.txt")
    pretty(r.json())

def test_peers():
    print("\n== Test /peers ==")
    r = requests.get(f"{BASE}/peers")
    pretty(r.json())

def test_files():
    print("\n== Test /files ==")
    r = requests.get(f"{BASE}/files")
    pretty(r.json())

def test_heartbeat_timeout():
    print("\n== Test heartbeat timeout ==")
    print("Đợi 35s (TIMEOUT = 30s)...")
    time.sleep(35)
    r = requests.get(f"{BASE}/peers")
    data = r.json()
    pretty(data)
    peer = data.get("peer1", {})
    print("Status:", peer.get("status"))

def test_summary():
    print("\n== Test /summary ==")
    r = requests.get(f"{BASE}/summary")
    pretty(r.json())

def test_health():
    print("\n== Test /health ==")
    r = requests.get(f"{BASE}/health")
    pretty(r.json())

if __name__ == "__main__":
    test_register()
    test_heartbeat()
    test_publish()
    test_lookup()
    test_peers()
    test_files()
    test_summary()
    test_health()
    # kiểm tra sau timeout (nếu muốn)
    test_heartbeat_timeout()