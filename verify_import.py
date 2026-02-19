"""Quick verification script to check imported questions via Laravel API."""
import requests
import json

BASE = "http://localhost:8000"

# Login
r = requests.post(
    f"{BASE}/api/login",
    json={"email": "info@coreminds.in", "password": "aq1sw2de3"},
    headers={"Accept": "application/json"},
)
token = r.json()["data"]["token"]
print(f"Logged in OK (token: {token[:20]}...)")

# Get batches
r2 = requests.get(
    f"{BASE}/api/v1/import/batches",
    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
)
data = r2.json()["data"]

print(f"\n=== Import Batches ({len(data)} total) ===")
for b in data:
    bid = b["batch_id"][:8]
    print(
        f"  Batch {bid}... | Status: {b['status']} | "
        f"Success: {b['successful_imports']} | Failed: {b['failed_imports']} | "
        f"Created: {b['created_at']}"
    )

# Get the latest batch status
if data:
    latest = data[0]
    r3 = requests.get(
        f"{BASE}/api/v1/import/status/{latest['batch_id']}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    detail = r3.json()["data"]
    print(f"\n=== Latest Batch Detail ===")
    print(json.dumps(detail, indent=2))

print("\nDone!")
