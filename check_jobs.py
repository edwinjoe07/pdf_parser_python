import urllib.request
import json

try:
    with urllib.request.urlopen("http://localhost:5000/api/jobs") as response:
        data = json.loads(response.read().decode())
        found = [j for j in data if "MB-820" in str(j)]
        print(json.dumps(found, indent=2))
except Exception as e:
    print(f"Error: {e}")
