import requests
import json
import os
from pathlib import Path

# Where to store the last seen release
STATE_FILE = Path(".github/scripts/last_duckdb_release.txt")

# Get latest DuckDB release
duckdb_release_url = "https://api.github.com/repos/duckdb/duckdb/releases/latest"
headers = {"Accept": "application/vnd.github+json"}

response = requests.get(duckdb_release_url, headers=headers)
response.raise_for_status()
latest_release = response.json()["tag_name"]

# Load last seen release
if STATE_FILE.exists():
    with open(STATE_FILE) as f:
        last_seen = f.read().strip()
else:
    last_seen = None

if latest_release != last_seen:
    print(f"New DuckDB release detected: {latest_release}")
    issue_title = f"[Update] Support DuckDB {latest_release}"
    issue_body = (
        f"A new DuckDB version `{latest_release}` has been released.\n\n"
        f"Please update the extension to support this version.\n\n"
        f"For instructions, see [#108](https://github.com/cwida/duckpgq-extension/issues/108).\n\n"
        f"Previous similar issue: [#231](https://github.com/cwida/duckpgq-extension/issues/231)"
    )

    repo = "cwida/duckpgq-extension"
    token = os.environ["GITHUB_TOKEN"]
    issues_url = f"https://api.github.com/repos/{repo}/issues"
    auth_headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    # Create the issue
    issue_data = {"title": issue_title, "body": issue_body}
    r = requests.post(issues_url, headers=auth_headers, json=issue_data)
    r.raise_for_status()
    print(f"Issue created: {r.json()['html_url']}")

    # Store the new release
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        f.write(latest_release)
else:
    print(f"No new release. Last seen: {last_seen}")