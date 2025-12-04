import requests
from bs4 import BeautifulSoup
import json
import sys
from urllib.parse import urlparse

def fetch_and_save_ld_json(url):
    # Fetch page
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    # Parse HTML
    soup = BeautifulSoup(response.text, "html.parser")

    # Find <script type="application/ld+json">
    script_tag = soup.find("script", {"type": "application/ld+json"})
    if not script_tag:
        raise ValueError("No ld+json script tag found!")

    # Extract JSON
    raw_json = script_tag.string.strip()
    data = json.loads(raw_json)

    # Create filename based on last path segment
    path_segment = urlparse(url).path.strip("/").split("/")[-1] or "data"
    filename = f"{path_segment}.json"

    # Save to file
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"Saved JSON to {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <URL>")
        sys.exit(1)

    url = sys.argv[1]
    fetch_and_save_ld_json(url)
