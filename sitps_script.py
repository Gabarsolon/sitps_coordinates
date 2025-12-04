# geocode_itp.py
import time
import json
import re
import requests

from pathlib import Path

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderUnavailable, GeocoderServiceError

INPUT_CSV  = "statii_itp_fara_diacritice.csv"   # pune aici fișierul tău
OUTPUT_CSV = "statii_itp_geoloc.csv"
OUTPUT_CSV_NEW = "statii_itp_geoloc_cu_rarom_api.csv"
CACHE_JSON = "geocode_cache.json"
RAROM_SITP_API = "https://portal.rarom.ro/rarApi/public/RarPublicAuthorizations/ITP";
STATIONS_COORDINATES_JSON = "stations_coordinates.json";

# 1) Normalizare adrese 
ABBR_MAP = {
    r"\bSTR\.\b": "Strada ",
    r"\bSOS\.\b": "Soseaua ",
    r"\bBD\.\b":  "Bulevardul ",
    r"\bjud\.\b": "judetul ",
}

def count_ok_status(df: pd.DataFrame) -> int:
    """Count the number of rows where the column STATUS is OK"""
    return len(df.loc[(df["GEOCODER_STATUS"] == "OK") | (df["GEOCODER_STATUS"] == "FALLBACK_NO_NUMBER")])

df = pd.read_csv(OUTPUT_CSV, sep=None, engine="python")
# df2 = pd.read_csv(OUTPUT_CSV_NEW, sep=None, engine="python")
print("OK in first file:", count_ok_status(df))
# print("OK in second file:", count_ok_status(df2))

#Load stations json
stations_path = Path(STATIONS_COORDINATES_JSON)
stations_json = json.loads(stations_path.read_text(encoding="utf-8"))

def normalize_address(addr: str) -> str:
    if not isinstance(addr, str):
        return ""
    s = addr.strip()
    for pat, repl in ABBR_MAP.items():
        s = re.sub(pat, repl, s, flags=re.IGNORECASE)
    # Adaugă "Romania" dacă lipsește
    if "Romania" not in s and "România" not in s:
        s = s + ", Romania"
    return s

# 2) Cache
def load_cache(path: str) -> dict:
    p = Path(path)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(path: str, data: dict):
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    df = pd.read_csv(INPUT_CSV, sep=None, engine="python")
    if "ADRESA_SITP" not in df.columns:
        raise ValueError("Coloana 'ADRESA_SITP' nu există în CSV.")

    # Pregatire coloane
    for col in ["LATITUDINE", "LONGITUDINE", "GEOCODER_STATUS"]:
        if col not in df.columns:
            df[col] = None

    cache = load_cache(CACHE_JSON)

    # Geocoder + ratelimiter (1 cerere/sec, cu retry)
    geolocator = Nominatim(user_agent="RAR_ITP_Geocode/1.0 (contact: it@rar.gov.ro)")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.0, swallow_exceptions=False)

    def geocode_once(q: str):
        # 3 încercări cu backoff
        for attempt in range(3):
            try:
                return geocode(q, addressdetails=False, timeout=10)
            except (GeocoderUnavailable, GeocoderServiceError):
                time.sleep(2 * (attempt + 1))
        return None

    # Procesaree rand pe rand 
    save_every = 100
    processed = 0

    for idx, row in df.iterrows():
        # Sari peste ce e deja geocodat
        if pd.notna(row.get("LATITUDINE")) and pd.notna(row.get("LONGITUDINE")):
            continue

        raw_addr = str(row["ADRESA_SITP"])
        q = normalize_address(raw_addr)

       
        # Cache
        if q in cache:
            lat, lon, status = cache[q]
        elif row.get("COD_STATIE") in stations_json:
            cod_statie = row.get("COD_STATIE")
            lat, lon, status = stations_json[cod_statie]["latitude"], stations_json[cod_statie]["longitude"], "OK"
        else:
            # Use nominatim
            loc = geocode_once(q)
            if loc:
                lat, lon, status = loc.latitude, loc.longitude, "OK"
            else:
                # Dacă adresa exactă nu merge, încearcă fără număr (fallback)
                q_fallback = re.sub(r"\s+\d+[A-Za-z]?\b", "", q)
                loc2 = geocode_once(q_fallback) if q_fallback != q else None
                if loc2:
                    lat, lon, status = loc2.latitude, loc2.longitude, "FALLBACK_NO_NUMBER"
                else:
                     # Try to get the coordinates from rarom api 
                    url = RAROM_SITP_API + "?searchTerm=" + row.get("COD_STATIE")
                    response = requests.get(url)
                    json = response.json()
                    if json:
                        branch = json[0].get("branch", {})
                    else:
                        branch = {}
                    address = branch.get("address", {})
                    gps_location = address.get("gpsLocation")

                    if response.status_code == 200 and gps_location:
                        lat, lon = gps_location.split(",")
                        status = "OK"
                    else:
                        lat, lon, status = None, None, "NOT_FOUND"

            cache[q] = [lat, lon, status]

        df.at[idx, "LATITUDINE"] = lat
        df.at[idx, "LONGITUDINE"] = lon
        df.at[idx, "GEOCODER_STATUS"] = status

        processed += 1
        if processed % save_every == 0:
            df.to_csv(OUTPUT_CSV, index=False)
            save_cache(CACHE_JSON, cache)
            print(f"[INFO] Progres salvat la {processed} rânduri...")

    # Salvare
    df.to_csv(OUTPUT_CSV, index=False)
    save_cache(CACHE_JSON, cache)
    print("[DONE] Geocodare finalizată. Fișier:", OUTPUT_CSV)

# if __name__ == "main":
# main()