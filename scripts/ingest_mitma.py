from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import requests
import xml.etree.ElementTree as ET
import re

MITMA_RSS_URL = "https://movilidad-opendata.mitma.es/RSS.xml"
BRONZE_DIR = "lakehouse/bronze/mitma"

def download_file(url, filename):
    out_path = os.path.join(BRONZE_DIR, filename)
    if os.path.exists(out_path):
        return f"[SKIP] {filename}"

    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

    return f"[OK] {filename}"


def ingest_mitma(polygon=None, start_date=None, end_date=None):
    os.makedirs(BRONZE_DIR, exist_ok=True)

    start_ym = start_date[:7].replace("-", "")
    end_ym = end_date[:7].replace("-", "")

    response = requests.get(MITMA_RSS_URL, timeout=60)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    items = root.findall(".//item")

    jobs = []

    for item in items:
        url = item.findtext("link")
        if not url:
            continue

        filename = os.path.basename(url)

        # SOLO datos agregados mensuales
        if not filename.endswith("_datos_agregados.csv"):
            continue

        m = re.match(r"(20\d{2})(0[1-9]|1[0-2])_datos_agregados\.csv", filename)
        if not m:
            continue

        ym = m.group(1) + m.group(2)
        if ym < start_ym or ym > end_ym:
            continue

        jobs.append((url, filename))

    print(f"[MITMA] Descargando {len(jobs)} ficheros en paralelo")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(download_file, url, filename)
            for url, filename in jobs
        ]

        for future in as_completed(futures):
            print("[MITMA]", future.result())
