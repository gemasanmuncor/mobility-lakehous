import requests
import xml.etree.ElementTree as ET

MITMA_XML_URL = "https://www.transportes.gob.es/.../rss.xml"

def get_mitma_files():
    response = requests.get(MITMA_XML_URL)
    root = ET.fromstring(response.content)

    files = []
    for item in root.findall(".//item"):
        url = item.find("link").text
        title = item.find("title").text
        files.append({
            "url": url,
            "title": title
        })
    return files
