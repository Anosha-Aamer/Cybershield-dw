"""Build the reference mapping files that turn anonymised codes into business names.

    Location n -> office site, city, province, region   (data/reference/location_map.csv)
    Category n -> cyber incident type, attack family     (data/reference/category_map.csv)

The mapping is deterministic (sorted codes, fixed seed), so every team member gets the same files.
Run:  python generator/build_reference_maps.py
"""
import random
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "incident_event_log.csv"
OUT = ROOT / "data" / "reference"

# city, province, region, weight (bigger cities get more sites)
CITIES = [
    ("Lahore", "Punjab", "Central", 14), ("Karachi", "Sindh", "South", 16),
    ("Islamabad", "Islamabad Capital Territory", "North", 12), ("Rawalpindi", "Punjab", "North", 8),
    ("Faisalabad", "Punjab", "Central", 8), ("Multan", "Punjab", "South Punjab", 7),
    ("Peshawar", "Khyber Pakhtunkhwa", "North West", 7), ("Quetta", "Balochistan", "West", 5),
    ("Hyderabad", "Sindh", "South", 5), ("Gujranwala", "Punjab", "Central", 5),
    ("Sialkot", "Punjab", "Central", 5), ("Bahawalpur", "Punjab", "South Punjab", 4),
    ("Sargodha", "Punjab", "Central", 4), ("Sukkur", "Sindh", "South", 4),
    ("Abbottabad", "Khyber Pakhtunkhwa", "North West", 4), ("Mardan", "Khyber Pakhtunkhwa", "North West", 3),
    ("Gwadar", "Balochistan", "West", 3), ("Muzaffarabad", "Azad Jammu & Kashmir", "North", 3),
    ("Gilgit", "Gilgit-Baltistan", "North", 3), ("Sahiwal", "Punjab", "Central", 3),
    ("Dera Ghazi Khan", "Punjab", "South Punjab", 3), ("Larkana", "Sindh", "South", 3),
    ("Mirpur", "Azad Jammu & Kashmir", "North", 3), ("Jhelum", "Punjab", "North", 3),
    ("Rahim Yar Khan", "Punjab", "South Punjab", 3),
]
SITE_TYPES = ["Head Office", "Regional Office", "Branch Office", "Data Center", "Service Center",
              "Operations Hub", "Training Campus", "Warehouse", "Call Center", "Disaster Recovery Site",
              "Sales Office", "Support Desk", "Branch Office 2", "Branch Office 3", "Branch Office 4",
              "Branch Office 5"]

# (incident type, attack family) ordered roughly by how common they are in a real SOC
CATEGORIES = [
    ("Phishing Email", "Social Engineering"), ("Malware Infection", "Malicious Code"),
    ("Unauthorized Access", "Access Violation"), ("Account Compromise", "Access Violation"),
    ("Suspicious Login", "Access Violation"), ("Spam / Scam Message", "Social Engineering"),
    ("Password Reset Abuse", "Access Violation"), ("Policy Violation", "Insider / Policy"),
    ("Lost or Stolen Device", "Physical Security"), ("Vulnerability Report", "Vulnerability"),
    ("Ransomware", "Malicious Code"), ("Data Leakage", "Data Breach"),
    ("Privilege Escalation", "Access Violation"), ("Web Application Attack", "Network / Web Attack"),
    ("Brute Force Attack", "Access Violation"), ("Trojan / Backdoor", "Malicious Code"),
    ("Spear Phishing", "Social Engineering"), ("Insider Threat", "Insider / Policy"),
    ("DDoS Attack", "Network / Web Attack"), ("Port Scanning", "Reconnaissance"),
    ("SQL Injection", "Network / Web Attack"), ("Cross-Site Scripting", "Network / Web Attack"),
    ("Worm Propagation", "Malicious Code"), ("Adware / PUP", "Malicious Code"),
    ("Misconfiguration", "Vulnerability"), ("Unpatched System", "Vulnerability"),
    ("Data Exfiltration", "Data Breach"), ("Email Spoofing", "Social Engineering"),
    ("USB Malware", "Malicious Code"), ("Rogue Access Point", "Network / Web Attack"),
    ("Physical Intrusion", "Physical Security"), ("Cryptojacking", "Malicious Code"),
    ("Business Email Compromise", "Social Engineering"), ("DNS Hijacking", "Network / Web Attack"),
    ("Man-in-the-Middle", "Network / Web Attack"), ("Credential Stuffing", "Access Violation"),
    ("Shadow IT Usage", "Insider / Policy"), ("Third-Party Breach", "Data Breach"),
    ("Zero-Day Exploit", "Vulnerability"), ("Keylogger", "Malicious Code"),
    ("Social Media Impersonation", "Social Engineering"),
]


def codes(series: pd.Series) -> list[int]:
    return sorted({int(v.split()[-1]) for v in series if v != "?"})


def main() -> None:
    df = pd.read_csv(RAW, dtype=str, keep_default_na=False, usecols=["location", "category"])
    OUT.mkdir(parents=True, exist_ok=True)
    rng = random.Random(2016)

    # ---- locations: busiest source locations go to the biggest cities.
    # City slots are ordered like a proportional (D'Hondt) allocation: weight / (slot + 1),
    # so Karachi/Lahore/Islamabad get the top sites and small cities get the quieter ones.
    freq = df.loc[df["location"] != "?", "location"].value_counts()
    loc_codes = [int(c.split()[-1]) for c in freq.index]
    slots = sorted(((w / (k + 1), rng.random(), c) for c in CITIES for k, w in [(k, c[3]) for k in range(20)]),
                   reverse=True)
    pool = [c for _, _, c in slots][: len(loc_codes)]
    used: dict[str, int] = {}
    rows = []
    for code, (city, prov, region, _) in zip(loc_codes, pool):
        i = used.get(city, 0)
        used[city] = i + 1
        site = SITE_TYPES[i] if i < len(SITE_TYPES) else f"Branch Office {i}"
        rows.append({"location_code": f"Location {code}", "site_name": f"{city} - {site}",
                     "city": city, "province": prov, "region": region, "country": "Pakistan"})
    pd.DataFrame(rows).sort_values("location_code", key=lambda s: s.str.split().str[-1].astype(int)) \
        .to_csv(OUT / "location_map.csv", index=False)

    # ---- categories: most frequent source category gets the most common incident type
    freq = df.loc[df["category"] != "?", "category"].value_counts()
    cat_rows = [{"category_code": code, "category_name": name, "attack_family": fam}
                for code, (name, fam) in zip(freq.index, CATEGORIES)]
    pd.DataFrame(cat_rows).sort_values("category_code", key=lambda s: s.str.split().str[-1].astype(int)) \
        .to_csv(OUT / "category_map.csv", index=False)

    print(f"location_map.csv: {len(rows)} rows, category_map.csv: {len(cat_rows)} rows")


if __name__ == "__main__":
    main()
