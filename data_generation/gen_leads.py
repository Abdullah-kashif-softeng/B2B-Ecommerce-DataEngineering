"""Generate marketing lead spreadsheets, one .xlsx per day -> output/leads/leads_YYYY-MM-DD.xlsx
  python gen_leads.py backfill | python gen_leads.py daily [DATE]
Won leads match a buyer company by name (after cleaning), so lead -> order joins work.
"""
import os, random
from datetime import timedelta
import pandas as pd
from common import *

# core KPI columns, then extras
COLS = ["lead_id", "created_date", "source", "campaign_name", "utm_source", "utm_medium", "utm_campaign", "country",
        "city", "company_name", "company_size", "industry", "lead_score", "estimated_order_volume", "status",
        "utm_content", "contact_name", "contact_email", "phone", "job_title", "sales_rep", "last_contacted_date"]


def messy(rng, l):
    r = dict(l)
    won = r["status"] == "Won"  # converted leads keep a usable contact email (it links to the customer)
    if HARD_MODE:
        if rng.random() < .08:  # typo in company name
            n = r["company_name"]; k = rng.randint(2, len(n) - 3)
            r["company_name"] = n[:k] + n[k + 1:]
        if rng.random() < .02:
            r["lead_score"] = rng.choice([150, -5, 999])  # out of range
        if rng.random() < .01:
            r["created_date"] = r["created_date"] + timedelta(rng.randint(60, 400))  # future date
        if not won and rng.random() < .03:
            r["contact_email"] = r["contact_email"].replace("@", "")  # invalid email
    if rng.random() < .08:
        r["source"] = rng.choice([r["source"].upper(), r["source"].lower(), f" {r['source']} "])
    if rng.random() < .08:
        r["country"] = rng.choice([r["country"].upper(), COUNTRIES[r["country"]], r["country"].lower()])
    if rng.random() < .05:
        r["lead_score"] = rng.choice([None, "N/A", ""])
    if rng.random() < .05:
        r["estimated_order_volume"] = rng.choice([None, f"${r['estimated_order_volume']:,}"])
    if rng.random() < .05:
        r["created_date"] = r["created_date"].strftime("%d/%m/%Y")  # mixed date formats
    if rng.random() < .05:
        r["company_name"] = rng.choice([r["company_name"].upper(), r["company_name"].replace(".", ""), r["company_name"] + "  "])
    if rng.random() < .04:
        r["city"] = None
    if rng.random() < .06:
        r["contact_email"] = r["contact_email"].upper() if won else rng.choice([None, r["contact_email"].upper()])
    if r["phone"] and rng.random() < .06:
        r["phone"] = rng.choice([None, r["phone"].replace(" ", "").replace("-", "")])
    return r


mode, days = days_from_cli()
os.makedirs(f"{OUT_DIR}/leads", exist_ok=True)
total = 0
for day in days:
    leads = build_leads(day)
    if not leads:
        continue
    rng = random.Random(f"{SEED}-leadmess-{day}")
    rows = []
    for l in leads:
        rows.append(messy(rng, l))
        if rng.random() < .02:  # duplicate row
            rows.append(dict(rows[-1]))
    skip = 2 if rng.random() < .03 else 0  # some exports have a junk title row above the header
    with pd.ExcelWriter(f"{OUT_DIR}/leads/leads_{day}.xlsx", engine="openpyxl") as w:
        pd.DataFrame(rows, columns=COLS).to_excel(w, index=False, startrow=skip)
        if skip:
            w.sheets["Sheet1"]["A1"] = "Marketing leads export"
    total += len(rows)
print(f"{mode}: {total} lead rows written")