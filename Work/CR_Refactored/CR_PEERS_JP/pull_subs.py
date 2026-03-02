import re
import time
import requests
import pandas as pd
from functools import lru_cache

HEADERS = {
    "User-Agent": "YourAppName your.email@domain.com",
    "Accept-Encoding": "gzip, deflate",
}

@lru_cache(maxsize=1)
def load_ticker_to_cik10():
    # SEC mapping file (ticker -> cik_str)
    data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS, timeout=30).json()
    m = {}
    for _, v in data.items():
        m[v["ticker"].upper()] = str(v["cik_str"]).zfill(10)
    return m

def get_latest_10k_accession(cik10: str) -> str:
    j = requests.get(f"https://data.sec.gov/submissions/CIK{cik10}.json", headers=HEADERS, timeout=30).json()
    recent = pd.DataFrame(j["filings"]["recent"])
    recent = recent[recent["form"].isin(["10-K", "10-K/A"])].copy()
    if recent.empty:
        raise ValueError(f"No recent 10-K found for CIK {cik10}")
    recent.sort_values("filingDate", ascending=False, inplace=True)
    return recent.iloc[0]["accessionNumber"]  # e.g., 0000320193-24-000106

def filing_base_dir(cik10: str, accession: str) -> str:
    cik_nolead = str(int(cik10))  # EDGAR path uses no leading zeros
    acc_nodash = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_nolead}/{acc_nodash}/"

def find_ex21_document_url(base_dir: str, accession: str) -> str:
    index_url = f"{base_dir}{accession}-index.html"
    # The “Documents” table usually has columns like: Seq, Description, Document, Type, Size
    tables = pd.read_html(index_url)
    docs = next(t for t in tables if "Document" in t.columns and "Type" in t.columns)

    ex21 = docs[docs["Type"].astype(str).str.contains(r"EX-21", na=False)]
    if ex21.empty:
        # fallback heuristic
        ex21 = docs[docs["Description"].astype(str).str.contains("subsidiar", case=False, na=False)]

    if ex21.empty:
        raise ValueError("EX-21 not found in filing index")

    doc_name = str(ex21.iloc[0]["Document"]).strip()
    return f"{base_dir}{doc_name}"

def parse_ex21_tables(ex21_url: str) -> pd.DataFrame:
    # Many EX-21 exhibits are one or more HTML tables
    dfs = pd.read_html(ex21_url)
    out = pd.concat(dfs, ignore_index=True)

    # light cleanup (you will want company-specific normalization)
    out.columns = [str(c).strip() for c in out.columns]
    out = out.dropna(how="all")
    return out

def get_subsidiaries_from_ticker(ticker: str) -> pd.DataFrame:
    cik10 = load_ticker_to_cik10()[ticker.upper()]
    time.sleep(0.2)  # be polite; keep request rate low
    accession = get_latest_10k_accession(cik10)
    base = filing_base_dir(cik10, accession)
    time.sleep(0.2)
    ex21_url = find_ex21_document_url(base, accession)
    time.sleep(0.2)
    subs = parse_ex21_tables(ex21_url)

    subs["source_accession"] = accession
    subs["source_ex21_url"] = ex21_url
    return subs

# Example:
df = get_subsidiaries_from_ticker("MSFT")
df.to_csv("msft_ex21_subsidiaries.csv", index=False)
