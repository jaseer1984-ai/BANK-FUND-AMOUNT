# app.py — Google Drive Amount Search + Inter-bank Pairing
# --------------------------------------------------------
# Run: streamlit run app.py

from __future__ import annotations
import os, io, re, tempfile
from datetime import timedelta, date
from typing import List, Tuple
import pandas as pd
import numpy as np
import streamlit as st

# ---- Google Drive (service account) ----
try:
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
    HAS_PYDRIVE2 = True
except Exception:
    HAS_PYDRIVE2 = False

st.set_page_config(page_title="Google Drive — Amount Search", layout="wide")
st.title("🔎 Google Drive — Amount Search (Bank Statements)")

# ---------------- Config ----------------
DATE_WINDOW_DAYS = 3        # for debit↔credit pairing window
AMOUNT_TOLERANCE = 0.05     # SAR tolerance for amount match

# Column aliases (lowercased)
HEADER_MAP = {
    "date":     ["value date","transaction date","trans: date","date","posted","posting date","processing date"],
    "narr":     ["details","description","transaction description","transaction details","remarks",
                 "narration","narration 1","narration 2","narration 3","narrative","narrative 1","narrative 2","narrative 3"],
    "debit":    ["debit","debit amount","amount dr.","debit (sar)","withdrawal","dr","amount dr"],
    "credit":   ["credit","credit amount","amount cr.","credit (sar)","deposit","cr","amount cr"],
    "amount":   ["amount","txn amount"],
    "balance":  ["balance","running balance","balance (sar)"],
    "account":  ["account","account no","iban"],
    "ref":      ["reference","reference no","reference number","customer reference","txt id","trace","utr","customer reference #"],
    "bank":     ["bank"],
}

# Banks for dropdowns (will be extended by data)
KNOWN_BANKS = ["SNB", "SABB/BSF", "ARB", "ANB", "RIB", "SIB", "NBK", "BAB", "INM"]

def detect_bank_from_name(name: str) -> str:
    n = name.lower()
    if "snb" in n or "ncb" in n: return "SNB"
    if "sabb" in n or "bsf" in n: return "SABB/BSF"
    if "arb" in n or "rajhi" in n: return "ARB"
    if "anb" in n: return "ANB"
    if "rib" in n: return "RIB"
    if "sib" in n: return "SIB"
    if "nbk" in n: return "NBK"
    if "bab" in n: return "BAB"
    if "inm" in n: return "INM"
    return os.path.splitext(name)[0].upper()

def pick_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    cols = [c.lower().strip() for c in df.columns]
    for c in candidates:
        if c in cols:
            return df.columns[cols.index(c)]
    return None

def read_any_excel_or_csv(content: bytes, name: str) -> pd.DataFrame:
    if name.lower().endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(content))
        except Exception:
            return pd.read_csv(io.BytesIO(content), sep=";")
    else:
        return pd.read_excel(io.BytesIO(content))

@st.cache_data(show_spinner=False)
def normalize_df(raw: pd.DataFrame, filename_hint: str) -> pd.DataFrame:
    c_date = pick_col(raw, HEADER_MAP["date"]) or raw.columns[0]
    c_narr = pick_col(raw, HEADER_MAP["narr"]) or raw.columns[min(1, len(raw.columns)-1)]
    c_deb  = pick_col(raw, HEADER_MAP["debit"])
    c_cred = pick_col(raw, HEADER_MAP["credit"])
    c_amt  = pick_col(raw, HEADER_MAP["amount"])
    c_bal  = pick_col(raw, HEADER_MAP["balance"]) or None
    c_acct = pick_col(raw, HEADER_MAP["account"]) or None
    c_ref  = pick_col(raw, HEADER_MAP["ref"]) or None
    c_bank = pick_col(raw, HEADER_MAP["bank"]) or None

    df = pd.DataFrame()
    df["date"] = pd.to_datetime(raw[c_date], errors="coerce").dt.date
    df["narration"] = raw[c_narr].astype(str).str.strip() if c_narr else ""

    debit  = pd.to_numeric(raw[c_deb], errors="coerce") if c_deb else None
    credit = pd.to_numeric(raw[c_cred], errors="coerce") if c_cred else None

    if c_amt:
        amt = pd.to_numeric(raw[c_amt], errors="coerce")
        if debit is None and credit is None:
            df["amount"] = amt
        else:
            df["amount"] = (credit.fillna(0) if credit is not None else 0) - (debit.fillna(0) if debit is not None else 0)
    else:
        if debit is not None or credit is not None:
            df["amount"] = (credit.fillna(0) if credit is not None else 0) - (debit.fillna(0) if debit is not None else 0)
        else:
            df["amount"] = pd.NA

    df["balance"] = pd.to_numeric(raw[c_bal], errors="coerce") if c_bal else pd.NA
    df["account"] = raw[c_acct].astype(str).str.strip() if c_acct else ""
    df["ref"] = raw[c_ref].astype(str).str.strip() if c_ref else ""

    if c_bank:
        df["bank"] = raw[c_bank].astype(str).str.strip()
    else:
        df["bank"] = detect_bank_from_name(filename_hint)

    df = df.dropna(subset=["date"]).copy()
    df = df[~df["amount"].isna()].copy()
    df["amount"] = df["amount"].astype(float)
    df["direction"] = np.where(df["amount"] < 0, "FROM (OUT)", "TO (IN)")
    df["abs_amount"] = df["amount"].abs().round(2)

    df["ref"] = df["ref"].str.replace("\n", " ").str.replace("\r", " ")
    df["narration"] = df["narration"].str.replace("\n", " ").str.replace("\r", " ")

    return df[["date","bank","account","narration","ref","amount","abs_amount","balance","direction"]]

# ---------- Google Drive list & download (fixed auth) ----------
def drive_list_and_download(sa_json: bytes, folder_id: str) -> List[Tuple[bytes,str]]:
    if not HAS_PYDRIVE2:
        st.error("pydrive2 not installed. Run: pip install pydrive2")
        return []

    # Write uploaded JSON to temp file
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp.write(sa_json)
    tmp.flush()

    # Authenticate with service account JSON
    gauth = GoogleAuth(settings={
        "client_config_backend": "service",
        "service_config": {"client_json_file_path": tmp.name},
    })
    gauth.ServiceAuth()  # ensures gauth.service is created
    drive = GoogleDrive(gauth)

    # List files in folder
    q = f"'{folder_id}' in parents and trashed=false"
    file_list = drive.ListFile({'q': q}).GetList()

    out: List[Tuple[bytes,str]] = []
    for f in file_list:
        name = f.get('title') or f.get('name')
        if not name.lower().endswith((".xlsx",".xls",".csv")):
            continue
        try:
            content = f.GetContentBinary()
            out.append((content, name))
        except Exception as e:
            st.warning(f"Failed to download {name}: {e}")
    return out

def parse_amount(text: str) -> float | None:
    s = text.strip().replace(",", " ").replace("\u066c", " ")
    s = re.sub(r"\s+", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        m = re.search(r"([0-9][0-9,]*)\.?([0-9]{0,2})", s)
        if not m: return None
        num = m.group(1).replace(",", "")
        dec = m.group(2) or ""
        return float(f"{num}.{dec}" if dec else num)

def pair_debit_credit(df: pd.DataFrame,
                      date_from: date | None,
                      date_to: date | None,
                      from_bank_hint: str | None,
                      to_bank_hint: str | None) -> pd.DataFrame:
    if df.empty:
        return df
    outs = df[df["amount"] < 0].copy()  # FROM
    ins  = df[df["amount"] > 0].copy()  # TO

    if from_bank_hint:
        outs = outs[outs["bank"].str.upper() == from_bank_hint.upper()]
    if to_bank_hint:
        ins  = ins[ins["bank"].str.upper() == to_bank_hint.upper()]

    if date_from:
        outs = outs[outs["date"] >= date_from]
        ins  = ins[ins["date"]  >= date_from]
    if date_to:
        outs = outs[outs["date"] <= date_to]
        ins  = ins[ins["date"]  <= date_to]

    matches, used_in = [], set()
    for _, o in outs.iterrows():
        cand = ins[(np.abs(ins["abs_amount"] - o["abs_amount"]) <= AMOUNT_TOLERANCE)]
        cand = cand[(pd.to_datetime(cand["date"]) >= pd.to_datetime(o["date"]) - pd.Timedelta(days=DATE_WINDOW_DAYS)) &
                    (pd.to_datetime(cand["date"]) <= pd.to_datetime(o["date"]) + pd.Timedelta(days=DATE_WINDOW_DAYS))]
        cand = cand[~cand.index.isin(used_in)]
        if cand.empty:
            continue
        cand = cand.assign(score=(pd.to_datetime(cand["date"]) - pd.to_datetime(o["date"])).abs().dt.days)
        m = cand.sort_values("score").iloc[0]
        used_in.add(m.name)
        matches.append({
            "date_from": o["date"], "bank_from": o["bank"], "acct_from": o["account"],
            "ref_from":  o["ref"],  "narr_from": o["narration"], "amt_from": o["amount"],
            "date_to":   m["date"], "bank_to":   m["bank"], "acct_to":   m["account"],
            "ref_to":    m["ref"],  "narr_to":   m["narration"], "amt_to":   m["amount"],
            "abs_amount": o["abs_amount"],
            "lag_days":   int(abs(pd.to_datetime(m["date"]) - pd.to_datetime(o["date"])).days),
        })
    return pd.DataFrame(matches)

def confirmation_line(row: pd.Series) -> str:
    return (
        f"DONE ✅ | {row['bank_from']}→{row['bank_to']} | SAR {row['abs_amount']:,.2f} "
        f"| DR Ref: {row['ref_from'] or ''} | CR Ref: {row['ref_to'] or ''} | Lag(d): {row['lag_days']}"
    )

# ---------------- Sidebar: Source Loader ----------------
st.sidebar.header("📦 Source")
mode = st.sidebar.radio("Choose source", ["Google Drive", "Local Folder"], index=0)
frames: List[pd.DataFrame] = []

if "_index" not in st.session_state:
    st.session_state._index = pd.DataFrame()

if mode == "Google Drive":
    st.sidebar.markdown("**Step 1** — Upload Service Account JSON")
    sa_file = st.sidebar.file_uploader("Service Account JSON", type=["json"])
    st.sidebar.markdown("**Step 2** — Paste Drive Folder ID")
    folder_input = st.sidebar.text_input("Folder ID", value="")
    if st.sidebar.button("Load from Drive"):
        if not sa_file or not folder_input.strip():
            st.error("Upload the JSON and paste Folder ID.")
        else:
            files = drive_list_and_download(sa_file.read(), folder_input.strip())
            for content, name in files:
                try:
                    raw = read_any_excel_or_csv(content, name)
                    norm = normalize_df(raw, name)
                    frames.append(norm.assign(source=name))
                except Exception as e:
                    st.warning(f"Failed to read {name}: {e}")
            if frames:
                st.session_state._index = pd.concat(frames, ignore_index=True)
                st.success(f"Loaded {len(frames)} files from Drive.")
else:
    folder_local = st.sidebar.text_input("Local folder path", value="")
    if st.sidebar.button("Load local"):
        if not folder_local or not os.path.isdir(folder_local):
            st.error("Folder not found")
        else:
            for name in os.listdir(folder_local):
                if not name.lower().endswith((".xlsx",".xls",".csv")) or name.startswith("~$"):
                    continue
                path = os.path.join(folder_local, name)
                try:
                    with open(path, "rb") as f:
                        content = f.read()
                    raw = read_any_excel_or_csv(content, name)
                    norm = normalize_df(raw, name)
                    frames.append(norm.assign(source=name))
                except Exception as e:
                    st.warning(f"Failed to read {name}: {e}")
            if frames:
                st.session_state._index = pd.concat(frames, ignore_index=True)
                st.success(f"Loaded {len(frames)} files from local.")

df_all = st.session_state._index

with st.expander("Preview (first 100 rows)", expanded=False):
    if not df_all.empty:
        st.dataframe(df_all.head(100), use_container_width=True)

# ---------------- Filters & Search ----------------
st.subheader("Search by Amount & Filters")

c1, c2, c3 = st.columns([2,1,1])
with c1:
    amt_str = st.text_input("Amount", value="1000000")
with c2:
    tol = st.number_input("Tolerance (SAR)", min_value=0.0, max_value=50.0, value=AMOUNT_TOLERANCE, step=0.05)
with c3:
    go = st.button("Search")

c4, c5 = st.columns(2)
with c4:
    date_from = st.date_input("From Date", value=None)
with c5:
    date_to = st.date_input("To Date", value=None)

# learn bank list from data
bank_list = sorted(set(df_all["bank"].dropna().astype(str))) if not df_all.empty else []
all_banks = ["All"] + sorted(set(KNOWN_BANKS + bank_list))

c6, c7 = st.columns(2)
with c6:
    from_bank = st.selectbox("From Bank (debit)", all_banks, index=0)
with c7:
    to_bank = st.selectbox("To Bank (credit)", all_banks, index=0)

def apply_filters(df: pd.DataFrame,
                  target: float,
                  tol: float,
                  date_from: date | None,
                  date_to: date | None) -> pd.DataFrame:
    x = df[np.abs(df["abs_amount"] - target) <= tol].copy()
    if date_from:
        x = x[x["date"] >= date_from]
    if date_to:
        x = x[x["date"] <= date_to]
    return x

if go:
    if df_all.empty:
        st.warning("No data loaded yet. Load from Drive or Local first.")
    else:
        target = parse_amount(amt_str)
        if target is None:
            st.error("Please enter a valid number, e.g., 1000000 or 1,000,000")
        else:
            hits = apply_filters(df_all, target, tol, date_from, date_to)

            st.markdown(f"**Results for SAR {target:,.2f} ± {tol}**")
            if hits.empty:
                st.info("No lines found with the selected amount and date filters.")
            else:
                st.dataframe(
                    hits.sort_values(["date","bank","direction"])[
                        ["date","bank","account","narration","ref","amount","balance","direction","source"]
                    ],
                    use_container_width=True
                )

            # Attempt debit↔credit pairing (respect bank hints if chosen)
            f_hint = from_bank if from_bank != "All" else None
            t_hint = to_bank   if to_bank   != "All" else None
            pairs = pair_debit_credit(hits, date_from, date_to, f_hint, t_hint)

            if not pairs.empty:
                st.subheader("Possible transfer pairs (same amount within ± days)")
                pairs["Confirmation"] = pairs.apply(confirmation_line, axis=1)
                st.dataframe(
                    pairs[["date_from","bank_from","acct_from","ref_from",
                           "date_to","bank_to","acct_to","ref_to",
                           "abs_amount","lag_days","Confirmation"]],
                    use_container_width=True
                )

                # Excel download
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine="openpyxl") as xw:
                    hits.to_excel(xw, index=False, sheet_name="All Hits")
                    pairs.to_excel(xw, index=False, sheet_name="Pairs")
                st.download_button(
                    "Download Excel (Hits + Pairs)",
                    data=out.getvalue(),
                    file_name=f"AmountSearch_{int(round(target))}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.caption("No debit↔credit pairs found. Counter-entry may post later or is in another date window.")
