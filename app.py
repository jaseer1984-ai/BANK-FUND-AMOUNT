# app.py — Bank Statements (Local)
# Header-detect + Normalization + Search/Pair + Raw row + Per-bank exports
from __future__ import annotations
import io, os, re, json
from typing import List, Dict, Tuple
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Bank Statements — Local", layout="wide")
st.title("🏦 Bank Statements (Local) — Header Model • Search/Pair • Export")

# -------------------- Config --------------------
DATE_WINDOW_DAYS = 3          # ± days for pairing
AMOUNT_TOLERANCE = 0.05       # SAR tolerance for matching

# Header aliases (EN + AR + common variants)
HEADER_MAP = {
    "date": [
        "value date","transaction date","trans: date","processing date","post date","posted","date",
        "Gregorian","Value date","Posted","Posted date","Posted Hijrah","Posted Hijrah",
        "تاريخ","تاريخ العملية","التاريخ","Gregorian date","Hijri","Gregorian"
    ],
    "narr": [
        "details","description","description details","transaction description","transaction details","remarks",
        "narration","narration 1","narration 2","narration 3","narrative","narrative 1","narrative 2","narrative 3",
        "Description Extra","Narrative #1","Narrative #2",
        # Arabic
        "البيان","الوصف","وصف","تفاصيل"
    ],
    "debit": [
        "debit","debit amount","amount dr.","withdrawal","dr","amount dr","debit (sar)","debit amount (sar)",
        "debitamount","debit amount",
        # Arabic
        "مدين","مديونية"
    ],
    "credit": [
        "credit","credit amount","amount cr.","deposit","cr","amount cr","credit (sar)","credit amount (sar)",
        "creditamount","credit amount",
        # Arabic
        "دائن","دائنية"
    ],
    "amount": [
        "amount","txn amount","transaction amount","amount (sar)","amount sar"
    ],
    "balance": [
        "balance","running balance","balance (sar)","running balance (sar)","running balance sar",
        "balance sar",
        # Arabic
        "الرصيد","رصيد"
    ],
    "account": [
        "account","account no","account number","iban","iban / account","bank reference","customer id",
        # Arabic
        "رقم الحساب","رقم العميل"
    ],
    "ref": [
        "reference","reference no","reference number","customer reference","txt id","trace","utr",
        "customer reference #","bank reference","reference #","reference #.","reference num",
        # Arabic
        "الرقم المرجعي"
    ],
    "bank": ["bank"],
    "drcr": ["dr/cr","d/c","dc","dr cr","dr or cr","type","transaction type","debit/credit","tran type","cr/dr","credit/debit","credit/debit/الرصيد"]
}

# Final normalized schema
MODEL_COLUMNS = ["date","bank","account","narration","ref","amount","abs_amount","balance","direction","source","_rawdata"]

# Known banks (SABB and BSF separate)
KNOWN_BANKS = ["SNB","SABB","BSF","ARB","ANB","RIB","SIB","NBK","BAB","INM"]

def detect_bank_from_name(name: str) -> str:
    n = name.lower()
    if "snb" in n or "ncb" in n: return "SNB"
    if "sabb" in n:              return "SABB"
    if "bsf" in n:               return "BSF"
    if "arb" in n or "rajhi" in n: return "ARB"
    if "anbod" in n or "anb od" in n: return "ANBOD"
    if "anb" in n:               return "ANB"
    if "rib" in n:               return "RIB"
    if "sib" in n:               return "SIB"
    if "nbk" in n and "call" in n: return "NBK CALL"
    if "nbk" in n:               return "NBK"
    if "bab" in n:               return "BAB"
    if "inm" in n:               return "INM"
    return os.path.splitext(name)[0].upper()

# -------------------- Robust readers with header detection --------------------
def _read_excel_any(path_or_bytes, is_bytes: bool, name: str) -> pd.DataFrame:
    if is_bytes:
        return pd.read_excel(io.BytesIO(path_or_bytes), header=None, dtype=str)
    return pd.read_excel(path_or_bytes, header=None, dtype=str)

def _read_csv_any(path_or_bytes, is_bytes: bool) -> pd.DataFrame:
    if is_bytes:
        try:    return pd.read_csv(io.BytesIO(path_or_bytes), header=None, dtype=str)
        except: return pd.read_csv(io.BytesIO(path_or_bytes), header=None, dtype=str, sep=";")
    else:
        try:    return pd.read_csv(path_or_bytes, header=None, dtype=str)
        except: return pd.read_csv(path_or_bytes, header=None, dtype=str, sep=";")

def _find_header_row(df0: pd.DataFrame) -> int:
    expect_words = [
        "date","value","transaction","debit","credit","balance","description","details",
        "reference","iban","account","dr","cr","type","narr",
        "تاريخ","الرصيد","وصف","البيان","الرقم","مرجعي"
    ]
    best_row, best_score = 0, -1
    max_scan = min(15, len(df0))
    for r in range(max_scan):
        row_vals = [str(x).strip().lower() for x in df0.iloc[r].fillna("").tolist()]
        score = sum(any(w in v for w in expect_words) for v in row_vals)
        if score > best_score:
            best_row, best_score = r, score
    return best_row

def _coerce_number(x):
    if pd.isna(x): return np.nan
    s = str(x).strip()
    if not s: return np.nan
    # Arabic comma and spaces
    s = s.replace("\u066c", ",").replace(" ", "")
    # Parentheses negatives
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    # remove thousand comma
    s = s.replace(",", "")
    if s in {"-", "--"}: return 0.0
    try:
        val = float(s)
        return -val if neg else val
    except Exception:
        return np.nan

def read_any_excel_or_csv_bytes(content: bytes, name: str) -> pd.DataFrame:
    if name.lower().endswith(".csv"):
        df0 = _read_csv_any(content, True)
    else:
        df0 = _read_excel_any(content, True, name)
    hdr = _find_header_row(df0)
    cols = df0.iloc[hdr].fillna("").astype(str).tolist()
    df = df0.iloc[hdr+1:].copy()
    df.columns = cols
    df = df.dropna(how="all", axis=1).dropna(how="all")
    return df

def read_any_excel_or_csv_path(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        df0 = _read_csv_any(path, False)
    else:
        df0 = _read_excel_any(path, False, path)
    hdr = _find_header_row(df0)
    cols = df0.iloc[hdr].fillna("").astype(str).tolist()
    df = df0.iloc[hdr+1:].copy()
    df.columns = cols
    df = df.dropna(how="all", axis=1).dropna(how="all")
    return df

# -------------------- Normalization (keeps RAW row JSON) --------------------
def normalize_df_with_raw_and_mapping(raw: pd.DataFrame, filename_hint: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    mapping: Dict[str, str] = {}

    def matched(key: str) -> str:
        cols_lower = [c.lower().strip() for c in raw.columns]
        for alias in HEADER_MAP[key]:
            a = alias.lower()
            if a in cols_lower:
                col_name = raw.columns[cols_lower.index(a)]
                mapping[key] = col_name
                return col_name
        mapping[key] = ""
        return ""

    raw_json_series = raw.apply(
        lambda r: json.dumps({str(k): (None if pd.isna(v) else str(v)) for k, v in r.items()}, ensure_ascii=False),
        axis=1
    )

    c_date = matched("date") or raw.columns[0]
    c_narr = matched("narr") or raw.columns[min(1, len(raw.columns)-1)]
    c_deb  = matched("debit")
    c_cred = matched("credit")
    c_amt  = matched("amount")
    c_bal  = matched("balance")
    c_acct = matched("account")
    c_ref  = matched("ref")
    c_bank = matched("bank")
    c_drcr = matched("drcr")

    df = pd.DataFrame(index=raw.index)
    dtry = pd.to_datetime(raw[c_date], errors="coerce", dayfirst=True)
    df["date"] = dtry.dt.date
    df["narration"] = raw[c_narr].astype(str).str.strip() if c_narr else ""
    df["balance"] = raw[c_bal].map(_coerce_number) if c_bal else pd.NA
    df["account"] = raw[c_acct].astype(str).str.strip() if c_acct else ""
    df["ref"] = raw[c_ref].astype(str).str.strip() if c_ref else ""
    df["bank"] = (raw[c_bank].astype(str).str.strip() if c_bank else detect_bank_from_name(filename_hint))
    df["_rawdata"] = raw_json_series

    debit  = raw[c_deb].map(_coerce_number) if c_deb else None
    credit = raw[c_cred].map(_coerce_number) if c_cred else None
    amount = raw[c_amt].map(_coerce_number) if c_amt else None

    if c_amt and not c_deb and not c_cred and c_drcr:
        drcr = raw[c_drcr].astype(str).str.strip().str.lower()
        signed = amount.abs()
        signed[drcr.isin(["dr","d","debit","مدين"])] *= -1
        signed[drcr.isin(["cr","c","credit","دائن"])] *= +1
        df["amount"] = signed
    elif c_amt and not c_deb and not c_cred:
        df["amount"] = amount
    else:
        df["amount"] = (credit.fillna(0) if credit is not None else 0) - (debit.fillna(0) if debit is not None else 0)

    df = df.dropna(subset=["date"]).copy()
    df = df[~df["amount"].isna()].copy()
    df["amount"] = df["amount"].astype(float)
    df["abs_amount"] = df["amount"].abs().round(2)
    df["direction"] = np.where(df["amount"] < 0, "FROM (OUT)", "TO (IN)")
    df["ref"] = df["ref"].str.replace("\n"," ").str.replace("\r"," ")
    df["narration"] = df["narration"].str.replace("\n"," ").str.replace("\r"," ")

    wanted = ["date","bank","account","narration","ref","amount","abs_amount","balance","direction","_rawdata"]
    for c in wanted:
        if c not in df.columns: df[c] = pd.NA
    return df[wanted], mapping

# -------------------- Pairing helpers --------------------
def parse_amount(text: str) -> float | None:
    s = text.strip().replace("\u066c", ",").replace(",", "").replace(" ", "")
    if not s: return None
    try: return float(s)
    except Exception:
        m = re.search(r"([0-9]+)\.?([0-9]{0,2})", s)
        if not m: return None
        num, dec = m.group(1), m.group(2)
        return float(f"{num}.{dec}" if dec else num)

def pair_debit_credit(outs: pd.DataFrame, ins: pd.DataFrame) -> pd.DataFrame:
    if outs.empty or ins.empty: return pd.DataFrame()
    matches, used_in = [], set()
    for _, o in outs.iterrows():
        cand = ins[(np.abs(ins["abs_amount"] - o["abs_amount"]) <= AMOUNT_TOLERANCE)]
        cand = cand[(pd.to_datetime(cand["date"]) >= pd.to_datetime(o["date"]) - pd.Timedelta(days=DATE_WINDOW_DAYS)) &
                    (pd.to_datetime(cand["date"]) <= pd.to_datetime(o["date"]) + pd.Timedelta(days=DATE_WINDOW_DAYS))]
        cand = cand[~cand.index.isin(used_in)]
        if cand.empty: continue
        cand = cand.assign(score=(pd.to_datetime(cand["date"]) - pd.to_datetime(o["date"])).abs().dt.days)
        m = cand.sort_values("score").iloc[0]
        used_in.add(m.name)
        matches.append({
            "date_from": o["date"], "bank_from": o["bank"], "acct_from": o["account"],
            "ref_from":  o["ref"],  "narr_from": o["narration"], "amt_from": o["amount"],
            "date_to":   m["date"], "bank_to":   m["bank"], "acct_to":   m["account"],
            "ref_to":    m["ref"],  "narr_to":   m["narration"], "amt_to":   m["amount"],
            "abs_amount": o["abs_amount"], "lag_days": int(abs(pd.to_datetime(m["date"]) - pd.to_datetime(o["date"])).days),
            "raw_from": o.get("_rawdata",""), "raw_to": m.get("_rawdata","")
        })
    return pd.DataFrame(matches)

def confirmation_line(row: pd.Series) -> str:
    return (f"DONE ✅ | {row['bank_from']}→{row['bank_to']} | SAR {row['abs_amount']:,.2f} "
            f"| DR Ref: {row['ref_from'] or ''} | CR Ref: {row['ref_to'] or ''} | Lag(d): {row['lag_days']}")

# -------------------- Sidebar: source loading --------------------
st.sidebar.header("📦 Source")
mode = st.sidebar.radio("Choose input method", ["Browse & Upload files", "Local Folder path"], index=0)

if "_index" not in st.session_state:     st.session_state._index = pd.DataFrame()
if "_headers" not in st.session_state:   st.session_state._headers: Dict[str, List[str]] = {}
if "_mappings" not in st.session_state:  st.session_state._mappings: Dict[str, Dict[str,str]] = {}

frames: List[pd.DataFrame] = []
loaded_names: List[str] = []

if mode == "Browse & Upload files":
    uploads = st.sidebar.file_uploader(
        "Upload statements (.xlsx, .xls, .csv)",
        type=["xlsx","xls","csv"],
        accept_multiple_files=True
    )
    if st.sidebar.button("Load uploaded files"):
        if not uploads:
            st.error("Please upload at least one file.")
        else:
            for up in uploads:
                try:
                    raw = read_any_excel_or_csv_bytes(up.read(), up.name)
                    st.session_state._headers[up.name] = list(raw.columns)
                    norm, mapping = normalize_df_with_raw_and_mapping(raw, up.name)
                    norm["source"] = up.name
                    frames.append(norm)
                    st.session_state._mappings[up.name] = mapping
                    loaded_names.append(up.name)
                except Exception as e:
                    st.warning(f"Failed to read {up.name}: {e}")
            if frames:
                st.session_state._index = pd.concat(frames, ignore_index=True)
                st.success(f"Loaded {len(frames)} files: {', '.join(loaded_names[:6])}{' …' if len(loaded_names)>6 else ''}")

else:
    folder_local = st.sidebar.text_input("Local folder path (e.g., D:/BANK SOA)", value="")
    if st.sidebar.button("Load local folder"):
        if not folder_local or not os.path.isdir(folder_local):
            st.error("Folder not found. Paste a valid local path.")
        else:
            for name in os.listdir(folder_local):
                if name.startswith("~$"): continue
                if not name.lower().endswith((".xlsx",".xls",".csv")): continue
                path = os.path.join(folder_local, name)
                try:
                    raw = read_any_excel_or_csv_path(path)
                    st.session_state._headers[name] = list(raw.columns)
                    norm, mapping = normalize_df_with_raw_and_mapping(raw, name)
                    norm["source"] = name
                    frames.append(norm)
                    st.session_state._mappings[name] = mapping
                    loaded_names.append(name)
                except Exception as e:
                    st.warning(f"Failed to read {name}: {e}")
            if frames:
                st.session_state._index = pd.concat(frames, ignore_index=True)
                st.success(f"Loaded {len(frames)} files: {', '.join(loaded_names[:6])}{' …' if len(loaded_names)>6 else ''}")

df_all = st.session_state._index

with st.expander("Preview normalized data (first 100 rows)", expanded=False):
    if not df_all.empty:
        st.dataframe(df_all.head(100), use_container_width=True)

# -------------------- Header Model tools + Export --------------------
st.subheader("🧱 Header Model / Mappings")

colh1, colh2 = st.columns(2)
with colh1:
    if st.button("Show per-file original headers"):
        if not st.session_state._headers:
            st.info("No files loaded yet.")
        else:
            rows = [{"File": fn, "Original Headers": ", ".join(cols)} for fn, cols in st.session_state._headers.items()]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

with colh2:
    if st.button("Show detected mappings (→ normalized model)"):
        if not st.session_state._mappings:
            st.info("No files loaded yet.")
        else:
            records = []
            rename = {"date":"date","narr":"narration","debit":"debit","credit":"credit",
                      "amount":"amount","balance":"balance","account":"account","ref":"ref",
                      "bank":"bank","drcr":"drcr"}
            for fn, mp in st.session_state._mappings.items():
                for k, v in mp.items():
                    records.append({"File": fn, "Normalized Field": rename.get(k,k), "Matched Source Column": v})
            st.dataframe(pd.DataFrame(records), use_container_width=True)

if st.button("📤 Export Header Model (Excel)"):
    if df_all.empty or not st.session_state._headers:
        st.warning("Load files first, then export.")
    else:
        original_headers = pd.DataFrame(
            [{"File": fn, "Original Headers": ", ".join(cols)} for fn, cols in st.session_state._headers.items()]
        )
        detected_mappings = []
        rename = {"date":"date","narr":"narration","debit":"debit","credit":"credit",
                  "amount":"amount","balance":"balance","account":"account","ref":"ref",
                  "bank":"bank","drcr":"drcr"}
        for fn, mp in st.session_state._mappings.items():
            for k, v in mp.items():
                detected_mappings.append({"File": fn, "Normalized Field": rename.get(k,k), "Matched Source Column": v})
        detected_mappings = pd.DataFrame(detected_mappings)

        model_template = pd.DataFrame({
            "Normalized Field": MODEL_COLUMNS,
            "Type / Notes": [
                "date (YYYY-MM-DD)",
                "bank short code (SNB, SABB, BSF, ARB, ANB, RIB, SIB, NBK, INM, BAB, ANBOD, NBK CALL)",
                "account/IBAN","transaction text","reference/trace/UTR",
                "signed number (+credit, -debit)","absolute amount (for pairing)","running balance",
                "FROM (OUT) or TO (IN)","source file name","JSON of the original row"
            ]
        })

        outbuf = io.BytesIO()
        with pd.ExcelWriter(outbuf, engine="openpyxl") as xw:
            original_headers.to_excel(xw, index=False, sheet_name="OriginalHeaders")
            detected_mappings.to_excel(xw, index=False, sheet_name="DetectedMapping")
            df_all.to_excel(xw, index=False, sheet_name="UnifiedNormalizedData")
            model_template.to_excel(xw, index=False, sheet_name="ModelTemplate")
        st.download_button(
            "Download Header Model.xlsx",
            data=outbuf.getvalue(),
            file_name="HeaderModel_and_NormalizedData.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.success("Header Model Excel is ready.")

# -------------------- Per-bank ORIGINAL headers export --------------------
def _bank_label_from_source_and_bank(source: str, bank: str) -> str:
    s = (source or "").lower()
    b = (bank or "").upper()
    if "call" in s and b == "NBK":     return "NBK CALL"
    if "anbod" in s or "anb od" in s.replace("_"," ").replace("-"," "): return "ANBOD"
    return b

def _raw_records_from_df(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, r in df.iterrows():
        try:
            raw = json.loads(r.get("_rawdata","{}") or "{}")
        except Exception:
            raw = {}
        raw["_bank"] = r.get("bank")
        raw["_source"] = r.get("source")
        raw["_date"] = str(r.get("date"))
        raw["_bank_label"] = _bank_label_from_source_and_bank(raw.get("_source",""), raw.get("_bank",""))
        records.append(raw)
    return pd.DataFrame(records) if records else pd.DataFrame()

if st.button("📦 Export ALL (per bank original headers)"):
    if df_all.empty:
        st.warning("Load files first.")
    else:
        raw_all = _raw_records_from_df(df_all)
        if raw_all.empty:
            st.warning("No raw rows available to export.")
        else:
            wanted_order = ["SNB","SABB","ARB","RIB","BSF","ANB","ANBOD","SIB","NBK","NBK CALL","INM","BAB"]
            present = [x for x in raw_all["_bank_label"].dropna().unique().tolist() if x]
            for x in present:
                if x not in wanted_order: wanted_order.append(x)

            outbuf_all = io.BytesIO()
            with pd.ExcelWriter(outbuf_all, engine="openpyxl") as xw:
                for label in wanted_order:
                    sub = raw_all[raw_all["_bank_label"] == label]
                    if sub.empty: continue
                    front = ["_date","_source"]
                    others = [c for c in sub.columns if c not in {"_date","_source","_bank","_bank_label"}]
                    sub = sub[front + others]
                    sheet_name = (label or "UNKNOWN")[:31]
                    sub.to_excel(xw, index=False, sheet_name=sheet_name)
                df_all.to_excel(xw, index=False, sheet_name="UnifiedNormalizedData")
            st.download_button(
                "Download Per-Bank Original Headers.xlsx",
                data=outbuf_all.getvalue(),
                file_name="PerBank_OriginalHeaders_and_Normalized.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("Per-bank Excel (with original headers) is ready.")

# -------------------- Search & Pair --------------------
st.subheader("🔎 Search by Amount & Pair")

c1, c2, c3 = st.columns([2,1,1])
with c1: amt_str = st.text_input("Amount", value="1000000")
with c2: tol = st.number_input("Tolerance (SAR)", min_value=0.0, max_value=50.0, value=AMOUNT_TOLERANCE, step=0.05)
with c3: go = st.button("Search & Pair")

c4, c5 = st.columns(2)
with c4: date_from = st.date_input("From Date", value=None)
with c5: date_to   = st.date_input("To Date",   value=None)

bank_list = sorted(set(df_all["bank"].dropna().astype(str))) if not df_all.empty else []
all_banks = ["All"] + sorted(set(KNOWN_BANKS + bank_list))

c6, c7 = st.columns(2)
with c6: from_bank = st.selectbox("From Bank (debit)", all_banks, index=0)
with c7: to_bank   = st.selectbox("To Bank (credit)", all_banks, index=0)

c8, c9 = st.columns(2)
with c8:
    from_debits_positive = st.checkbox("From bank debits are positive?", value=False,
                                       help="Enable if From-bank file shows debits as positive numbers.")
with c9:
    to_credits_negative = st.checkbox("To bank credits are negative?", value=False,
                                      help="Enable if To-bank file shows credits as negative numbers.")

def filter_base(df: pd.DataFrame, target: float, tol: float,
                date_from: date | None, date_to: date | None) -> pd.DataFrame:
    x = df[np.abs(df["abs_amount"] - target) <= tol].copy()
    if date_from: x = x[x["date"] >= date_from]
    if date_to:   x = x[x["date"] <= date_to]
    return x

if go:
    if df_all.empty:
        st.warning("No data loaded yet. Load files first.")
    else:
        target = parse_amount(amt_str)
        if target is None:
            st.error("Please enter a valid number, e.g., 1000000 or 1,000,000")
        else:
            base = filter_base(df_all, target, tol, date_from, date_to)

            outs = base[base["amount"] < 0].copy()
            if from_bank != "All": outs = outs[outs["bank"].str.upper() == from_bank.upper()]
            if from_debits_positive and from_bank != "All":
                extra = base[(base["bank"].str.upper() == from_bank.upper()) & (base["amount"] > 0)].copy()
                extra["amount"] = -extra["amount"].abs()
                extra["abs_amount"] = extra["amount"].abs().round(2)
                extra["direction"] = "FROM (OUT) [assumed]"
                outs = pd.concat([outs, extra], ignore_index=True)

            ins = base[base["amount"] > 0].copy()
            if to_bank != "All": ins = ins[ins["bank"].str.upper() == to_bank.upper()]
            if to_credits_negative and to_bank != "All":
                extra_in = base[(base["bank"].str.upper() == to_bank.upper()) & (base["amount"] < 0)].copy()
                extra_in["amount"] = extra_in["amount"].abs()
                extra_in["abs_amount"] = extra_in["amount"].abs().round(2)
                extra_in["direction"] = "TO (IN) [assumed]"
                ins = pd.concat([ins, extra_in], ignore_index=True)

            st.markdown(f"**Filtered rows for SAR {target:,.2f} ± {tol}**")
            colA, colB = st.columns(2)
            with colA:
                st.markdown("**FROM candidates (debit/OUT)**")
                st.dataframe(
                    outs.sort_values(["date","bank"])[
                        ["date","bank","account","narration","ref","amount","balance","direction","source"]
                    ],
                    use_container_width=True
                )
            with colB:
                st.markdown("**TO candidates (credit/IN)**")
                st.dataframe(
                    ins.sort_values(["date","bank"])[
                        ["date","bank","account","narration","ref","amount","balance","direction","source"]
                    ],
                    use_container_width=True
                )

            pairs = pair_debit_credit(outs, ins)
            if not pairs.empty:
                st.subheader("Matched transfer pairs")
                pairs["Confirmation"] = pairs.apply(confirmation_line, axis=1)
                st.dataframe(
                    pairs[["date_from","bank_from","acct_from","ref_from",
                           "date_to","bank_to","acct_to","ref_to",
                           "abs_amount","lag_days","Confirmation"]],
                    use_container_width=True
                )
                for i, r in pairs.iterrows():
                    with st.expander(f"🔎 Pair {i+1}: {r['bank_from']} → {r['bank_to']} | SAR {r['abs_amount']:,.2f}"):
                        st.markdown("**FROM raw row (complete):**")
                        try: st.json(json.loads(r["raw_from"]))
                        except Exception: st.text(r["raw_from"])
                        st.markdown("**TO raw row (complete):**")
                        try: st.json(json.loads(r["raw_to"]))
                        except Exception: st.text(r["raw_to"])

                outbuf = io.BytesIO()
                with pd.ExcelWriter(outbuf, engine="openpyxl") as xw:
                    outs.to_excel(xw, index=False, sheet_name="FROM_candidates")
                    ins.to_excel(xw, index=False, sheet_name="TO_candidates")
                    pairs.to_excel(xw, index=False, sheet_name="Pairs_with_RawRows")
                st.download_button(
                    "Download Excel (FROM, TO, Pairs + Raw Rows)",
                    data=outbuf.getvalue(),
                    file_name=f"Amount_{int(round(target))}_from_to_pairs_raw.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.caption("No debit↔credit pairs found. Try toggling sign fixes, widening dates, or removing bank filters.")
