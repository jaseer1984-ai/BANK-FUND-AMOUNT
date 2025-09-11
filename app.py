# app.py — Find-by Amount & Date (no BANK column)
# Input Excel: AMOUNT | DATE
# Output: one Excel with a separate sheet per input row containing all matches (date >= input date AND amount match).
# Works with: streamlit==1.28.0, pandas==2.0.3, numpy==1.24.3, openpyxl==3.1.2, xlrd==2.0.1

import io
import re
import zipfile
import hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Amount + Date Finder", page_icon="🔎", layout="wide")

# ----------------------- heuristics & aliases -----------------------

MONEY_TOKENS = [
    "amount", "credit", "debit", "value", "sar", "balance",
    "رصيد", "مبلغ", "مدين", "دائن", "قيمة", "ائتمان"
]
DATE_TOKENS = [
    "date", "value date", "posting", "transaction", "txn", "val",
    "تاريخ", "تاريخ العملية", "تاريخ المعاملة", "قيمة التاريخ"
]

CURRENCY_RE = re.compile(r"[^\d\-\.\(\)]", re.UNICODE)

def _clean_amount(x) -> Optional[float]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    if s == "":
        return None
    s = CURRENCY_RE.sub("", s.replace(",", "").replace(" ", ""))
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        v = float(s)
        if neg:
            v = -v
        return round(v, 2)
    except Exception:
        return None

def _parse_date(v) -> Optional[pd.Timestamp]:
    if pd.isna(v) or str(v).strip() == "":
        return None
    return pd.to_datetime(v, errors="coerce")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _read_all_sheets(file_like: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    ext = filename.lower().rsplit(".", 1)[-1]
    engine = "openpyxl" if ext == "xlsx" else ("xlrd" if ext == "xls" else None)
    xls = pd.ExcelFile(io.BytesIO(file_like), engine=engine)
    return {s: _norm_cols(xls.parse(s, dtype=object)) for s in xls.sheet_names}

def _extract_files_from_zip(zip_bytes: bytes) -> List[Tuple[str, bytes]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for zi in zf.infolist():
            name = zi.filename
            if name.endswith("/") or name.startswith("__MACOSX"):
                continue
            if name.lower().endswith((".xlsx", ".xls")):
                out.append((name.split("/")[-1], zf.read(zi)))
    return out

def _bytes_from_uploader(uploaded_files) -> List[Tuple[str, bytes]]:
    collected = []
    for uf in uploaded_files or []:
        name = uf.name
        data = uf.read()
        if name.lower().endswith(".zip"):
            collected.extend(_extract_files_from_zip(data))
        else:
            collected.append((name, data))
    collected = [(n, b) for (n, b) in collected if n.lower().endswith((".xlsx", ".xls"))]
    # de-dup by filename+hash
    seen = set()
    uniq = []
    for (n, b) in collected:
        key = (n.lower(), hashlib.md5(b).hexdigest())
        if key in seen:
            continue
        seen.add(key)
        uniq.append((n, b))
    return uniq

def _first_present(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        k = cand.strip().lower()
        if k in lower:
            return lower[k]
    return None

def _find_col_regex(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    for col in df.columns:
        lc = str(col).lower()
        for pat in patterns:
            if re.search(pat, lc):
                return col
    return None

def _guess_date_column(df: pd.DataFrame) -> Optional[str]:
    best_col, best_score = None, -1.0
    threshold_abs, threshold_frac = 15, 0.30
    for c in df.columns:
        ser = pd.to_datetime(df[c], errors="coerce")
        valid = ser.notna().sum()
        if valid == 0:
            continue
        yrs = ser.dt.year.dropna()
        within = ((yrs >= 2000) & (yrs <= 2035)).mean() if len(yrs) else 0
        name_bonus = 1.0 if any(t in str(c).lower() for t in DATE_TOKENS) else 0.0
        score = valid * (0.6 + 0.4 * within) + name_bonus
        if valid >= max(threshold_abs, threshold_frac * len(df)) and score > best_score:
            best_col, best_score = c, score
    return best_col

def _guess_amount_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    name_scores = {
        c: sum(tok in str(c).lower() for tok in MONEY_TOKENS +
               ["credit amount", "debit amount", "amount sar", "balance (sar)", "credit (sar)", "debit (sar)"])
        for c in df.columns
    }
    best_col, best_score = None, -1.0
    threshold_abs, threshold_frac = 20, 0.40
    for c in df.columns:
        cleaned = df[c].apply(_clean_amount)
        valid = cleaned.notna().sum()
        if valid >= max(threshold_abs, threshold_frac * len(df)):
            score = name_scores.get(c, 0) * 1.0 + valid * 0.01
            if score > best_score:
                best_col, best_score = c, score
    return best_col

def _normalize_ledger(df: pd.DataFrame, amount_cands: List[str], date_cands: List[str],
                      enable_debit_credit=True, auto_date=True):
    """
    Returns df with normalized columns:
      _DATE_   : parsed datetime
      _SIGNED_ : signed amount (credit - debit if pair exists, else the single detected amount column)
      _CREDIT_, _DEBIT_ : explicit columns when available
    Also returns an info dict describing which columns were used.
    """
    info = {"amt_col": None, "date_col": None, "credit_col": None, "debit_col": None,
            "net_logic": False, "amt_auto": False, "date_auto": False}
    if df.empty:
        df["_SIGNED_"] = np.nan; df["_DATE_"] = pd.NaT; df["_CREDIT_"] = np.nan; df["_DEBIT_"] = np.nan
        return df, info

    amt_col = _first_present(df, amount_cands)

    # BSF patterns
    credit_sar = _find_col_regex(df, [r"\bcredit\s*\(sar\)\b", r"\bcredit\s*\(s\.?a\.?r\.?\)\b"])
    debit_sar  = _find_col_regex(df,  [r"\bdebit\s*\(sar\)\b",  r"\bdebit\s*\(s\.?a\.?r\.?\)\b"])

    credit_col = debit_col = None
    if enable_debit_credit:
        if credit_sar and debit_sar:
            credit_col, debit_col = credit_sar, debit_sar
        else:
            debit_col  = _first_present(df, ["debit","dr","debit amount","مدين"])
            credit_col = _first_present(df, ["credit","cr","credit amount","دائن"])
            if debit_col is None:
                for c in df.columns:
                    cl = str(c).lower()
                    if "debit" in cl or "مدين" in cl:
                        debit_col = c; break
            if credit_col is None:
                for c in df.columns:
                    cl = str(c).lower()
                    if "credit" in cl or "دائن" in cl:
                        credit_col = c; break

    if credit_col and enable_debit_credit:
        df[credit_col] = df[credit_col].apply(_clean_amount)
    if debit_col and enable_debit_credit:
        df[debit_col] = df[debit_col].apply(_clean_amount)

    if enable_debit_credit and credit_col and debit_col:
        info["credit_col"], info["debit_col"] = credit_col, debit_col
        info["net_logic"] = True
        signed = (df[credit_col].fillna(0) - df[debit_col].fillna(0)).round(2)
    else:
        if amt_col is None:
            for c in df.columns:
                cl = str(c).lower()
                if any(tok in cl for tok in ["amount","credit amount","debit amount","balance","balance (sar)","المبلغ","الرصيد"]):
                    amt_col = c; break
        if amt_col is None:
            guess_amt = _guess_amount_column(df)
            if guess_amt:
                amt_col = guess_amt; info["amt_auto"] = True
        if amt_col:
            df[amt_col] = df[amt_col].apply(_clean_amount); info["amt_col"] = amt_col
            signed = df[amt_col]
        else:
            signed = pd.Series([np.nan] * len(df))

    date_col = _first_present(df, date_cands)
    if date_col:
        df[date_col] = df[date_col].apply(_parse_date); info["date_col"] = date_col
    elif auto_date:
        guess = _guess_date_column(df)
        if guess:
            df[guess] = pd.to_datetime(df[guess], errors="coerce"); info["date_col"] = guess; info["date_auto"] = True

    df["_SIGNED_"] = signed
    df["_CREDIT_"] = df[info["credit_col"]] if info["credit_col"] in df.columns else np.nan
    df["_DEBIT_"]  = df[info["debit_col"]]  if info["debit_col"]  in df.columns else np.nan
    df["_DATE_"]   = df[info["date_col"]]   if info["date_col"]   in df.columns else pd.NaT
    return df, info

# ----------------------- UI -----------------------

st.markdown("<h2 style='margin:0'>Amount + Date Finder</h2>", unsafe_allow_html=True)

c0a, c0b = st.columns([1,1])
with c0a:
    input_file = st.file_uploader("Input (AMOUNT, DATE)", type=["xlsx","xls"])
with c0b:
    stmt_files = st.file_uploader("Statements (xlsx/xls or ZIP)", type=["xlsx","xls","zip"], accept_multiple_files=True)

st.divider()

c1,c2,c3 = st.columns([1,1,1])
with c1:
    amount_candidates_text = st.text_input(
        "Amount column candidates",
        value="Amount, Credit, Credit Amount, Credit (SAR), CR, Debit, Debit Amount, Debit (SAR), DR, Value, Value Amount, Balance, Balance (SAR), الرصيد, المبلغ, مدين, دائن"
    )
with c2:
    date_candidates_text = st.text_input(
        "Date column candidates",
        value="Date, Transaction Date, Value Date, Posting Date, تاريخ, تاريخ العملية, تاريخ المعاملة"
    )
with c3:
    exact_amount = st.checkbox("Exact amount (2 decimals)", value=True)

c4,c5 = st.columns([1,1])
with c4:
    use_abs = st.checkbox("Match by absolute amount", value=False)  # handles sign differences
with c5:
    auto_detect_date = st.checkbox("Auto-detect Date column if not found", value=True)

run_btn = st.button("🔎 Find", type="primary", use_container_width=True)

# ----------------------- Main -----------------------

if run_btn:
    if not input_file:
        st.error("Upload the Input Excel.")
        st.stop()
    if not stmt_files:
        st.error("Upload statements (or a ZIP).")
        st.stop()

    # Read input
    try:
        df_in = pd.read_excel(input_file, dtype=object)
        df_in = _norm_cols(df_in)
    except Exception as e:
        st.error(f"Failed to read Input: {e}")
        st.stop()

    m = {c.lower(): c for c in df_in.columns}
    c_amt_in = m.get("amount"); c_date_in = m.get("date")
    if not (c_amt_in and c_date_in):
        st.error("Input must have columns: AMOUNT and DATE.")
        st.stop()

    df_in["_AMOUNT"] = df_in[c_amt_in].apply(_clean_amount)
    df_in["_DATE"]   = df_in[c_date_in].apply(_parse_date)
    before = len(df_in)
    df_in = df_in.dropna(subset=["_AMOUNT","_DATE"])
    if len(df_in) < before:
        st.warning(f"Skipped {before - len(df_in):,} invalid row(s).")
    if df_in.empty:
        st.error("No valid rows after cleaning.")
        st.stop()

    files = _bytes_from_uploader(stmt_files)
    with st.expander("Uploaded files", expanded=False):
        st.write([n for n,_ in files] or "No Excel files uploaded.")
    if not files:
        st.error("No readable Excel files found.")
        st.stop()

    amt_cands  = [s.strip() for s in amount_candidates_text.split(",") if s.strip()]
    date_cands = [s.strip() for s in date_candidates_text.split(",") if s.strip()]

    # Read & normalize all sheets
    repo: Dict[str, Dict[str, pd.DataFrame]] = {}
    diag_rows = []

    with st.status("Reading & normalizing statements…", expanded=False) as st_read:
        for (fname, fbytes) in files:
            try:
                sheets = _read_all_sheets(fbytes, fname)
            except Exception as e:
                st.warning(f"Skipped {fname}: {e}")
                continue
            repo[fname] = {}
            for sname, df in sheets.items():
                df_norm, info = _normalize_ledger(
                    df.copy(), amt_cands, date_cands,
                    enable_debit_credit=True, auto_date=auto_detect_date
                )
                repo[fname][sname] = df_norm
                n_amt = df_norm["_SIGNED_"].notna().sum()
                n_dt  = df_norm["_DATE_"].notna().sum()
                diag_rows.append({
                    "File": fname, "Sheet": sname,
                    "Amount Col": (info["amt_col"] + " (auto)" if info.get("amt_auto") else info.get("amt_col")) or info.get("credit_col") or info.get("debit_col"),
                    "Date Col": (info["date_col"] + " (auto)" if info.get("date_auto") else info.get("date_col")),
                    "Using Credit/Debit Pair?": "Yes" if info.get("net_logic") else "No",
                    "Parsed Amount Rows": int(n_amt),
                    "Parsed Date Rows": int(n_dt),
                })
        st_read.update(label="Finished.", state="complete")

    with st.expander("Diagnostics", expanded=False):
        if diag_rows:
            st.dataframe(pd.DataFrame(diag_rows), use_container_width=True)
        else:
            st.info("No sheets parsed.")

    tol = 0.0 if exact_amount else 0.01
    per_input_matches: Dict[int, pd.DataFrame] = {}
    summary_rows = []

    with st.status("Searching…", expanded=False) as st_find:
        for idx, row in df_in.iterrows():
            amt_in = float(abs(row["_AMOUNT"]) if use_abs else row["_AMOUNT"])
            d0 = row["_DATE"]

            found_parts = []
            for fname in repo:
                for sname, d in repo[fname].items():
                    if d.empty or d["_DATE_"].isna().all():
                        continue
                    series_amt = d["_SIGNED_"]
                    if use_abs:
                        series_amt = series_amt.abs()
                    mask_amt  = (series_amt == amt_in) if tol == 0.0 else (series_amt - amt_in).abs() <= tol
                    mask_date = d["_DATE_"] >= d0
                    mask = mask_amt & mask_date
                    if mask.any():
                        m = d.loc[mask].copy()
                        m.insert(0, "Source File", fname)
                        m.insert(1, "Sheet", sname)
                        m.insert(2, "AMOUNT (Input)", amt_in)
                        m.insert(3, "DATE From (Input)", pd.to_datetime(d0).date())
                        found_parts.append(m)

            if found_parts:
                per_input_matches[idx] = pd.concat(found_parts, ignore_index=True)
                total = len(per_input_matches[idx])
            else:
                per_input_matches[idx] = pd.DataFrame()
                total = 0

            summary_rows.append({
                "Input Row": int(idx)+1,
                "AMOUNT": amt_in,
                "DATE From": pd.to_datetime(d0).date(),
                "Matches": total
            })
        st_find.update(label="Done.", state="complete")

    st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

    # If nothing found at all
    if all(df.empty for df in per_input_matches.values()):
        st.warning("No matches found for any input rows.")
        st.stop()

    # Export: one sheet per input row
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for idx, dfm in per_input_matches.items():
            sheet_title = f"Row{idx+1}_Amt_{str(df_in.loc[idx, '_AMOUNT']).replace('.', '_')}"
            sheet_title = sheet_title[:31].translate(str.maketrans({":":"-","/":"-","\\":"-","*":"-","?":"-","[":"(","]":")"}))
            if dfm.empty:
                # write a small note sheet so you still see the row
                pd.DataFrame([{"Note":"No matches"}]).to_excel(writer, index=False, sheet_name=sheet_title)
            else:
                dfm.drop_duplicates().to_excel(writer, index=False, sheet_name=sheet_title)
    out.seek(0)

    st.download_button(
        "⬇️ Download Matches (Excel: one sheet per input row)",
        data=out.getvalue(),
        file_name=f"AmountDate_Matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
