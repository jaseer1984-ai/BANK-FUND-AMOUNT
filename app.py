# app.py — Amount+Date Finder (by BANK sheets; SIB/SAIB support; anti-Hijri bias)
# Input Excel: AMOUNT | DATE
# Output: Excel with one sheet per detected bank + optional All_Matches overview.
# Requirements: streamlit==1.28.0, pandas==2.0.3, numpy==1.24.3, openpyxl==3.1.2, xlrd==2.0.1

import io, re, zipfile, hashlib
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Amount + Date Finder", page_icon="🔎", layout="wide")

# ------------ Bank aliases (now includes SIB/SAIB) ------------
BANK_ALIASES: Dict[str, List[str]] = {
    "SNB":  ["SNB","NCB","Saudi National","Saudi National Bank","National Commercial","ALAHLI","AL AHLI","AHLI","AL-AHLI","الاهلي","الأهلي"],
    "SABB": ["SABB","AWWAL","HSBC","الأول","ساب"],
    "ARB":  ["ARB","AL RAJHI","ALRAJHI","RAJHI","AL-RAJHI","الراجحي","مصرف الراجحي"],
    "BSF":  ["BSF","SAUDI FRANSI","FRANSI","البنك السعودي الفرنسي","فرنسي"],
    "RIB":  ["RIB","RIYAD","RIYAD BANK","RIYADH BANK","RIYADBANK","بنك الرياض","الرياض"],
    "INMA": ["INMA","ALINMA","AL INMA","AL-INMA","INMAA","مصرف الإنماء","الإنماء","الانماء"],
    "ANB":  ["ANB","Arab National Bank","العربي","البنك العربي الوطني"],
    "SIB":  ["SIB","SAIB","Saudi Investment Bank","The Saudi Investment Bank","البنك السعودي للاستثمار","السعودي للاستثمار"],
}

def _detect_bank(filename: str, sheetname: str, df: pd.DataFrame) -> str:
    """Detect bank from file name, sheet name, or top area of the sheet."""
    def _hit(s: str) -> Optional[str]:
        s = (s or "").lower()
        for code, aliases in BANK_ALIASES.items():
            for a in aliases:
                if a.lower() in s:
                    return code
        return None
    # filename or sheet name
    for s in (filename, sheetname):
        code = _hit(s)
        if code: return code
    # headers
    code = _hit(" ".join(map(str, df.columns)))
    if code: return code
    # top preview cells
    with pd.option_context("display.max_colwidth", None):
        preview = " ".join(df.head(15).astype(str).fillna("").values.ravel().tolist())
    code = _hit(preview)
    return code or "OTHER"

# ------------ Parsing helpers ------------
MONEY_TOKENS = ["amount","credit","debit","value","sar","balance","رصيد","مبلغ","مدين","دائن","قيمة","ائتمان"]
DATE_TOKENS  = ["date","value date","posting","transaction","txn","val","تاريخ","تاريخ العملية","تاريخ المعاملة"]

CURRENCY_RE = re.compile(r"[^\d\.\-]", re.UNICODE)
ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩٬٫", "0123456789,.")

def _clean_amount(x) -> Optional[float]:
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip()
    if s == "": return None
    s = s.translate(ARABIC_DIGITS)
    s = s.replace("\u200f","").replace("\u200e","").replace("\u202a","").replace("\u202b","").replace("\u202c","")
    s = s.replace("SAR","").replace("ر.س","").replace("ريال","").replace("﷼","").replace(" ","")
    neg = False
    if s.startswith("(") and s.endswith(")"): neg, s = True, s[1:-1]
    if len(s)>1 and s[-1] in "+-":
        if s[-1]=="-": neg = not neg if s.startswith("-") else True
        s = s[:-1]
    s = s.replace(",", "")
    s = CURRENCY_RE.sub("", s)
    if s.count("-")>1: s = "-" + s.replace("-","")
    if "-" in s and not s.startswith("-"): s = "-" + s.replace("-","")
    try:
        v = float(s);  v = -v if neg else v
        return round(v, 2)
    except Exception:
        return None

def _parse_date(v) -> Optional[pd.Timestamp]:
    if pd.isna(v) or str(v).strip()=="": return None
    return pd.to_datetime(v, errors="coerce")

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); df.columns = [str(c).strip() for c in df.columns]; return df

def _read_all_sheets(file_like: bytes, filename: str) -> Dict[str, pd.DataFrame]:
    ext = filename.lower().rsplit(".", 1)[-1]
    engine = "openpyxl" if ext=="xlsx" else ("xlrd" if ext=="xls" else None)
    xls = pd.ExcelFile(io.BytesIO(file_like), engine=engine)
    return {s: _norm_cols(xls.parse(s, dtype=object)) for s in xls.sheet_names}

def _extract_files_from_zip(zip_bytes: bytes) -> List[Tuple[str, bytes]]:
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for zi in zf.infolist():
            name = zi.filename
            if name.endswith("/") or name.startswith("__MACOSX"): continue
            if name.lower().endswith((".xlsx",".xls")): out.append((name.split("/")[-1], zf.read(zi)))
    return out

def _bytes_from_uploader(uploaded_files) -> List[Tuple[str, bytes]]:
    collected=[]
    for uf in uploaded_files or []:
        name, data = uf.name, uf.read()
        if name.lower().endswith(".zip"): collected.extend(_extract_files_from_zip(data))
        else: collected.append((name, data))
    collected=[(n,b) for (n,b) in collected if n.lower().endswith((".xlsx",".xls"))]
    seen=set(); uniq=[]
    for (n,b) in collected:
        key=(n.lower(), hashlib.md5(b).hexdigest())
        if key in seen: continue
        seen.add(key); uniq.append((n,b))
    return uniq

def _first_present(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lower={c.lower():c for c in df.columns}
    for cand in candidates:
        k=cand.strip().lower()
        if k in lower: return lower[k]
    return None

def _find_col_regex(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    for col in df.columns:
        lc=str(col).lower()
        for pat in patterns:
            if re.search(pat, lc): return col
    return None

def _guess_date_column(df: pd.DataFrame) -> Optional[str]:
    """Prefer Gregorian columns and penalize Hijri-labeled ones."""
    best_col, best_score = None, -1e9
    total = len(df)
    for c in df.columns:
        name = str(c).lower()
        if "hijri" in name or "hijrah" in name: bias = -0.5  # penalize
        else: bias = 0.0
        ser = pd.to_datetime(df[c], errors="coerce")
        valid = ser.notna().sum()
        if valid == 0: continue
        yrs = ser.dt.year.dropna()
        within = ((yrs>=2000) & (yrs<=2035)).mean() if len(yrs) else 0
        name_bonus = 0.6 if "value date" in name else (0.4 if "posted" in name or "posting" in name else 0.0)
        score = valid/ max(total,1) + 2.0*within + name_bonus + bias
        if score > best_score:
            best_col, best_score = c, score
    return best_col

def _guess_amount_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty: return None
    best_col, best_score = None, -1.0
    for c in df.columns:
        lc=str(c).lower()
        if not any(tok in lc for tok in ["amount","credit","debit","balance","الرصيد","مبلغ","مدين","دائن"]): 
            continue
        cleaned = df[c].apply(_clean_amount)
        valid = cleaned.notna().sum()
        if valid > best_score:
            best_col, best_score = c, valid
    return best_col

def _normalize_ledger(df: pd.DataFrame, amount_cands: List[str], date_cands: List[str],
                      enable_debit_credit=True, auto_date=True):
    info={"amt_col":None,"date_col":None,"credit_col":None,"debit_col":None,"net_logic":False,"amt_auto":False,"date_auto":False}
    if df.empty:
        df["_SIGNED_"]=np.nan; df["_DATE_"]=pd.NaT; df["_CREDIT_"]=np.nan; df["_DEBIT_"]=np.nan
        return df, info

    amt_col=_first_present(df, amount_cands)
    credit_sar=_find_col_regex(df,[r"\bcredit\s*\(sar\)\b", r"\bcredit\s*\(s\.?a\.?r\.?\)\b"])
    debit_sar =_find_col_regex(df,[ r"\bdebit\s*\(sar\)\b",  r"\bdebit\s*\(s\.?a\.?r\.?\)\b"])

    credit_col=debit_col=None
    if enable_debit_credit:
        if credit_sar and debit_sar: credit_col,debit_col=credit_sar,debit_sar
        else:
            debit_col = _first_present(df, ["debit","dr","debit amount","مدين"])
            credit_col= _first_present(df, ["credit","cr","credit amount","دائن"])
            if debit_col is None:
                for c in df.columns:
                    lc=str(c).lower()
                    if "debit" in lc or "مدين" in lc: debit_col=c; break
            if credit_col is None:
                for c in df.columns:
                    lc=str(c).lower()
                    if "credit" in lc or "دائن" in lc: credit_col=c; break

    if credit_col: df[credit_col]=df[credit_col].apply(_clean_amount)
    if debit_col:  df[debit_col] =df[debit_col] .apply(_clean_amount)

    if credit_col and debit_col:
        info["credit_col"], info["debit_col"], info["net_logic"]=credit_col, debit_col, True
        signed=(df[credit_col].fillna(0)-df[debit_col].fillna(0)).round(2)
    else:
        if amt_col is None:
            for c in df.columns:
                lc=str(c).lower()
                if any(tok in lc for tok in ["amount","credit amount","debit amount","balance","balance (sar)","المبلغ","الرصيد"]): amt_col=c; break
        if amt_col is None:
            guess=_guess_amount_column(df)
            if guess: amt_col, info["amt_auto"]=guess, True
        if amt_col:
            df[amt_col]=df[amt_col].apply(_clean_amount); info["amt_col"]=amt_col
            signed=df[amt_col]
        else:
            signed=pd.Series([np.nan]*len(df))

    date_col=_first_present(df, date_cands)
    if date_col:
        df[date_col]=df[date_col].apply(_parse_date); info["date_col"]=date_col
    elif auto_date:
        guess=_guess_date_column(df)
        if guess:
            df[guess]=pd.to_datetime(df[guess], errors="coerce"); info["date_col"]=guess; info["date_auto"]=True

    df["_SIGNED_"]=signed
    df["_CREDIT_"]=df[info["credit_col"]] if info["credit_col"] in df.columns else np.nan
    df["_DEBIT_"] =df[info["debit_col"]]  if info["debit_col"]  in df.columns else np.nan
    df["_DATE_"]  =df[info["date_col"]]   if info["date_col"]   in df.columns else pd.NaT
    return df, info

# ------------ UI ------------
st.markdown("<h2 style='margin:0'>Amount + Date Finder</h2>", unsafe_allow_html=True)
c0a,c0b=st.columns([1,1])
with c0a: input_file=st.file_uploader("Input (AMOUNT, DATE)", type=["xlsx","xls"])
with c0b: stmt_files=st.file_uploader("Statements (xlsx/xls or ZIP)", type=["xlsx","xls","zip"], accept_multiple_files=True)

st.divider()
c1,c2,c3=st.columns([1,1,1])
with c1:
    amount_candidates_text=st.text_input("Amount column candidates",
        value="Amount, Credit, Credit Amount, Credit (SAR), CR, Debit, Debit Amount, Debit (SAR), DR, Value, Value Amount, Balance, Balance (SAR), الرصيد, المبلغ, مدين, دائن")
with c2:
    date_candidates_text=st.text_input("Date column candidates",
        value="Value Date, Posted, Date, Transaction Date, Posting Date, تاريخ, تاريخ العملية, تاريخ المعاملة")
with c3:
    exact_amount=st.checkbox("Exact amount (2 decimals)", value=True)
c4,c5=st.columns([1,1])
with c4: use_abs=st.checkbox("Match by absolute amount", value=False)
with c5: auto_detect_date=st.checkbox("Auto-detect Date column if not found", value=True)
include_overview=st.checkbox("Include 'All_Matches' overview sheet", value=True)

run_btn=st.button("🔎 Find", type="primary", use_container_width=True)

# ------------ Main ------------
if run_btn:
    if not input_file: st.error("Upload the Input Excel."); st.stop()
    if not stmt_files: st.error("Upload statements (or a ZIP)."); st.stop()

    try:
        df_in=pd.read_excel(input_file, dtype=object); df_in=_norm_cols(df_in)
    except Exception as e:
        st.error(f"Failed to read Input: {e}"); st.stop()

    m={c.lower():c for c in df_in.columns}
    c_amt_in, c_date_in = m.get("amount"), m.get("date")
    if not (c_amt_in and c_date_in):
        st.error("Input must have columns: AMOUNT and DATE."); st.stop()

    df_in["_AMOUNT"]=df_in[c_amt_in].apply(_clean_amount)
    df_in["_DATE"]  =df_in[c_date_in].apply(_parse_date)
    df_in=df_in.dropna(subset=["_AMOUNT","_DATE"])
    if df_in.empty: st.error("No valid rows after cleaning."); st.stop()

    files=_bytes_from_uploader(stmt_files)
    if not files: st.error("No readable Excel files found."); st.stop()

    amt_cands=[s.strip() for s in amount_candidates_text.split(",") if s.strip()]
    date_cands=[s.strip() for s in date_candidates_text.split(",") if s.strip()]

    # Read & normalize all sheets
    repo: Dict[str, Dict[str, Tuple[pd.DataFrame,str]]] = {}
    diag=[]
    for (fname,fbytes) in files:
        try: sheets=_read_all_sheets(fbytes, fname)
        except Exception as e:
            st.warning(f"Skipped {fname}: {e}"); continue
        repo[fname]={}
        for sname, df in sheets.items():
            df_norm, info = _normalize_ledger(df.copy(), amt_cands, date_cands, enable_debit_credit=True, auto_date=auto_detect_date)
            bank_code=_detect_bank(fname, sname, df)
            repo[fname][sname]=(df_norm, bank_code)
            diag.append({"File":fname,"Sheet":sname,"Bank":bank_code,
                         "Amount Col":info.get("amt_col") or info.get("credit_col") or info.get("debit_col"),
                         "Date Col": (info.get("date_col") + (" (auto)" if info.get("date_auto") else "")) if info.get("date_col") else None,
                         "Net Logic":"Yes" if info.get("net_logic") else "No",
                         "Amount rows": int(df_norm["_SIGNED_"].notna().sum()),
                         "Date rows": int(df_norm["_DATE_"].notna().sum())})
    with st.expander("Diagnostics", expanded=False):
        if diag: st.dataframe(pd.DataFrame(diag), use_container_width=True)
        else: st.info("No sheets parsed.")

    tol=0.0 if exact_amount else 0.01
    matched_rows=[]
    for idx, r in df_in.iterrows():
        amt=float(abs(r["_AMOUNT"]) if use_abs else r["_AMOUNT"])
        d0=r["_DATE"]
        for fname in repo:
            for sname,(d, bank_code) in repo[fname].items():
                if d.empty or d["_DATE_"].isna().all(): continue
                series=d["_SIGNED_"]
                mask = (
                    (series -  amt).abs() <= tol |
                    (series +  amt).abs() <= tol |
                    (d["_CREDIT_"].fillna(0) - amt).abs() <= tol |
                    (d["_DEBIT_"].fillna(0)  - amt).abs() <= tol |
                    ((series.abs() - amt).abs() <= tol if use_abs else False)
                )
                mask = mask & (d["_DATE_"] >= d0)
                if mask.any():
                    m=d.loc[mask].copy()
                    m.insert(0,"Bank (Detected)", bank_code)
                    m.insert(1,"Source File", fname)
                    m.insert(2,"Sheet", sname)
                    m.insert(3,"AMOUNT (Input)", amt)
                    m.insert(4,"DATE From (Input)", pd.to_datetime(d0).date())
                    m.insert(5,"Input Row", int(idx)+1)
                    matched_rows.append(m)

    if not matched_rows:
        st.warning("No matches found."); st.stop()

    all_matches=pd.concat(matched_rows, ignore_index=True).drop_duplicates()

    # Export by bank
    out=io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        if include_overview:
            ov=(all_matches.groupby(["Bank (Detected)","Input Row"], as_index=False)
                .size().rename(columns={"size":"Matches"}))
            ov.to_excel(writer, index=False, sheet_name="All_Matches")
        for bank, dfb in all_matches.groupby("Bank (Detected)"):
            sheet=(bank or "OTHER")[:31]
            dfb.to_excel(writer, index=False, sheet_name=sheet)
    out.seek(0)

    st.success(f"Saved {len(all_matches):,} rows into {all_matches['Bank (Detected)'].nunique():,} bank sheet(s).")
    st.download_button(
        "⬇️ Download Excel (sheets by bank)",
        data=out.getvalue(),
        file_name=f"AmountDate_ByBank_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
