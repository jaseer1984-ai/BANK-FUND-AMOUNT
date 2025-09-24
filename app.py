# app.py — Local Amount Search: FROM/TO + Pairing + Raw Row Copy
# --------------------------------------------------------------
from __future__ import annotations
import os, io, re, json
from datetime import date
from typing import List
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="Local — Amount Search (Bank Statements)", layout="wide")
st.title("💾 Local — Amount Search (Bank Statements)")

# ---------- Config ----------
DATE_WINDOW_DAYS = 3        # ± days for pairing
AMOUNT_TOLERANCE = 0.05     # SAR tolerance

# Column aliases (lowercased)
HEADER_MAP = {
    "date":     ["value date","transaction date","trans: date","date","posted","posting date","processing date"],
    "narr":     ["details","description","transaction description","transaction details","remarks",
                 "narration","narration 1","narration 2","narration 3","narrative","narrative 1","narrative 2","narrative 3"],
    "debit":    ["debit","debit amount","amount dr.","debit (sar)","withdrawal","dr","amount dr"],
    "credit":   ["credit","credit amount","amount cr.","credit (sar)","deposit","cr","amount cr"],
    "amount":   ["amount","txn amount","transaction amount"],
    "balance":  ["balance","running balance","balance (sar)"],
    "account":  ["account","account no","iban","account number"],
    "ref":      ["reference","reference no","reference number","customer reference","txt id","trace","utr","customer reference #"],
    "bank":     ["bank"],
    "drcr":     ["dr/cr","d/c","dc","dr cr","dr or cr","type","transaction type","debit/credit","tran type","cr/dr"],
}

# Bank list (SABB & BSF are separate)
KNOWN_BANKS = ["SNB","SABB","BSF","ARB","ANB","RIB","SIB","NBK","BAB","INM"]

def detect_bank_from_name(name: str) -> str:
    n = name.lower()
    if "snb" in n or "ncb" in n: return "SNB"
    if "sabb" in n: return "SABB"
    if "bsf"  in n: return "BSF"
    if "arb"  in n or "rajhi" in n: return "ARB"
    if "anb"  in n: return "ANB"
    if "rib"  in n: return "RIB"
    if "sib"  in n: return "SIB"
    if "nbk"  in n: return "NBK"
    if "bab"  in n: return "BAB"
    if "inm"  in n: return "INM"
    return os.path.splitext(name)[0].upper()

def pick_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    cols = [c.lower().strip() for c in df.columns]
    for c in candidates:
        if c in cols:
            return df.columns[cols.index(c)]
    return None

def read_any_excel_or_csv_bytes(content: bytes, name: str) -> pd.DataFrame:
    if name.lower().endswith(".csv"):
        try:
            return pd.read_csv(io.BytesIO(content))
        except Exception:
            return pd.read_csv(io.BytesIO(content), sep=";")
    return pd.read_excel(io.BytesIO(content))

def read_any_excel_or_csv_path(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.read_csv(path, sep=";")
    return pd.read_excel(path)

def normalize_drcr(value: str) -> str | None:
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if v in {"dr","d","debit","-debit-","debits"}: return "DR"
    if v in {"cr","c","credit","+credit+","credits"}: return "CR"
    return None

@st.cache_data(show_spinner=False)
def normalize_df_with_raw(raw: pd.DataFrame, filename_hint: str) -> pd.DataFrame:
    """
    Normalize to common columns AND attach a `_rawdata` column that contains the complete
    original row (JSON) so we can show/download the exact source row later.
    The index alignment is preserved so drops keep the same rows.
    """
    # Prepare raw string/JSON per-row BEFORE filtering so it follows the index
    # Use JSON so it's easier to read back
    raw_json_series = raw.apply(lambda r: json.dumps({str(k): (None if pd.isna(v) else str(v)) for k, v in r.items()}, ensure_ascii=False), axis=1)

    c_date = pick_col(raw, HEADER_MAP["date"]) or raw.columns[0]
    c_narr = pick_col(raw, HEADER_MAP["narr"]) or raw.columns[min(1, len(raw.columns)-1)]
    c_deb  = pick_col(raw, HEADER_MAP["debit"])
    c_cred = pick_col(raw, HEADER_MAP["credit"])
    c_amt  = pick_col(raw, HEADER_MAP["amount"])
    c_bal  = pick_col(raw, HEADER_MAP["balance"]) or None
    c_acct = pick_col(raw, HEADER_MAP["account"]) or None
    c_ref  = pick_col(raw, HEADER_MAP["ref"]) or None
    c_bank = pick_col(raw, HEADER_MAP["bank"]) or None
    c_drcr = pick_col(raw, HEADER_MAP["drcr"]) or None

    df = pd.DataFrame(index=raw.index)
    df["date"] = pd.to_datetime(raw[c_date], errors="coerce").dt.date
    df["narration"] = raw[c_narr].astype(str).str.strip() if c_narr else ""
    df["balance"] = pd.to_numeric(raw[c_bal], errors="coerce") if c_bal else pd.NA
    df["account"] = raw[c_acct].astype(str).str.strip() if c_acct else ""
    df["ref"] = raw[c_ref].astype(str).str.strip() if c_ref else ""
    df["bank"] = (raw[c_bank].astype(str).str.strip() if c_bank else detect_bank_from_name(filename_hint))
    df["_rawdata"] = raw_json_series  # attach full original row

    debit  = pd.to_numeric(raw[c_deb], errors="coerce") if c_deb else None
    credit = pd.to_numeric(raw[c_cred], errors="coerce") if c_cred else None
    amount = pd.to_numeric(raw[c_amt], errors="coerce") if c_amt else None

    # Compute signed amount with all available signals
    if c_amt is not None and c_deb is None and c_cred is None and c_drcr is not None:
        drcr = raw[c_drcr].map(normalize_drcr)
        signed = amount.abs()
        signed[drcr == "DR"] *= -1
        signed[drcr == "CR"] *= +1
        df["amount"] = signed
    elif c_amt is not None and (c_deb is None and c_cred is None):
        # Amount only — assume already signed
        df["amount"] = amount
    else:
        # Use debit/credit columns if present
        df["amount"] = (credit.fillna(0) if credit is not None else 0) - (debit.fillna(0) if debit is not None else 0)

    # Filter unusable rows and finalize
    df = df.dropna(subset=["date"]).copy()
    df = df[~df["amount"].isna()].copy()
    df["amount"] = df["amount"].astype(float)
    df["abs_amount"] = df["amount"].abs().round(2)
    df["direction"] = np.where(df["amount"] < 0, "FROM (OUT)", "TO (IN)")
    df["ref"] = df["ref"].str.replace("\n"," ").str.replace("\r"," ")
    df["narration"] = df["narration"].str.replace("\n"," ").str.replace("\r"," ")
    return df[["date","bank","account","narration","ref","amount","abs_amount","balance","direction","_rawdata"]]

def parse_amount(text: str) -> float | None:
    s = text.strip().replace(",", " ").replace("\u066c", " ")
    s = re.sub(r"\s+", "", s)
    if not s: return None
    try: return float(s)
    except Exception:
        m = re.search(r"([0-9][0-9,]*)\.?([0-9]{0,2})", s)
        if not m: return None
        num = m.group(1).replace(",", "")
        dec = m.group(2) or ""
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
            # summarized fields
            "date_from": o["date"], "bank_from": o["bank"], "acct_from": o["account"],
            "ref_from":  o["ref"],  "narr_from": o["narration"], "amt_from": o["amount"],
            "date_to":   m["date"], "bank_to":   m["bank"], "acct_to":   m["account"],
            "ref_to":    m["ref"],  "narr_to":   m["narration"], "amt_to":   m["amount"],
            "abs_amount": o["abs_amount"], "lag_days": int(abs(pd.to_datetime(m["date"]) - pd.to_datetime(o["date"])).days),
            # raw rows (complete)
            "raw_from": o.get("_rawdata",""), "raw_to": m.get("_rawdata","")
        })
    return pd.DataFrame(matches)

def confirmation_line(row: pd.Series) -> str:
    return (f"DONE ✅ | {row['bank_from']}→{row['bank_to']} | SAR {row['abs_amount']:,.2f} "
            f"| DR Ref: {row['ref_from'] or ''} | CR Ref: {row['ref_to'] or ''} | Lag(d): {row['lag_days']}")

# ---------- Sidebar: Source ----------
st.sidebar.header("📦 Source")
mode = st.sidebar.radio("Choose input method", ["Browse & Upload files", "Local Folder path"], index=0)

if "_index" not in st.session_state:
    st.session_state._index = pd.DataFrame()

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
                    norm = normalize_df_with_raw(raw, up.name)
                    frames.append(norm.assign(source=up.name))
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
                    norm = normalize_df_with_raw(raw, name)
                    frames.append(norm.assign(source=name))
                    loaded_names.append(name)
                except Exception as e:
                    st.warning(f"Failed to read {name}: {e}")
            if frames:
                st.session_state._index = pd.concat(frames, ignore_index=True)
                st.success(f"Loaded {len(frames)} files: {', '.join(loaded_names[:6])}{' …' if len(loaded_names)>6 else ''}")

df_all = st.session_state._index

with st.expander("Preview (first 100 rows)", expanded=False):
    if not df_all.empty:
        st.dataframe(df_all.head(100), use_container_width=True)

# ---------- Filters & Search ----------
st.subheader("Search by Amount & Filters")

c1, c2, c3 = st.columns([2,1,1])
with c1:
    amt_str = st.text_input("Amount", value="1000000")
with c2:
    tol = st.number_input("Tolerance (SAR)", min_value=0.0, max_value=50.0,
                          value=AMOUNT_TOLERANCE, step=0.05)
with c3:
    go = st.button("Search")

c4, c5 = st.columns(2)
with c4:
    date_from = st.date_input("From Date", value=None)
with c5:
    date_to = st.date_input("To Date", value=None)

bank_list = sorted(set(df_all["bank"].dropna().astype(str))) if not df_all.empty else []
all_banks = ["All"] + sorted(set(KNOWN_BANKS + bank_list))

c6, c7 = st.columns(2)
with c6:
    from_bank = st.selectbox("From Bank (debit)", all_banks, index=0)
with c7:
    to_bank = st.selectbox("To Bank (credit)", all_banks, index=0)

# Sign-fix toggles
c8, c9 = st.columns(2)
with c8:
    from_debits_positive = st.checkbox("From bank debits are positive?", value=False,
                                       help="Enable if From Bank's export lists debits as positive.")
with c9:
    to_credits_negative = st.checkbox("To bank credits are negative?", value=False,
                                      help="Enable if To Bank's export lists credits as negative.")

def filter_base(df: pd.DataFrame, target: float, tol: float,
                date_from: date | None, date_to: date | None) -> pd.DataFrame:
    x = df[np.abs(df["abs_amount"] - target) <= tol].copy()
    if date_from: x = x[x["date"] >= date_from]
    if date_to:   x = x[x["date"] <= date_to]
    return x

if go:
    if df_all.empty:
        st.warning("No data loaded yet. Upload files or load a local folder from the sidebar.")
    else:
        target = parse_amount(amt_str)
        if target is None:
            st.error("Please enter a valid number, e.g., 1000000 or 1,000,000")
        else:
            base = filter_base(df_all, target, tol, date_from, date_to)

            # FROM candidates (true negatives)
            outs = base[base["amount"] < 0].copy()
            if from_bank != "All":
                outs = outs[outs["bank"].str.upper() == from_bank.upper()]

            # If From bank exports debits as positive, include those positives as assumed OUT
            if from_debits_positive and from_bank != "All":
                extra = base[(base["bank"].str.upper() == from_bank.upper()) & (base["amount"] > 0)].copy()
                extra["amount"] = -extra["amount"].abs()
                extra["abs_amount"] = extra["amount"].abs().round(2)
                extra["direction"] = "FROM (OUT) [assumed]"
                outs = pd.concat([outs, extra], ignore_index=True)

            # TO candidates (true positives)
            ins = base[base["amount"] > 0].copy()
            if to_bank != "All":
                ins = ins[ins["bank"].str.upper() == to_bank.upper()]

            # If To bank exports credits as negative, include those negatives as assumed IN
            if to_credits_negative and to_bank != "All":
                extra_in = base[(base["bank"].str.upper() == to_bank.upper()) & (base["amount"] < 0)].copy()
                extra_in["amount"] = extra_in["amount"].abs()
                extra_in["abs_amount"] = extra_in["amount"].abs().round(2)
                extra_in["direction"] = "TO (IN) [assumed]"
                ins = pd.concat([ins, extra_in], ignore_index=True)

            st.markdown(f"**Results for SAR {target:,.2f} ± {tol}**")

            colA, colB = st.columns(2)
            with colA:
                st.markdown("**FROM candidates (debit / OUT)**")
                if outs.empty:
                    st.caption("No FROM (debit) rows in the filter.")
                else:
                    st.dataframe(
                        outs.sort_values(["date","bank"])[
                            ["date","bank","account","narration","ref","amount","balance","direction","source"]
                        ],
                        use_container_width=True
                    )
            with colB:
                st.markdown("**TO candidates (credit / IN)**")
                if ins.empty:
                    st.caption("No TO (credit) rows in the filter.")
                else:
                    st.dataframe(
                        ins.sort_values(["date","bank"])[
                            ["date","bank","account","narration","ref","amount","balance","direction","source"]
                        ],
                        use_container_width=True
                    )

            # Pair ONLY the filtered candidates
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

                # Show COMPLETE original rows for each pair (FROM & TO)
                for i, r in pairs.iterrows():
                    with st.expander(f"🔎 Pair {i+1}: {r['bank_from']} → {r['bank_to']} | SAR {r['abs_amount']:,.2f}"):
                        st.markdown("**FROM raw row (complete):**")
                        try:
                            st.json(json.loads(r["raw_from"]))
                        except Exception:
                            st.text(r["raw_from"])
                        st.markdown("**TO raw row (complete):**")
                        try:
                            st.json(json.loads(r["raw_to"]))
                        except Exception:
                            st.text(r["raw_to"])

                # Build Excel with FROM/TO candidates + Pairs (including raw rows)
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
                st.caption("No debit↔credit pairs found. Try enabling a sign-fix toggle, widening dates, or removing bank filters.")
