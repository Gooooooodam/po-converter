# po_excel_converter_app.py – Fixed template string & brace issues (2025‑05‑08)
"""
Flask mini‑app — Convert customer PO Excel → ERP‑formatted CSV
==============================================================
• Mirrors CLI logic dated 2025‑05‑08
• Fixed: unterminated triple‑quoted string & f‑string/Jinja brace conflicts
• Template is now a plain triple‑quoted string; `year` is passed from route.
"""
from pathlib import Path
from io import BytesIO
from datetime import datetime
import re

import pandas as pd
from flask import Flask, render_template_string, request, send_file, flash, redirect, url_for

app = Flask(__name__)
app.secret_key = "change‑this‑secret-key"

# ───────────────────────── CONFIG ──────────────────────────
COL_MAP = {
    "PO No.": "**ThirdPartyRefNo",
    "Vendor Short Name": "**VendorName",  # placeholder, overwritten later
    "PO Release Date": "**DateOrder",
    "Orig Req XFD": "VendorReqDate",
    "Curr CFM XFD": "**DateExpectedDelivery",
    "Exp or Act XFD": "ExpectedShipDate",
    "Reason Remark": "Memo",              # placeholder; rebuilt later
    "FOB Price": "**UnitPrice",
    "Quantity": "**QtyOrder",
    "PO Reference No.": "PO Reference No.",
    "Customer Order No.": "Customer Order No.",
}

DATE_COLS = {"**DateOrder", "VendorReqDate", "**DateExpectedDelivery", "ExpectedShipDate"}

STORE_NAME = "PORT DROP OFF"
CURRENCY_CODE = "USD"
CURRENCY_RATE = 1
VENDOR_JASAN = "ZHEJIANG JASAN HOLDING GROUP CO., LTD"
VENDOR_FUTURESTITCH = "ZHEJIANG FUTURESTITCH SPORTS CO., LTD"

HEADER_RE = re.compile(r"^([QS])(\d)(\d{2})(?:\s*-\s*((?:[1-5])|OC))?", re.I)

FINAL_COLS = [
    "**ThirdPartyRefNo", "**StoreName", "**CurrencyCode", "**VendorName",
    "**DateOrder", "VendorReqDate", "**DateExpectedDelivery", "ExpectedShipDate",
    "**CurrencyRate", "Memo", "RefNumber", "**PoItemNumber", "**UnitPrice",
    "**QtyOrder", "DropOffAddress", "Tags",
]

# ─────────────────────── HELPER FUNCTIONS ────────────────────────

def clean_date(series):
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def safe_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in ("", "nan"):
        return ""
    return s[:-2] if s.endswith(".0") and s.replace(".", "", 1).isdigit() else s


def choose_vendor(style_desc):
    return VENDOR_JASAN if isinstance(style_desc, str) and re.search(r"(daily|value)", style_desc, re.I) else VENDOR_FUTURESTITCH


def build_memo(row):
    parts = [
        safe_str(row.get("Brand")),
        safe_str(row.get("Market")),
        safe_str(row.get("PO Header Identifier")),
        "PO REFERENCE #",
        safe_str(row.get("**ThirdPartyRefNo")),
        safe_str(row.get("RefNumber")),
        safe_str(row.get("Customer Order No.")),
    ]
    return ", ".join([p for p in parts if p])


def build_tags(identifier):
    if not isinstance(identifier, str):
        return ""
    m = HEADER_RE.match(identifier.strip())
    if not m:
        return ""
    q_or_s, q_digit, yy, tail = m.groups()
    base = f"S1 {yy}" if (q_or_s.upper() == "Q" and q_digit in ("1", "2")) else (f"S2 {yy}" if q_or_s.upper() == "Q" else f"S{q_digit} {yy}")
    detail = "SPECIAL EVENTS" if (tail and tail.upper() == "OC") else (f"BUY{tail}" if tail else "")
    return base + (", " + detail if detail else "")

# ─────────────────────── CORE CONVERSION ────────────────────────

def convert_excel(excel_bytes):
    df = pd.read_excel(BytesIO(excel_bytes), skiprows=5, engine="openpyxl")

    missing = [c for c in COL_MAP if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns: {missing}")
    df = df.rename(columns=COL_MAP)

    df["**StoreName"] = STORE_NAME
    df["**CurrencyCode"] = CURRENCY_CODE
    df["**CurrencyRate"] = CURRENCY_RATE
    df["DropOffAddress"] = ""

    df["**VendorName"] = df.get("Style Description", pd.Series()).apply(choose_vendor)

    df["RefNumber"] = df["Customer Order No."].fillna("").astype(str).str.strip()
    mask = df["RefNumber"] == ""
    df.loc[mask, "RefNumber"] = df.loc[mask, "PO Reference No."].fillna("").astype(str).str.strip()
    mask = df["RefNumber"] == ""
    df.loc[mask, "RefNumber"] = df.loc[mask, "**ThirdPartyRefNo"].astype(str)

    df["**PoItemNumber"] = (
        df["Style/Part No."].fillna("").astype(str).str.strip() + "_" +
        df["Color/Width"].fillna("").astype(str).str.strip() + "_" +
        df["Size"].fillna("").astype(str).str.strip()
    )

    df["Memo"] = df.apply(build_memo, axis=1)
    df["Tags"] = df["PO Header Identifier"].apply(build_tags)

    for col in DATE_COLS & set(df.columns):
        df[col] = clean_date(df[col])

    df["**QtyOrder"] = pd.to_numeric(df["**QtyOrder"], errors="coerce").fillna(0).astype(int)
    df["**UnitPrice"] = pd.to_numeric(df["**UnitPrice"], errors="coerce")

    df_final = df[FINAL_COLS]
    out = BytesIO()
    df_final.to_csv(out, index=False, encoding="utf-8")
    out.seek(0)
    return out

# ─────────────────────────── FLASK UI ────────────────────────────

INDEX_HTML = """
<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='UTF-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1.0' />
  <title>PO ➜ ERP CSV Converter</title>
  <style>
    body{font-family:system-ui,sans-serif;max-width:760px;margin:40px auto;padding:0 1rem;color:#333}
    footer{margin-top:3rem;font-size:.8rem;color:#777;text-align:center}
    .box{border:2px dashed #777;padding:2rem;text-align:center;border-radius:12px}
    input[type=file]{margin:1rem 0}
    button{padding:.6rem 1.2rem;border:0;border-radius:8px;font-weight:600;cursor:pointer}
  </style>
</head>
<body>
  <h1>PO Excel ➜ ERP CSV Converter</h1>
  <p>Upload a customer PO .xlsx file, click <em>Convert</em>. The first 5 rows (metadata) are skipped automatically.</p>
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <ul style='color:red;'>
        {% for m in messages %}<li>{{ m }}</li>{% endfor %}
      </ul>
    {% endif %}
  {% endwith %}
  <form class='box' action='/convert' method='post' enctype='multipart/form-data'>
      <input type='file' name='file' accept='.xlsx' required /> <br/>
      <button type='submit'>Convert</button>
  </form>
  <footer>&copy; {{ year }} PO Converter</footer>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML, year=datetime.now().year)


@app.route("/convert", methods=["POST"])
def convert_route():
    file = request.files.get("file")
    if not file:
        flash("No file uploaded")
        return redirect(url_for("index"))
    try:
        csv_io = convert_excel(file.read())
        filename = Path(file.filename).stem + "_converted.csv"
        return send_file(csv_io, download_name=filename, as_attachment=True, mimetype="text/csv")
    except Exception as e:
        flash(str(e))
        return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0",port=5000,use_reloader=False)
