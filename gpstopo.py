#!/usr/bin/env python
# coding: utf-8
"""
PO & SO Converter  (2025-05-09)
  • 客户 PO Excel (.xlsx)  — skiprows=5
  • ERP 产品 CSV  (.csv)  — 必含列: ItemNumber, Title, StandardUnitCost
"""

from pathlib import Path
from io import BytesIO
from datetime import datetime
import re
# 在 gpstopo.py（或新建 api.py）里添加
from flask import jsonify
import requests, tempfile, uuid
import pandas as pd
from flask import (
    Flask, render_template_string, request, send_file,
    flash, redirect, url_for
)

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

STATIC_DIR = Path(__file__).with_name("static")
STATIC_DIR.mkdir(exist_ok=True)

TOKEN = "ERIN"            # 简单 Bearer 认证

@app.route("/api/convert", methods=["POST"])
def api_convert():
    # 1) 简单鉴权
    if request.headers.get("Authorization") != f"Bearer {TOKEN}":
        return jsonify({"error": "unauthorized"}), 401

    data = request.json or {}
    file_url   = data.get("file_url")
    erp_url    = data.get("erp_url")
    doc_type   = data.get("doc_type", "po")   # "po" | "so"

    if not file_url or not erp_url:
        return jsonify({"error": "file_url and erp_url required"}), 400

    try:
      # 2) 下载两份文件到临时目录
      with tempfile.TemporaryDirectory() as tmp:
          xls_path = Path(tmp) / "gps.xlsx"
          csv_path = Path(tmp) / "erp.csv"
          xls_path.write_bytes(requests.get(file_url).content)
          csv_path.write_bytes(requests.get(erp_url).content)
          erp_df = pd.read_csv(csv_path)
  
          # 3) 调用现有逻辑
          with open(xls_path, "rb") as f:
              if doc_type == "so":
                  out_io = convert_so(f.read(), erp_df)
              else:
                  out_io = convert_po(f.read(), erp_df)
  
          # 4) 保存输出文件到静态目录，生成可公开访问的 URL
          out_name = f"{uuid.uuid4().hex}_{doc_type.upper()}.csv"
          out_path = STATIC_DIR / out_name
          out_path.parent.mkdir(exist_ok=True)
          out_path.write_bytes(out_io.getvalue())
  
      # 5) 把可下载链接返回给 GPT
      return jsonify({"download_url": request.host_url + "static/" + out_name})
    except Exception as e:
      return jsonify({"error: ": str(e)}),500
        


# ─────────── 常量 & 列映射 ───────────
STORE_NAME, CURRENCY_CODE, CURRENCY_RATE = "PORT DROP OFF", "USD", 1
VENDOR_JASAN  = "ZHEJIANG JASAN HOLDING GROUP CO., LTD"
VENDOR_FUTURE = "ZHEJIANG FUTURESTITCH SPORTS CO., LTD"

COL_MAP = {
    "PO No.":              "**ThirdPartyRefNo",
    "Vendor Short Name":   "**VendorName",
    "PO Release Date":     "**DateOrder",
    "Orig Req XFD":        "VendorReqDate",
    "Curr CFM XFD":        "**DateExpectedDelivery",
    "Exp or Act XFD":      "ExpectedShipDate",
    "Quantity":            "**QtyOrder",
    "Reason Remark":       "Memo",
    "PO Reference No.":    "PO Reference No.",
    "Customer Order No.":  "Customer Order No.",
}

DATE_COLS = {"**DateOrder", "VendorReqDate", "**DateExpectedDelivery", "ExpectedShipDate"}

HEADER_RE = re.compile(r"^([QS])(\d)(\d{2})(?:\s*-\s*((?:[1-5])|OC))?", re.I)

FINAL_COLS_PO = [
    "**ThirdPartyRefNo", "**StoreName", "**CurrencyCode", "**VendorName",
    "**DateOrder", "VendorReqDate", "**DateExpectedDelivery", "ExpectedShipDate",
    "**CurrencyRate", "Memo", "RefNumber", "**PoItemNumber", "**UnitPrice",
    "**QtyOrder", "DropOffAddress", "Tags"
]

FINAL_COLS_SO = [
    "**ThirdPartyRefNo", "**SaleStoreName", "StoreName", "**CurrencyCode",
    "**CustomerName", "CustomerPO", "**DateOrder", "**DateToBeShipped",
    "**ExchangeRate", "Memo", "**ItemNumber", "**UnitPrice", "**Qty",
    "OrderType", "DateToBeCancelled", "ItemUPC"
]

# ─────────── 工具函数 ───────────
def clean_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")

def choose_vendor(desc: str) -> str:
    return VENDOR_JASAN if isinstance(desc, str) and re.search(r"(daily|value)", desc, re.I) else VENDOR_FUTURE

def build_tags(identifier: str) -> str:
    if not isinstance(identifier, str): return ""
    m = HEADER_RE.match(identifier.strip())
    if not m: return ""
    q_or_s, q_digit, yy, tail = m.groups()
    base = f"S1 {yy}" if (q_or_s.upper()=="Q" and q_digit in ("1","2")) else (f"S2 {yy}" if q_or_s.upper()=="Q" else f"S{q_digit} {yy}")
    detail = "SPECIAL EVENTS" if tail and tail.upper()=="OC" else (f"BUY{tail}" if tail else "")
    return base + (", "+detail if detail else "")

def safe_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in ("", "nan"):
        return ""
    return s[:-2] if s.endswith(".0") and s.replace(".", "", 1).isdigit() else s

def build_memo(row):
    parts = [
        safe_str(row.get("Brand")),
        safe_str(row.get("Market")),
        safe_str(row.get("PO Header Identifier")),
        "PO REFERENCE #",
        safe_str(row.get("PO Reference No.")),
        safe_str(row.get("Customer Order No.")),
    ]
    return ", ".join([p for p in parts if p])


# ─────────── 基础清洗 ───────────
def base_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COL_MAP)
    df["**StoreName"]    = STORE_NAME
    df["**CurrencyCode"] = CURRENCY_CODE
    df["**CurrencyRate"] = CURRENCY_RATE
    df["DropOffAddress"] = ""
    df["**VendorName"]   = df.get("Style Description", pd.Series()).apply(choose_vendor)

    df["RefNumber"] = df["Customer Order No."].fillna("").astype(str).str.strip()
    m = df["RefNumber"] == ""
    df.loc[m, "RefNumber"] = df.loc[m, "PO Reference No."].fillna("").astype(str).str.strip()
    m = df["RefNumber"] == ""
    df.loc[m, "RefNumber"] = df.loc[m, "**ThirdPartyRefNo"].astype(str)

    df["**PoItemNumber"] = (
        df["Style/Part No."].astype(str).str.strip() + "_" +
        df["Color/Width"].astype(str).str.strip()   + "_" +
        df["Size"].astype(str).str.strip()
    )

    for col in DATE_COLS & set(df.columns):
        df[col] = clean_date(df[col])

    df["**QtyOrder"] = pd.to_numeric(df["**QtyOrder"], errors="coerce").fillna(0).astype(int)
    return df

# ─────────── 合并 ERP 单价/Title ───────────
def merge_erp(df: pd.DataFrame, erp_df: pd.DataFrame) -> pd.DataFrame:
    need = {"ItemNumber", "Title", "StandardUnitCost"}
    if not need.issubset(erp_df.columns):
        raise KeyError(f"Missing Columns in Xoro file: {need - set(erp_df.columns)}")
    cost_map  = dict(zip(erp_df["ItemNumber"], erp_df["StandardUnitCost"]))
    title_map = dict(zip(erp_df["ItemNumber"], erp_df["Title"]))
    df["**UnitPrice"] = df["**PoItemNumber"].map(cost_map).astype(float).fillna(0)
    df["ERP_Title"]   = df["**PoItemNumber"].map(title_map)
    return df

# ─────────── PO & SO 生成 ───────────
def convert_po(po_bytes: bytes, erp_df: pd.DataFrame) -> BytesIO:
    df = pd.read_excel(BytesIO(po_bytes), skiprows=5, engine="openpyxl")
    df = base_clean(df)
    df = merge_erp(df, erp_df)

    df["Memo"] = df.apply(build_memo, axis=1)

    df["Tags"] = df["PO Header Identifier"].apply(build_tags)

    out = BytesIO()
    df[FINAL_COLS_PO].to_csv(out, index=False, encoding="utf-8")
    out.seek(0)
    return out

def convert_so(po_bytes: bytes, erp_df: pd.DataFrame) -> BytesIO:
    df = pd.read_excel(BytesIO(po_bytes), skiprows=5, engine="openpyxl")
    df = base_clean(df)
    df = merge_erp(df, erp_df)

    df["**SaleStoreName"]   = STORE_NAME
    df["StoreName"]         = ""
    df["**ExchangeRate"]    = 1
    df["CustomerPO"]        = ""
    df["OrderType"]         = ""
    df["DateToBeCancelled"] = ""
    df["ItemUPC"]           = ""

    df["**UnitPrice"] = 0  # SO 单价固定 0
    df["**Qty"]       = df["**QtyOrder"]      # 列名转换
    df["**ItemNumber"] = df["**PoItemNumber"]
    df["**DateToBeShipped"] = df["**DateExpectedDelivery"]

    # CustomerName
    mkt = df["Market"].fillna("").astype(str).str.upper().str.strip()
    df["**CustomerName"] = ["NEW BALANCE US" if m=="UNITED STATES" else f"NEW BALANCE {m}" for m in mkt]

    # Memo = ERP Title + PO Header Identifier
    df["Style Description"] = df["ERP_Title"].fillna(df["Style Description"])
    df["Memo"] = (
        df["Style Description"].fillna("").astype(str).str.strip() + ", " +
        df["PO Header Identifier"].fillna("").astype(str).str.strip()
    ).str.strip(", ")

    out = BytesIO()
    df[FINAL_COLS_SO].to_csv(out, index=False, encoding="utf-8")
    out.seek(0)
    return out

# ─────────── 前端 HTML ───────────
HTML = """
<!DOCTYPE html><html><head>
<meta charset='UTF-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>PO / SO Converter</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:760px;margin:40px auto;padding:1rem;color:#333}
 .box{border:2px dashed #777;padding:2rem;text-align:center;border-radius:12px}
 label{display:block;margin:1.2rem 0 .3rem;font-weight:600;text-align:left}
 button{margin:.4rem;padding:.6rem 1.2rem;border:0;border-radius:8px;font-weight:600;cursor:pointer}
 ul{color:red} footer{margin-top:3rem;text-align:center;color:#777;font-size:.8rem}
</style>
</head><body>
<h1>PO / SO CSV Converter</h1>
{% with messages = get_flashed_messages() %}
  {% if messages %}<ul>{% for m in messages %}<li>{{ m }}</li>{% endfor %}</ul>{% endif %}
{% endwith %}
<form class='box' method='post' enctype='multipart/form-data'>
 <label>1️⃣ XPC GPS Report (.xlsx)</label><input type='file' name='po'  accept='.xlsx' required>
 <label>2️⃣ Xoro data file (.csv)</label><input type='file' name='erp' accept='.csv'  required><br>
 <button formaction='/convert_po' type='submit'>Convert to PO</button>
 <button formaction='/convert_so' type='submit'>Convert to SO</button>
</form>
<footer>&copy; {{year}} Converter</footer>
</body></html>
"""

@app.route("/")
def index():
    return render_template_string(HTML, year=datetime.now().year)

def _get_files():
    po_file  = request.files.get("po")
    erp_file = request.files.get("erp")
    if not po_file or not erp_file:
        flash("Please upload both GPS report and Xoro file: ")
        return None, None
    try:
        erp_df = pd.read_csv(erp_file)
    except Exception as e:
        flash(f"Unreadable Xoro file: {e}")
        return None, None
    return po_file, erp_df

@app.route("/convert_po", methods=["POST"])
def route_po():
    po_file, erp_df = _get_files()
    if po_file is None: return redirect(url_for("index"))
    try:
        csv_io = convert_po(po_file.read(), erp_df)
        fn = Path(po_file.filename).stem + "_PO.csv"
        return send_file(csv_io, download_name=fn, as_attachment=True, mimetype="text/csv")
    except Exception as e:
        flash(str(e)); return redirect(url_for("index"))

@app.route("/convert_so", methods=["POST"])
def route_so():
    po_file, erp_df = _get_files()
    if po_file is None: return redirect(url_for("index"))
    try:
        csv_io = convert_so(po_file.read(), erp_df)
        fn = Path(po_file.filename).stem + "_SO.csv"
        return send_file(csv_io, download_name=fn, as_attachment=True, mimetype="text/csv")
    except Exception as e:
        flash(str(e)); return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
