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
from pandas.tseries.offsets import DateOffset
import pandas as pd
from flask import (
    Flask, render_template_string, request, send_file,
    flash, redirect, url_for
)

app = Flask(__name__)
app.secret_key = "change-this-secret-key"

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
    "**ThirdPartyRefNo", "ThirdPartySource", "ThirdPartyIconUrl", "ThirdPartyDisplayName", "**SaleStoreName",
    "StoreName", "**CurrencyCode", "**CustomerName", "CustomerFirstName", "CustomerLastName", "CustomerMainPhone",
    "CustomerEmailMain", "CustomerPO", "CustomerId", "CustomerAccountNumber", "**OrderDate", "**DateToBeShipped",
    "LastDateToBeShipped", "DateToBeCancelled", "OrderClassCode", "OrderClassName", "OrderTypeCode", "OrderTypeName",
    "**ExchangeRate", "Memo", "PaymentTermsName", "PaymentTermsType", "DepositRequiredTypeName",
    "DepositRequiredAmount", "RefNo", "Tags", "SalesRepId", "SalesRepName", "ShipMethodName", "CarrierName",
    "CarrierCode", "ShipServiceName", "ShipServiceCode", "FobName", "IsOrderTaxExempt", "ShippingTaxItemCode1",
    "ShippingTaxItemValue1", "ShippingTaxItemCode2", "ShippingTaxItemValue2", "ShippingTaxItemCode3",
    "ShippingTaxItemValue3", "ShippingTaxItemCode4", "ShippingTaxItemValue4", "ShippingTermsName",
    "ShippingAccountNumber", "ShippingCost", "ShippingNotes", "ShipToFirstName", "ShipToLastName", "ShipToName",
    "ShipToCompanyName", "ShipToAddr", "ShipToAddr2", "ShipToCity", "ShipToCountry", "ShipToCountryISO2", "ShipToState",
    "ShipToStateAbbr", "ShipToZpCode", "ShipToPhoneNumber", "ShipToEmail", "ShipToAddrName", "BuyerName",
    "BillToFirstName", "BillToLastName", "BillToName", "BillToAddr", "BillToAddr2", "BillToCity", "BillToCountry",
    "BillToCountryISO2", "BillToState", "BillToStateAbbr", "BillToZpCode", "BillToCompanyName", "BillToPhoneNumber",
    "BillToEmail", "BillToAddrName", "CustomerGroupName", "**ItemNumber", "ItemUpc", "ItemBrand", "Description",
    "**UnitPrice", "**Qty", "QtyAllocated", "Discount", "DiscountTypeName", "SellUomName", "ItemNotes", "TaxItemCode1",
    "TaxItemValue1", "TaxItemCode2", "TaxItemValue2", "TaxItemCode3", "TaxItemValue3", "TaxItemCode4", "TaxItemValue4",
    "DepositAmount", "DepositPercentage", "DepositAccountName", "LiabilityAccountName", "PaymentMethodName",
    "AutoDepositTotalAmount", "CustomerServiceRepId", "ItemCategoryName", "ItemGroupName", "ItemShippingCost",
    "ItemShippingTaxItemCode1", "ItemShippingTaxItemValue1", "ItemShippingTaxItemCode2", "ItemShippingTaxItemValue2",
    "ItemShippingTaxItemCode3", "ItemShippingTaxItemValue3", "ItemShippingTaxItemCode4", "ItemShippingTaxItemValue4",
    "ThirdPartyTotalAmount", "ShipFromAddrName", "ShipFromFirstName", "ShipFromLastName", "ShipFromName",
    "ShipFromAddr", "ShipFromAddr2", "ShipFromCity", "ShipFromState", "ShipFromStateAbbr", "ShipFromZpCode",
    "ShipFromCountry", "ShipFromCountryISO2", "ShipFromPhoneNumber", "ShipFromEmail", "BaseUomCode", "SellUomCode",
    "VASItemName", "VASItemCost", "ReCalcTaxesFlag", "ReCalcShippingTaxesFlag", "CustomerItemNumber", "IsEdiFlag",
    "IsEdiConfirmationSentFlag", "OrderLineClassName", "OrderLineClassCode", "DefaultLocationName",
    "QtyRemainingToShip", "AutoReleaseSalesOrder", "AutoWaveSalesOrder", "WaveAllocationCode", "AutoLockWave",
    "LineNumber", "CustomerParentName", "EdiStatusId", "IsEdiAckRequiredFlag", "ItemIdentifierCode", "ItemUnitCost",
    "TotalTaxAmount", "ReCalcPricing", "LineStatus", "ShipStatus", "OrderThirdPartyRefNumber", "ThirdPartyRefName",
    "AutoApplyVASRule", "VoidAndCreate", "KeepOriginalOrderNumber", "ProductCategoryName", "AccountCode3PL",
    "ItemQualityCode", "IsVASRequired", "VASInstruction", "AlternativeItemNumber1", "AlternativeItemNumber2",
    "AlternativeItemNumber3", "LastShipDate", "QtyShipped", "QtyOrdered", "CancelQty", "LastWaveAttemptDttm",
    "PickedDttm", "PackedDttm", "ReadyToShipDttm", "MinimumATSPercent", "FillRate", "FillRateFailedFlag",
    "RequirePackAndHold", "EIN", "DutyPaymentTerms", "CustomComment", "ResidentialFlag", "PriorityCode", "Option1Value",
    "Option1Code", "Option2Value", "Option2Code", "BasePartNumber", "ProductTitle", "PtoId", "LastWaveNumber",
    "LastWaveDatetime", "PromiseDate", "Season", "TotalPrice", "CustomPrice1", "CustomPrice2", "CustomPrice3",
    "CustomPrice4", "CustomPrice5", "CustomPrice6", "CustomPrice7", "CustomPrice8", "CustomPrice9", "CustomPrice10",
    "CustomPrice11", "CustomPrice12", "CustomPrice13", "CustomPrice14", "InvoiceNumber", "ImportError", "CustomFieldH1",
    "CustomFieldH2", "CustomFieldH3", "CustomFieldD1"

]

# ─────────── 工具函数 ───────────
def to_datetime_any(series: pd.Series) -> pd.Series:
    """
    兼容普通日期字符串/真正的 datetime/Excel 序列号 (float、int、str 数字)。
    """
    s = pd.to_datetime(series, errors="coerce")               # 先正常解析
    mask = s.isna() & series.notna()                          # 仅剩无法识别的
    if mask.any():
        num = pd.to_numeric(series[mask], errors="coerce")
        mask_num = mask & num.notna()
        s.loc[mask_num] = pd.to_datetime(
            num.loc[mask_num], unit="d", origin="1899-12-30"
        )
    return s

def adjust_dates(df: pd.DataFrame, offset_days: int = 60) -> pd.DataFrame:
    offset = DateOffset(days=offset_days)
    date_col = "**DateExpectedDelivery"
    ship_col = "ExpectedShipDate"

    # ── 让两列都来自同一原始字段 ──
    if date_col not in df.columns or df[date_col].isna().all():
        df[date_col] = df[ship_col]        # Exp or Act XFD 赋给两列
    else:
        df[ship_col] = df[date_col]        # 保险：保持一致

    # ── 解析为真正 datetime ──
    df[date_col] = to_datetime_any(df[date_col])
    df[ship_col] = df[date_col]            # 先让两列完全一致

    # ── 美国订单提前 60 天 ──
    us_mask = (
        df["Market"]
        .fillna("")
        .str.strip()
        .str.upper()
        .eq("UNITED STATES")
        & df[date_col].notna()
    )
    df.loc[us_mask, [date_col, ship_col]] -= offset

    # ── 输出为字符串 ──
    df[date_col] = df[date_col].dt.strftime("%Y-%m-%d")
    df[ship_col] = df[ship_col].dt.strftime("%Y-%m-%d")
    return df


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
def base_clean(df: pd.DataFrame, offset_days: int = 60) -> pd.DataFrame:
    df = df.rename(columns=COL_MAP)
    df["**StoreName"]    = STORE_NAME
    df["**CurrencyCode"] = CURRENCY_CODE
    df["**CurrencyRate"] = CURRENCY_RATE
    df["DropOffAddress"] = ""
    df["**VendorName"]   = df.get("Style Description", pd.Series()).apply(choose_vendor)
    df["_RawExpectedShipDate"] = df["ExpectedShipDate"]
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
    df = adjust_dates(df, offset_days)
    df["**QtyOrder"] = pd.to_numeric(df["**QtyOrder"], errors="coerce").fillna(0).astype(int)
    return df

# ─────────── 合并 ERP 单价/Title ───────────
def merge_erp(df: pd.DataFrame, erp_df: pd.DataFrame) -> pd.DataFrame:
    need = {"ItemNumber", "Title", "StandardUnitCost"}
    if not need.issubset(erp_df.columns):
        raise KeyError(f"ERP CSV 缺列: {need - set(erp_df.columns)}")
    cost_map  = dict(zip(erp_df["ItemNumber"], erp_df["StandardUnitCost"]))
    title_map = dict(zip(erp_df["ItemNumber"], erp_df["Title"]))
    df["**UnitPrice"] = df["**PoItemNumber"].map(cost_map).astype(float).fillna(0)
    df["ERP_Title"]   = df["**PoItemNumber"].map(title_map)
    return df

# ─────────── PO & SO 生成 ───────────
def convert_po(po_bytes: bytes, erp_df: pd.DataFrame, offset_days: int) -> BytesIO:
    df = pd.read_excel(BytesIO(po_bytes), skiprows=5, engine="openpyxl")
    df = base_clean(df, offset_days)
    df = merge_erp(df, erp_df)

    df["Memo"] = df.apply(build_memo, axis=1)

    df["Tags"] = df["PO Header Identifier"].apply(build_tags)

    out = BytesIO()
    df[FINAL_COLS_PO].to_csv(out, index=False, encoding="utf-8")
    out.seek(0)
    return out

def convert_so(po_bytes: bytes, erp_df: pd.DataFrame) -> BytesIO:
    df = pd.read_excel(BytesIO(po_bytes), skiprows=5, engine="openpyxl")
    df = base_clean(df, offset_days=0)
    df = merge_erp(df, erp_df)

    for col in FINAL_COLS_SO:
        if col not in df.columns:
            # 这里用 "" 填空；如果你知道某列应为数字，可改成 0
            df[col] = ""

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
    df["**DateToBeShipped"] = df["_RawExpectedShipDate"]

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
 <label>1️⃣ GPS Report (.xlsx)</label><input type='file' name='po'  accept='.xlsx' required>
 <label>2️⃣ Xoro file (.csv)</label><input type='file' name='erp' accept='.csv'  required><br>
 
  <!-- ⚠️ 新增：让用户输入提前天数（默认 60） -->
  <label>3️⃣ Offset Days (Defalut: 60 days) </label>
  <input type='number' name='offset_days' value='60' min='0' required>
  
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
        flash("请同时上传 PO Excel 和 ERP CSV 文件")
        return None, None
    try:
        erp_df = pd.read_csv(erp_file)
    except Exception as e:
        flash(f"无法读取 ERP 文件: {e}")
        return None, None
    return po_file, erp_df

@app.route("/convert_po", methods=["POST"])
def route_po():
    po_file, erp_df = _get_files()
    if po_file is None: return redirect(url_for("index"))
    try:
        offset_days = int(request.form.get("offset_days", 0))
        if offset_days < 0: offset_days = 0
    except ValueError:
        offset_days = 0

    try:
        csv_io = convert_po(po_file.read(), erp_df, offset_days)
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
