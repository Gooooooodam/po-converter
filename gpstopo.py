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
VENDOR_JASAN = "ZHEJIANG JASAN HOLDING GROUP CO., LTD"
VENDOR_FUTURE = "ZHEJIANG FUTURESTITCH SPORTS CO., LTD"

COL_MAP = {
    "PO No.": "**ThirdPartyRefNo",
    "Vendor Short Name": "**VendorName",
    "PO Release Date": "**DateOrder",
    "Orig Req XFD": "VendorReqDate",
    "Exp or Act XFD": "ExpectedShipDate",
    "Quantity": "**QtyOrder",
    "Reason Remark": "Memo",
    "PO Reference No.": "PO Reference No.",
    "Customer Order No.": "Customer Order No.",
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
    s = pd.to_datetime(series, errors="coerce")  # 先正常解析
    mask = s.isna() & series.notna()  # 仅剩无法识别的
    if mask.any():
        num = pd.to_numeric(series[mask], errors="coerce")
        mask_num = mask & num.notna()
        s.loc[mask_num] = pd.to_datetime(
            num.loc[mask_num], unit="d", origin="1899-12-30"
        )
    return s


def choose_vendor(desc: str) -> str:
    return VENDOR_JASAN if isinstance(desc, str) and re.search(r"(daily|value)", desc, re.I) else VENDOR_FUTURE


def build_tags(identifier: str) -> str:
    if not isinstance(identifier, str): return ""
    m = HEADER_RE.match(identifier.strip())
    if not m: return ""
    q_or_s, q_digit, yy, tail = m.groups()
    base = f"S1 {yy}" if (q_or_s.upper() == "Q" and q_digit in ("1", "2")) else (
        f"S2 {yy}" if q_or_s.upper() == "Q" else f"S{q_digit} {yy}")
    detail = "SPECIAL EVENTS" if tail and tail.upper() == "OC" else (f"BUY{tail}" if tail else "")
    return base + (", " + detail if detail else "")


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

def map_refno(row):
    """SO 的 RefNo 依据 Region:
       - INTNI → Customer Order No.
       - U.S.、CAN → PO No.（即 **ThirdPartyRefNo）
       - UK → PO Reference No.
       其余/缺失时回退到 **ThirdPartyRefNo
    """
    region = safe_str(row.get("Region")).upper()
    if region == "INTNI":
        return safe_str(row.get("Customer Order No."))
    elif region in {"U.S.", "US", "UNITED STATES", "CAN", "CANADA"}:
        return safe_str(row.get("**ThirdPartyRefNo"))  # PO No. 的重命名列
    elif region in {"UK", "UNITED KINGDOM"}:
        return safe_str(row.get("PO Reference No."))
    else:
        return safe_str(row.get("**ThirdPartyRefNo"))


# ─────────── 基础清洗 ───────────
def base_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COL_MAP)
    df["**StoreName"] = STORE_NAME
    df["**CurrencyCode"] = CURRENCY_CODE
    df["**CurrencyRate"] = CURRENCY_RATE
    df["DropOffAddress"] = ""
    df["**VendorName"] = df.get("Style Description", pd.Series()).apply(choose_vendor)
    df["_RawExpectedShipDate"] = df["ExpectedShipDate"]
    df["RefNumber"] = df["Customer Order No."].fillna("").astype(str).str.strip()
    # 保证"**DateExpectedDelivery"和"ExpectedShipDate"都以"Exp or Act XFD"为准且一致
    df["**DateExpectedDelivery"] = df["ExpectedShipDate"]
    m = df["RefNumber"] == ""
    df.loc[m, "RefNumber"] = df.loc[m, "PO Reference No."].fillna("").astype(str).str.strip()
    m = df["RefNumber"] == ""
    df.loc[m, "RefNumber"] = df.loc[m, "**ThirdPartyRefNo"].astype(str)

    df["**PoItemNumber"] = (
            df["Style/Part No."].astype(str).str.strip() + "_" +
            df["Color/Width"].astype(str).str.strip() + "_" +
            df["Size"].astype(str).str.strip()
    )
    df["**QtyOrder"] = pd.to_numeric(df["**QtyOrder"], errors="coerce").fillna(0).astype(int)
    # 让 ExpectedShipDate 跟 **DateExpectedDelivery 保持一致
    if "**DateExpectedDelivery" in df.columns:
        df["ExpectedShipDate"] = df["**DateExpectedDelivery"]

    # 格式化日期
    for col in ("**DateExpectedDelivery", "ExpectedShipDate", "_RawExpectedShipDate"):
        if col in df.columns:
            df[col] = to_datetime_any(df[col]).dt.strftime("%Y-%m-%d")
    return df


# ─────────── 合并 ERP 单价/Title ───────────
def merge_erp(df: pd.DataFrame, erp_df: pd.DataFrame) -> pd.DataFrame:
    need = {"ItemNumber", "Title", "StandardUnitCost"}
    if not need.issubset(erp_df.columns):
        raise KeyError(f"ERP CSV 缺列: {need - set(erp_df.columns)}")
    cost_map = dict(zip(erp_df["ItemNumber"], erp_df["StandardUnitCost"]))
    title_map = dict(zip(erp_df["ItemNumber"], erp_df["Title"]))
    df["**UnitPrice"] = df["**PoItemNumber"].map(cost_map).astype(float).fillna(0)
    df["ERP_Title"] = df["**PoItemNumber"].map(title_map)
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


def convert_so(po_bytes: bytes, erp_df: pd.DataFrame, offset_days: int) -> BytesIO:
    df = pd.read_excel(BytesIO(po_bytes), skiprows=5, engine="openpyxl")
    df = base_clean(df)
    df = merge_erp(df, erp_df)

    # 补齐 SO 所需列（无则置空）
    for col in FINAL_COLS_SO:
        if col not in df.columns:
            df[col] = ""

    # 基础默认值
    df["**SaleStoreName"] = STORE_NAME
    df["StoreName"] = ""
    df["**ExchangeRate"] = 1
    df["CustomerPO"] = ""
    df["OrderType"] = ""
    df["DateToBeCancelled"] = ""
    df["ItemUPC"] = ""

    # OrderDate = 当前日期（格式化为 YYYY-MM-DD）
    df["**OrderDate"] = datetime.now().strftime("%Y-%m-%d")


    # SO 单价固定 0；Qty 映射；ItemNumber 来自 PO 的 PoItemNumber
    df["**UnitPrice"] = 0
    df["**Qty"] = df["**QtyOrder"]
    df["**ItemNumber"] = df["**PoItemNumber"]

    # 发货日期：先从 _RawExpectedShipDate 解析；US 市场加 offset
    df["**DateToBeShipped"] = to_datetime_any(df["_RawExpectedShipDate"])
    us_mask = df["Market"].fillna("").str.strip().str.upper().eq("UNITED STATES")
    offset = DateOffset(days=offset_days)
    df.loc[us_mask, "**DateToBeShipped"] += offset
    df["**DateToBeShipped"] = df["**DateToBeShipped"].dt.strftime("%Y-%m-%d")

    # CustomerName：沿用你原来的 Market → NEW BALANCE {Market} 逻辑（US 特殊为 NEW BALANCE US）
    mkt = df["Market"].fillna("").astype(str).str.upper().str.strip()
    df["**CustomerName"] = ["NEW BALANCE USA" if m == "UNITED STATES" else f"NEW BALANCE {m}" for m in mkt]

    # ===== 关键变更开始 =====
    # 1) RefNo：按 Region 映射
    df["RefNo"] = df.apply(map_refno, axis=1)

    # 2) Tags：与 PO 一致（基于 PO Header Identifier）
    df["Tags"] = df["PO Header Identifier"].apply(build_tags)

    # 3) Memo：与 PO 一致（用 build_memo）
    df["Memo"] = df.apply(build_memo, axis=1)

    # 4) SalesRepId：固定写入 "SR"
    df["SalesRepId"] = "SR"
    # ===== 关键变更结束 =====

    # 为了让 Memo 中的 Style Description 有值时更友好，仍保留 ERP 标题对 Style Description 的回填（不影响上面的 Memo）
    df["Style Description"] = df["ERP_Title"].fillna(df["Style Description"])

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
    po_file = request.files.get("po")
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
        csv_io = convert_po(po_file.read(), erp_df)
        fn = Path(po_file.filename).stem + "_PO.csv"
        return send_file(csv_io, download_name=fn, as_attachment=True, mimetype="text/csv")
    except Exception as e:
        flash(str(e));
        return redirect(url_for("index"))


@app.route("/convert_so", methods=["POST"])
def route_so():
    po_file, erp_df = _get_files()
    if po_file is None: return redirect(url_for("index"))
    try:
        offset_days = int(request.form.get("offset_days", 0))
        if offset_days < 0: offset_days = 0
    except ValueError:
        offset_days = 0
    try:
        csv_io = convert_so(po_file.read(), erp_df, offset_days)
        fn = Path(po_file.filename).stem + "_SO.csv"
        return send_file(csv_io, download_name=fn, as_attachment=True, mimetype="text/csv")
    except Exception as e:
        flash(str(e));
        return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)




