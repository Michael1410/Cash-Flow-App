import os, re, json, datetime
from typing import Optional, List, Dict, Any, Tuple
import pdfplumber
from fastapi import FastAPI, File, UploadFile, HTTPException, Query, Body
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from rapidfuzz import process, fuzz
import tempfile

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql+psycopg2://postgres:password@localhost:5432/finstat")
engine = create_engine(POSTGRES_DSN, future=True)

# Load taxonomy (config-driven)
with open("AWS/app/taxonomy.json", "r", encoding="utf-8") as f:
    TAX = json.load(f)

CANON = TAX["canon"]
ROLLUPS = TAX["rollups"]
COMPANY_MARKERS = [x.lower() for x in TAX.get("company_markers", [])]

YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
DATE_LONG_RE = re.compile(
    r"\b(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{1,2}),?\s*((?:19|20)\d{2})\b",
    re.IGNORECASE
)
NUM_RE = re.compile(r"\(?\$?\s*-?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?|\b-?\d+(?:\.\d+)?\b")
MONTHS = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE","JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"]

def norm_label(s: str) -> str:
    s = re.sub(r"\(note\s*\d+\)", "", s, flags=re.IGNORECASE)
    s = s.replace("’", "'").replace("—", "-")
    s = re.sub(r"\s+", " ", s).strip(" :-–—")
    return s

def parse_money(tok: str) -> float:
    neg = tok.strip().startswith("(") and tok.strip().endswith(")")
    tok = tok.strip("()$ ").replace(",", "")
    if tok in ("", "-"):
        return 0.0
    v = float(tok)
    return -v if neg else v

def extract_company_and_period(text: str) -> Tuple[Optional[str], Optional[datetime.date]]:
    company = None
    for line in text.splitlines():
        L = line.strip()
        if any(m in L.lower() for m in COMPANY_MARKERS) and len(L) > 5:
            if company is None or len(L) > len(company):
                company = L
    m = DATE_LONG_RE.search(text)
    period = None
    if m:
        month, day, year = m.groups()
        month_idx = MONTHS.index(month.upper()) + 1
        period = datetime.date(int(year), month_idx, int(day))
    return company, period

def detect_currency(full_text: str) -> str:
    t = full_text.upper()
    for code in ["USD","CAD","EUR","GBP","AUD","NZD","JPY","CHF","SEK","NOK","DKK","HKD","SGD","INR"]:
        if code in t:
            return code
    if "£" in full_text: return "GBP"
    if "€" in full_text: return "EUR"
    return "AUTO"

# Build fuzzy choices
FUZZY_CHOICES = []
CANON_KEY_FOR = {}
for canon_key, desc in CANON.items():
    for s in desc.get("synonyms", []):
        FUZZY_CHOICES.append(s.lower())
        CANON_KEY_FOR[s.lower()] = canon_key
    for rx in desc.get("regex", []):
        FUZZY_CHOICES.append(rx.lower())

def match_canonical(label: str, threshold: int = 84) -> Optional[str]:
    lab = label.lower()
    # regex pass
    for canon_key, desc in CANON.items():
        for rx in desc.get("regex", []):
            if re.search(rx, lab, flags=re.IGNORECASE):
                return canon_key
    # fuzzy pass
    best = process.extractOne(lab, FUZZY_CHOICES, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= threshold:
        return CANON_KEY_FOR.get(best[0], None)
    return None

class UploadResult(BaseModel):
    id: int
    company: str
    fiscal_year: int
    period_end: Optional[datetime.date]
    payload: Dict[str, Any]

app = FastAPI(title="Universal Financial Statement Parser", version="2.0")

def parse_pdf_to_year_buckets(pdf_bytes: bytes, filename: str) -> Tuple[str, Optional[datetime.date], str, Dict[int, Dict[str, float]]]:
    tmp_path = tempfile.gettempdir()  # cross-platform
    tmp_file = os.path.join(tmp_path, "_fs_universal.pdf")
    with open(tmp_file, "wb") as f:
        f.write(pdf_bytes)

    pages_text: List[str] = []
    company, period = None, None
    with pdfplumber.open(tmp_file) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            pages_text.append(t)
            if not company or not period:
                c, p = extract_company_and_period(t)
                if c and (not company or len(c) > len(company)):
                    company = c
                if p and not period:
                    period = p

    full_text = "\n".join(pages_text)
    doc_currency = detect_currency(full_text)

    years = sorted({int(y) for y in YEAR_RE.findall(full_text)})
    if not years:
        years = [period.year if period else datetime.date.today().year]

    raw: Dict[int, Dict[str, float]] = {y: {} for y in years}
    current_year_context = max(years)

    for t in pages_text:
        page_years = [int(y) for y in YEAR_RE.findall(t)]
        if page_years:
            current_year_context = max(page_years)

        for line in t.splitlines():
            line = line.strip()
            if not line:
                continue
            nums = [m.group(0) for m in NUM_RE.finditer(line)]
            if not nums:
                continue
            label_part = line.split(nums[0])[0].strip()
            label_part = norm_label(label_part)
            if not label_part:
                continue

            canon = match_canonical(label_part)
            change_map = {
                "accounts receivable": "change_accounts_receivable",
                "investment tax credits receivable": "change_investment_tax_credits_receivable",
                "inventories": "change_inventories",
                "prepaid expenses": "change_prepaid_expenses",
                "accounts payable and accrued liabilities": "change_accounts_payable_and_accrued_liabilities",
                "government remittances payable": "change_government_remittances_payable",
                "deferred revenue": "change_deferred_revenue"
            }
            if not canon:
                for syn, ck in change_map.items():
                    if syn in label_part.lower():
                        canon = ck
                        break
            if not canon:
                continue

            values = [parse_money(x) for x in nums if re.search(r"\d", x)]
            if not values:
                continue

            if len(values) >= 2 and len(years) >= 2:
                v_curr, v_prev = values[-2], values[-1]
                raw[max(years)][canon] = v_curr
                raw[min(years)][canon] = v_prev
            else:
                raw[current_year_context][canon] = values[-1]

    return (company or "UNKNOWN"), period, doc_currency, raw

def diagnostics(payload: dict) -> dict:
    d = {"checks": {}, "warnings": []}

    bs = payload["balance_sheet"]
    is_ = payload["income_statement"]
    cf = payload["cash_flows"]

    A  = bs["assets"]["total_assets"]
    LQ = bs["liabilities_and_equity"]["current_liabilities"]["total"]
    LP = bs["liabilities_and_equity"]["loan_payable"]
    EQ = bs["liabilities_and_equity"]["shareholders_equity"]["total"]
    d["checks"]["balance_sheet_identity"] = round(A - (LQ + LP + EQ), 2)

    beg = is_["retained_earnings_rollforward"]["beginning"]
    ni  = is_["net_income"]
    div = is_["retained_earnings_rollforward"]["dividends"]
    end = is_["retained_earnings_rollforward"]["end"]
    d["checks"]["retained_earnings_recon"] = round((beg + ni + div) - end, 2)

    cb = cf["cash_beginning"]
    inc = cf["increase_in_cash"]
    ce = cf["cash_end"]
    d["checks"]["cash_bridge"] = round((cb + inc) - ce, 2)

    rev = is_["revenue"]
    exp = is_["expenses"]["total"]
    ibt = is_["income_before_income_taxes"]
    d["checks"]["ibt_vs_rev_minus_exp"] = round((rev - exp) - ibt, 2)

    for k, v in d["checks"].items():
        if abs(v) > 1.0:
            d["warnings"].append(f"{k} mismatch: {v}")
    return d

def rollup_payload(company: str, period: Optional[datetime.date], currency: str, year: int, r: Dict[str, float]) -> Dict[str, Any]:
    def g(k: str) -> float: return float(r.get(k, 0.0))
    def sum_keys(keys: List[str]) -> float: return float(sum(g(k) for k in keys))

    ca_keys = ROLLUPS["balance_sheet"]["assets"]["current_assets"]
    current_assets = {k: g(k) for k in ca_keys}
    current_assets_total = sum_keys(ca_keys)

    singletons = ROLLUPS["balance_sheet"]["assets"]["singletons"]
    assets_extra = {k: g(k) for k in singletons}
    total_assets = current_assets_total + sum_keys(singletons)

    cl_keys = ROLLUPS["balance_sheet"]["liabilities_and_equity"]["current_liabilities"]
    current_liab = {k: g(k) for k in cl_keys}
    current_liab_total = sum_keys(cl_keys)

    loan_payable_key = ROLLUPS["balance_sheet"]["liabilities_and_equity"]["loan_payable"]
    loan_payable = g(loan_payable_key) if isinstance(loan_payable_key, str) else 0.0

    sh_keys = ROLLUPS["balance_sheet"]["liabilities_and_equity"]["shareholders_equity"]
    shareholders = {k: g(k) for k in sh_keys}
    shareholders_total = sum_keys(sh_keys)
    total_liab_equity = current_liab_total + loan_payable + shareholders_total

    exp_keys = ROLLUPS["income_statement"]["expenses"]
    expenses = {k: g(k) for k in exp_keys}
    expenses_total = sum_keys(exp_keys)

    payload = {
        "company": company,
        "fiscal_year": year,
        "period_end": period.isoformat() if period else None,
        "currency": currency,
        "balance_sheet": {
            "assets": {
                "current_assets": {"components": current_assets, "total": round(current_assets_total, 2)},
                "investment_in_xyz_ltd": g("investment_in_xyz_ltd"),
                "equipment_and_software": g("equipment_and_software"),
                "intangible_assets": g("intangible_assets"),
                "total_assets": round(total_assets, 2)
            },
            "liabilities_and_equity": {
                "current_liabilities": {"components": current_liab, "total": round(current_liab_total, 2)},
                "loan_payable": round(loan_payable, 2),
                "shareholders_equity": {"components": shareholders, "total": round(shareholders_total, 2)},
                "total_liabilities_and_equity": round(total_liab_equity, 2)
            }
        },
        "income_statement": {
            "revenue": g("revenue"),
            "expenses": {"components": expenses, "total": round(expenses_total, 2)},
            "income_before_income_taxes": g("income_before_income_taxes"),
            "income_taxes": g("income_taxes"),
            "net_income": g("net_income"),
            "retained_earnings_rollforward": {
                "beginning": g("retained_earnings_beginning"),
                "dividends": g("dividends"),
                "end": g("retained_earnings_end_of_year")
            }
        },
        "cash_flows": {
            "operating_activities": {
                "components": {
                    "net_income": g("net_income"),
                    "adjustments_amortization_tangible": g("amortization_of_tangible_assets"),
                    "adjustments_amortization_intangible": g("amortization_of_intangible_assets"),
                    "change_accounts_receivable": g("change_accounts_receivable"),
                    "change_investment_tax_credits_receivable": g("change_investment_tax_credits_receivable"),
                    "change_inventories": g("change_inventories"),
                    "change_prepaid_expenses": g("change_prepaid_expenses"),
                    "change_accounts_payable_and_accrued_liabilities": g("change_accounts_payable_and_accrued_liabilities"),
                    "change_government_remittances_payable": g("change_government_remittances_payable"),
                    "change_deferred_revenue": g("change_deferred_revenue")
                },
                "total": round(
                    g("net_income") + g("amortization_of_tangible_assets") + g("amortization_of_intangible_assets") +
                    g("change_accounts_receivable") + g("change_investment_tax_credits_receivable") + g("change_inventories") +
                    g("change_prepaid_expenses") + g("change_accounts_payable_and_accrued_liabilities") +
                    g("change_government_remittances_payable") + g("change_deferred_revenue"), 2)
            },
            "investing_activities": {
                "components": {"purchase_of_property_and_equipment": g("purchase_of_property_and_equipment")},
                "total": round(g("purchase_of_property_and_equipment"), 2)
            },
            "financing_activities": {
                "components": {
                    "repayment_of_shareholder_loan": g("repayment_of_shareholder_loan"),
                    "dividends_paid": g("dividends_paid"),
                    "repayment_of_loan_payable": g("repayment_of_loan_payable"),
                    "redemption_of_capital_stock": g("redemption_of_capital_stock")
                },
                "total": round(g("repayment_of_shareholder_loan") + g("dividends_paid") + g("repayment_of_loan_payable") + g("redemption_of_capital_stock"), 2)
            },
            "increase_in_cash": g("increase_in_cash"),
            "cash_beginning": g("cash_beginning"),
            "cash_end": g("cash_end")
        }
    }
    payload["_diagnostics"] = diagnostics(payload)
    return payload

@app.post("/statements/upload", response_model=List[UploadResult])
async def upload_statement(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")
    pdf_bytes = await file.read()

    try:
        company, period, currency, buckets = parse_pdf_to_year_buckets(pdf_bytes, file.filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parse error: {e}")

    results: List[UploadResult] = []
    with engine.begin() as conn:
        for year, r in sorted(buckets.items()):
            payload = rollup_payload(company, period, currency, year, r)
            row = conn.execute(
                text("""
                  INSERT INTO financial_statements
                    (company, fiscal_year, period_end, currency, source_filename, payload)
                  VALUES
                    (:company, :fy, :period_end, :currency, :src, CAST(:payload AS JSONB))
                  ON CONFLICT (company, fiscal_year)
                  DO UPDATE SET
                    period_end = EXCLUDED.period_end,
                    currency = EXCLUDED.currency,
                    source_filename = EXCLUDED.source_filename,
                    payload = EXCLUDED.payload
                  RETURNING id, company, fiscal_year, period_end, payload
                """),
                {
                    "company": payload["company"],
                    "fy": payload["fiscal_year"],
                    "period_end": payload["period_end"],
                    "currency": payload["currency"],
                    "src": file.filename,
                    "payload": json.dumps(payload, ensure_ascii=False)
                }
            ).mappings().first()
            results.append(UploadResult(**row))
    return results

@app.get("/statements", response_model=List[UploadResult])
def list_statements(company: Optional[str] = Query(None), year: Optional[int] = Query(None)):
    where, params = [], {}
    if company:
        where.append("company = :c"); params["c"] = company
    if year:
        where.append("fiscal_year = :y"); params["y"] = year
    sql = "SELECT id, company, fiscal_year, period_end, payload FROM financial_statements"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY company, fiscal_year DESC"
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [UploadResult(**r) for r in rows]

@app.get("/statements/{id}", response_model=UploadResult)
def get_statement(id: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id, company, fiscal_year, period_end, payload FROM financial_statements WHERE id=:id"), {"id": id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    return UploadResult(**row)
