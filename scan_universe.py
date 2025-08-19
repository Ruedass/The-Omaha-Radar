# scan_universe.py  (ponerlo en C:\Users\paulo\OneDrive\Escritorio\Screener)
import sqlite3, math
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=1 * 60 * 1000, key="auto")  # 1 minuto
DB_PATH = "screener.db"
UNIVERSE_FILE = "universe_500.txt"

DEFAULTS = (13.0, 5.0, 14.0, 57, 44, 0.64, 0.36)  # pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual

def load_settings():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id=1),
        pe_cap REAL, pb_cap REAL, eve_cap REAL, val_th INTEGER, qual_th INTEGER, w_val REAL, w_qual REAL
    )""")
    conn.commit()
    cur.execute("SELECT pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual FROM settings WHERE id=1")
    row = cur.fetchone()
    conn.close()
    return row if row else DEFAULTS

def sub_inv(x, cap):
    try:
        if x is None or (isinstance(x,float) and math.isnan(x)): return float("nan")
        return max(0.0, min(1.0, (cap - float(x))/cap))
    except: return float("nan")

def score_val(pe,pb,eve, caps):
    pe_cap,pb_cap,eve_cap = caps
    vals = [sub_inv(pe, pe_cap), sub_inv(pb, pb_cap), sub_inv(eve, eve_cap)]
    vals = [v for v in vals if not (isinstance(v,float) and math.isnan(v))]
    return float(sum(vals)/len(vals)*100) if vals else float("nan")

def score_calidad_de(de_ratio):
    if de_ratio is None or (isinstance(de_ratio,float) and math.isnan(de_ratio)): return float("nan")
    try:
        x = float(de_ratio)
        if x <= 0: return 100.0
        return max(0.0, min(100.0, (3.0 - x) * 50.0))
    except: return float("nan")

def fetch_one(t):
    try:
        tk = yf.Ticker(t)
        info = {}
        try: info = tk.info or {}
        except: pass
        name = info.get("longName") or info.get("shortName") or ""
        pe = info.get("trailingPE")
        pb = info.get("priceToBook")
        eve = info.get("enterpriseToEbitda")
        de = info.get("debtToEquity")
        if isinstance(de,(int,float)) and de>10: de = de/100.0  # % -> veces
        return t, name, pe, pb, eve, de
    except:
        return t, "", None, None, None, None

def ensure_alert_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS alerts (
        ticker TEXT PRIMARY KEY,
        empresa TEXT,
        ivr REAL,
        etiqueta TEXT,
        ts TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS alerts_log (
        ticker TEXT,
        empresa TEXT,
        ivr REAL,
        etiqueta TEXT,
        ts TEXT
    )""")
    conn.commit(); conn.close()

def upsert_alert(ticker, empresa, ivr, etiqueta):
    ts = datetime.utcnow().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""INSERT INTO alerts (ticker,empresa,ivr,etiqueta,ts)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(ticker) DO UPDATE SET
                     empresa=excluded.empresa, ivr=excluded.ivr, etiqueta=excluded.etiqueta, ts=excluded.ts
                """, (ticker,empresa,ivr,etiqueta,ts))
    cur.execute("""INSERT INTO alerts_log (ticker,empresa,ivr,etiqueta,ts)
                   VALUES (?,?,?,?,?)""", (ticker,empresa,ivr,etiqueta,ts))
    conn.commit(); conn.close()

def main():
    pe_cap,pb_cap,eve_cap,val_th,qual_th,w_val,w_qual = load_settings()
    ensure_alert_tables()

    # Umbral de ALERTA (independiente del panel)
    ALERT_IVR_MIN = 85.0
    ALERT_REQUIRE = "Barato y sano"  # poné None si no lo querés exigir

    with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
        tickers = [ln.strip().upper() for ln in f if ln.strip()]

    results = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for fu in as_completed([ex.submit(fetch_one, t) for t in tickers]):
            results.append(fu.result())

    for t,name,pe,pb,eve,de in results:
        sv = score_val(pe,pb,eve, (pe_cap,pb_cap,eve_cap))
        sc = score_calidad_de(de)
        if isinstance(sv,float) and math.isnan(sv): 
            continue
        if sc is None or (isinstance(sc,float) and math.isnan(sc)): sc = 0.0
        ivr = round(w_val*float(sv) + w_qual*float(sc), 2)

        if sv >= val_th and sc >= qual_th:
            etiqueta = "Barato y sano"
        elif sv >= val_th:
            etiqueta = "Barato pero frágil"
        else:
            etiqueta = "No barato"

        if ivr >= ALERT_IVR_MIN and (ALERT_REQUIRE is None or etiqueta == ALERT_REQUIRE):
            upsert_alert(t, name, ivr, etiqueta)

if __name__ == "__main__":
    main()
