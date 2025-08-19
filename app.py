import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import yfinance as yf
import os
import math
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=3 * 60 * 1000, key="auto")  # 3 minutos

# ======================== Config básica ========================
ASSETS_DIR = "assets"
FAVICON_PATH = os.path.join(ASSETS_DIR, "favicon.ico")
LOGO_PATH = os.path.join(ASSETS_DIR, "LOGO.png")

st.set_page_config(
    page_title="The Omaha Radar",
    page_icon=FAVICON_PATH if os.path.exists(FAVICON_PATH) else "📈",
    layout="wide",
)

DB_PATH = "screener.db"

# Defaults (si no hay settings guardados aún)
DEFAULTS = {
    "pe_cap": 25.0,
    "pb_cap": 5.0,
    "eve_cap": 15.0,
    "val_th": 70,   # umbral Valor para etiqueta
    "qual_th": 50,  # umbral Calidad para etiqueta
    "w_val": 0.7,   # peso Valor en IVR
    "w_qual": 0.3,  # peso Calidad en IVR
}

# ======================== DB mínima ============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS tickers (
        ticker TEXT PRIMARY KEY,
        created_at TEXT
    )
    """
)
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        pe_cap REAL,
        pb_cap REAL,
        eve_cap REAL,
        val_th INTEGER,
        qual_th INTEGER,
        w_val REAL,
        w_qual REAL
    )
    """
)
conn.commit()

def ensure_settings_row():
    cursor.execute("SELECT COUNT(*) FROM settings WHERE id = 1")
    n = cursor.fetchone()[0]
    if n == 0:
        cursor.execute(
            """
            INSERT INTO settings (id, pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                DEFAULTS["pe_cap"], DEFAULTS["pb_cap"], DEFAULTS["eve_cap"],
                DEFAULTS["val_th"], DEFAULTS["qual_th"],
                DEFAULTS["w_val"], DEFAULTS["w_qual"],
            ),
        )
        conn.commit()

def load_settings() -> dict:
    ensure_settings_row()
    cursor.execute("SELECT pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual FROM settings WHERE id = 1")
    row = cursor.fetchone()
    keys = ["pe_cap", "pb_cap", "eve_cap", "val_th", "qual_th", "w_val", "w_qual"]
    return dict(zip(keys, row))

def save_settings(pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual):
    cursor.execute(
        """
        UPDATE settings
        SET pe_cap=?, pb_cap=?, eve_cap=?, val_th=?, qual_th=?, w_val=?, w_qual=?
        WHERE id = 1
        """,
        (pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual),
    )
    conn.commit()

def listar_tickers() -> list:
    df = pd.read_sql_query("SELECT ticker FROM tickers ORDER BY ticker ASC", conn)
    return df["ticker"].tolist()

def agregar_ticker(t: str) -> None:
    t = (t or "").strip().upper()
    if not t:
        return
    cursor.execute(
        "INSERT OR IGNORE INTO tickers (ticker, created_at) VALUES (?, ?)",
        (t, datetime.utcnow().isoformat()),
    )
    conn.commit()

def eliminar_ticker(t: str) -> None:
    cursor.execute("DELETE FROM tickers WHERE ticker = ?", (t,))
    conn.commit()

# ======================== Datos de Yahoo! =======================
@st.cache_data(ttl=300, show_spinner=False)
def fetch_metrics(ticker: str) -> dict:
    """Trae métricas simples desde yfinance. Devuelve dict con NaN si falta algo."""
    out = {
        "Ticker": ticker,
        "Empresa": "",
        "Sector": "",
        "Industry": "",
        "Precio": float("nan"),
        "P/E": float("nan"),
        "P/B": float("nan"),
        "EV/EBITDA": float("nan"),
        "Debt/Equity": float("nan"),
        "ROE": float("nan"),  # Return on Equity (ratio, ej. 0.15 = 15%)
    }
    try:
        tk = yf.Ticker(ticker)

        # Precio
        price = None
        try:
            fi = tk.fast_info
            price = fi.get("last_price")
        except Exception:
            price = None
        if price is None:
            try:
                h = tk.history(period="1d")
                if not h.empty:
                    price = float(h["Close"].iloc[-1])
            except Exception:
                price = None
        if price is not None:
            out["Precio"] = float(price)

        # Info general
        info = {}
        try:
            info = tk.info or {}
        except Exception:
            info = {}

        out["Empresa"] = info.get("longName") or info.get("shortName") or ""
        out["Sector"]  = info.get("sector") or ""
        out["Industry"]= info.get("industry") or ""

        pe  = info.get("trailingPE")
        pb  = info.get("priceToBook")
        eve = info.get("enterpriseToEbitda")
        de_raw = info.get("debtToEquity")  # a veces en %
        roe = info.get("returnOnEquity")   # ratio (0.12 = 12%)

        out["P/E"] = float(pe) if isinstance(pe, (int, float)) else float("nan")
        out["P/B"] = float(pb) if isinstance(pb, (int, float)) else float("nan")
        out["EV/EBITDA"] = float(eve) if isinstance(eve, (int, float)) else float("nan")

        if isinstance(de_raw, (int, float)):
            de_val = float(de_raw)
            if de_val > 10:  # si parece porcentaje, pásalo a veces
                de_val = de_val / 100.0
            out["Debt/Equity"] = de_val

        if isinstance(roe, (int, float)):
            out["ROE"] = float(roe)

    except Exception:
        pass
    return out

# ======================== Helpers de score =======================
def is_financial_row(row: pd.Series) -> bool:
    s = (row.get("Sector") or "").lower()
    i = (row.get("Industry") or "").lower()
    keys = ["financial", "bank", "insurance", "capital markets", "diversified financial"]
    return any(k in s for k in keys) or any(k in i for k in keys)

def subscore_inverse(x: float, cap: float) -> float:
    """Mayor puntaje cuando el múltiplo es bajo. Escala 0..1 con cap simple."""
    try:
        if pd.isna(x):
            return float("nan")
        return max(0.0, min(1.0, (cap - float(x)) / cap))
    except Exception:
        return float("nan")

def score_val_non_fin(row: pd.Series, pe_cap: float, pb_cap: float, eve_cap: float) -> float:
    subs = [
        subscore_inverse(row.get("P/E"), pe_cap),
        subscore_inverse(row.get("P/B"), pb_cap),
        subscore_inverse(row.get("EV/EBITDA"), eve_cap),
    ]
    s = pd.Series(subs, dtype="float64").dropna()
    return float(s.mean() * 100.0) if not s.empty else float("nan")

def score_val_bank(row: pd.Series, pe_cap: float, pb_cap: float) -> float:
    """Para bancos/financieras: pondera P/B (x2) y P/E (x1)."""
    pb_sub = subscore_inverse(row.get("P/B"), pb_cap)
    pe_sub = subscore_inverse(row.get("P/E"), pe_cap)
    subs = [pb_sub, pb_sub, pe_sub]  # peso 2:1
    s = pd.Series(subs, dtype="float64").dropna()
    return float(s.mean() * 100.0) if not s.empty else float("nan")

def calcular_val_score_mixto(row: pd.Series, pe_cap: float, pb_cap: float, eve_cap: float) -> float:
    return score_val_bank(row, pe_cap, pb_cap) if is_financial_row(row) else \
           score_val_non_fin(row, pe_cap, pb_cap, eve_cap)

def score_calidad_de(de_ratio: float) -> float:
    """Calidad por D/E: menor deuda mejor. D/E<=0→100; 2→50; 3→0."""
    if pd.isna(de_ratio):
        return float("nan")
    try:
        x = float(de_ratio)
        if x <= 0:
            return 100.0
        return float(max(0.0, min(100.0, (3.0 - x) * 50.0)))
    except Exception:
        return float("nan")

def score_calidad_roe(roe_ratio: float) -> float:
    """Calidad por ROE (para bancos): 5%→0; 10%→50; 15%→100 (cap 0..100)."""
    if pd.isna(roe_ratio):
        return float("nan")
    try:
        r = float(roe_ratio)
        if r > 1.0:  # por si viniera en %
            r = r / 100.0
        score = (r - 0.05) / 0.10 * 100.0
        return float(max(0.0, min(100.0, score)))
    except Exception:
        return float("nan")

def calidad_router(row: pd.Series) -> pd.Series:
    """Devuelve score de calidad y la métrica usada (ROE para bancos, D/E para resto)."""
    if is_financial_row(row):
        sc = score_calidad_roe(row.get("ROE"))
        metric = "ROE"
    else:
        sc = score_calidad_de(row.get("Debt/Equity"))
        metric = "D/E"
    return pd.Series({"Score Calidad": sc, "Calidad por": metric})

# --- Formato hora local (Buenos Aires) sin microsegundos ---
TZ_LOCAL = "America/Argentina/Buenos_Aires"
def fmt_local(ts):
    """
    Recibe cualquier timestamp (string/datetime) en UTC o naive,
    y devuelve 'YYYY-MM-DD HH:MM:SS' en hora de Buenos Aires.
    """
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    if pd.isna(t):
        return ""
    return t.tz_convert(TZ_LOCAL).strftime("%Y-%m-%d %H:%M:%S")


# ======================== Alertas (universo) =======================
@st.cache_data(ttl=60, show_spinner=False)
def load_alerts_df() -> pd.DataFrame:
    """Lee la tabla 'alerts' creada por el escáner externo. Devuelve df vacío si no existe."""
    try:
        with sqlite3.connect(DB_PATH) as c:
            df = pd.read_sql_query(
                """
                SELECT ticker, empresa, ivr, etiqueta, ts
                FROM alerts
                ORDER BY ivr DESC, ticker ASC
                """,
                c,
            )
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].str.upper()
        return df
    except Exception:
        return pd.DataFrame(columns=["ticker", "empresa", "ivr", "etiqueta", "ts"])

# ======================== Cargar settings =======================
S = load_settings()
pe_cap, pb_cap, eve_cap = float(S["pe_cap"]), float(S["pb_cap"]), float(S["eve_cap"])
val_th, qual_th = int(S["val_th"]), int(S["qual_th"])
w_val, w_qual = float(S["w_val"]), float(S["w_qual"])
caro_th = max(10, min(90, 100 - val_th))

# ======================== Header (logo + título) ================
left_header, mid_header, right_header = st.columns([0.25, 2.2, 1])
with left_header:
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=200)
    else:
        st.markdown("### 📊")
with mid_header:
    st.markdown("<h2 style='margin-bottom:0'>The Omaha Radar</h2>", unsafe_allow_html=True)
    st.caption("v0.3 — bancos: P/B+P/E; calidad por ROE — con alertas de universo")
with right_header:
    st.markdown(f"**Última actualización:** {datetime.now().strftime('%H:%M:%S')}")

# ======================== Sidebar: Ajustes persistentes =========
with st.sidebar:
    st.subheader("Ajustes (persisten)")

    pe_cap = st.slider("Tope P/E", min_value=5.0, max_value=80.0, value=pe_cap, step=0.5)
    pb_cap = st.slider("Tope P/B", min_value=0.5, max_value=20.0, value=pb_cap, step=0.1)
    eve_cap = st.slider("Tope EV/EBITDA (no se usa en bancos)", min_value=2.0, max_value=40.0, value=eve_cap, step=0.5)

    st.markdown("---")
    st.markdown("**Margen de seguridad (umbrales de etiqueta)**")
    val_th = st.slider("Umbral de Valor (0–100)", min_value=40, max_value=90, value=val_th, step=1)
    qual_th = st.slider("Umbral de Calidad (0–100)", min_value=30, max_value=80, value=qual_th, step=1)
    st.caption(f"Etiqueta 'Barato y sano' si Valor ≥ {val_th} y Calidad ≥ {qual_th}")

    st.markdown("---")
    st.markdown("**Pesos del IVR**")
    w_val = st.number_input("Peso Valor", min_value=0.0, max_value=1.0, value=w_val, step=0.05, format="%.2f")
    w_qual = st.number_input("Peso Calidad", min_value=0.0, max_value=1.0, value=w_qual, step=0.05, format="%.2f")
    w_sum = w_val + w_qual
    if w_sum == 0:
        w_val, w_qual = DEFAULTS["w_val"], DEFAULTS["w_qual"]
        st.warning("Pesos en 0: restablecidos a 0.7/0.3.")
    elif abs(w_sum - 1.0) > 1e-9:
        w_val, w_qual = w_val / w_sum, w_qual / w_sum
        st.info(f"Pesos normalizados a Valor={w_val:.2f} / Calidad={w_qual:.2f}")

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Guardar ajustes"):
            save_settings(pe_cap, pb_cap, eve_cap, val_th, qual_th, w_val, w_qual)
            st.success("Ajustes guardados.")
            st.rerun()
    with c2:
        if st.button("Restablecer defaults"):
            save_settings(**DEFAULTS)
            st.success("Ajustes restablecidos.")
            st.rerun()

# ======================== UI principal ==========================
with st.form("add"):
    new_ticker = st.text_input("Agregar ticker (ej: AAPL, MELI, GGAL.BA, BMA.BA, NU)", value="")
    submitted = st.form_submit_button("Agregar")
    if submitted:
        agregar_ticker(new_ticker)
        st.rerun()

current = listar_tickers()

col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("Mis tickers")

    # Tags clickeables para quitar
    if current:
        st.caption("Click en un tag para quitar el ticker:")
        n_cols = 6
        cols = st.columns(n_cols)
        for i, t in enumerate(current):
            col = cols[i % n_cols]
            with col:
                if st.button(f"✕ {t}", key=f"tag_{t}"):
                    eliminar_ticker(t)
                    st.rerun()
    else:
        st.info("Aún no agregaste nada.")

    # Tabla
    if current:
        rows = [fetch_metrics(t) for t in current]
        df = pd.DataFrame(rows)

        # Valoración: mixto (bancos vs no bancos)
        df["Score (0-100)"] = df.apply(lambda r: calcular_val_score_mixto(r, pe_cap, pb_cap, eve_cap), axis=1)

        # Calidad: ROE para bancos, D/E para resto
        qual_df = df.apply(calidad_router, axis=1, result_type="expand")
        qual_df.columns = ["Score Calidad", "Calidad por"]
        df = pd.concat([df, qual_df], axis=1)

                # Etiqueta con bandas: Barato / Neutral / Caro (y "sano"/"frágil" según Calidad)
        def etiqueta(row):
            sv = row["Score (0-100)"]   # Valor
            sc = row["Score Calidad"]   # Calidad
            if pd.isna(sv):
                return "Sin datos"
            # Barato
            if sv >= val_th and (not pd.isna(sc)) and sc >= qual_th:
                return "Barato y sano"
            if sv >= val_th:
                return "Barato pero frágil"
            # Caro
            if sv <= caro_th and (not pd.isna(sc)) and sc >= qual_th:
                return "Caro pero sano"
            if sv <= caro_th:
                return "Caro y frágil"
            # Resto
            return "Neutral"

        df["Etiqueta"] = df.apply(etiqueta, axis=1)

        # Señal operativa mínima a partir de la Etiqueta
        def senal_simple(et):
            if et.startswith("Barato"):
                return "Comprar"
            if et.startswith("Caro"):
                return "Vender"
            return "Mantener"

        df["Señal"] = df["Etiqueta"].apply(senal_simple)

        # IVR final con pesos configurables; si falta calidad, tomamos 0 (conservador)
        def calc_ivr(row):
            sv = row["Score (0-100)"]
            sc = row["Score Calidad"]
            if pd.isna(sv):
                return float("nan")
            if pd.isna(sc):
                sc = 0.0
            return round(w_val * float(sv) + w_qual * float(sc), 2)

        df["IVR"] = df.apply(calc_ivr, axis=1)


        # Orden: mejores primero por IVR
        df = df.sort_values(by=["IVR", "Ticker"], ascending=[False, True], na_position="last")

        # Columnas a mostrar
        cols_show = [
    "Empresa","Sector","Precio","P/E","P/B","EV/EBITDA","Debt/Equity","ROE",
    "Calidad por","Score (0-100)","Score Calidad","IVR","Etiqueta","Señal"
        ]
        st.dataframe(
            df.set_index("Ticker")[cols_show],
            use_container_width=True, height=520
        )

with col2:
    st.subheader("Actualizar")
    if st.button("Refrescar datos"):
        fetch_metrics.clear()
        st.rerun()

    st.subheader("Alertas")
    alerts_df = load_alerts_df()
    if alerts_df.empty:
        st.info("Todavía no hay alertas. Ejecutá la tarea programada o esperá al próximo escaneo.")
    else:
        only_mine = st.checkbox("Mostrar solo mis tickers", value=True)
        view_df = alerts_df.copy()
        if only_mine:
            mine = set([t.upper() for t in current])
            view_df = view_df[view_df["ticker"].isin(mine)]

        last_ts = view_df["ts"].max() if not view_df.empty else alerts_df["ts"].max()
        if pd.notna(last_ts):
            st.caption(f"Último scan: {fmt_local(last_ts)}")


        view_df = view_df.rename(columns={
            "ticker": "Ticker",
            "empresa": "Empresa",
            "ivr": "IVR",
            "etiqueta": "Etiqueta",
            "ts": "Scan"
        })
        st.dataframe(
            view_df.set_index("Ticker")[["Empresa", "IVR", "Etiqueta", "Scan"]],
            use_container_width=True, height=320
        )

    if st.button("Actualizar alertas"):
        load_alerts_df.clear()
        st.rerun()

st.caption(
    "Notas: en bancos/financieras no se usa EV/EBITDA; valoración pondera P/B (x2) y P/E (x1). "
    "Calidad: ROE (bancos) o D/E (resto). Umbrales y pesos configurables. Caché 5 minutos. "
    "El panel 'Alertas (Universo)' lee la tabla 'alerts' generada por el escáner programado."
)

