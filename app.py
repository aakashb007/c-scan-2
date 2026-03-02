import streamlit as st
import asyncio
import pandas as pd
import requests, time, csv, os, json
from datetime import datetime, timezone, timedelta

# Heavy imports - loaded lazily to prevent Colab proxy timeout
@st.cache_resource(show_spinner=False)
def _load_heavy():
    import ccxt.async_support as ccxt_mod
    import pandas_ta as ta_mod
    return ccxt_mod, ta_mod

ccxt, ta = _load_heavy()

try:
    import asyncio, nest_asyncio
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)
except ImportError:
    pass
except Exception:
    pass

st.set_page_config(page_title="APEX // Pump & Dump Scanner", page_icon="🔥", layout="wide", initial_sidebar_state="expanded")

for k,v in [('results',[]),('last_scan',"—"),('scan_count',0),
            ('btc_price',0),('btc_trend',"—"),('fng_val',50),('fng_txt',"Neutral"),
            ('scan_errors',[]),('last_raw_count',0),('logged_sigs',set()),
            ('sentinel_active', False), ('sentinel_results', []),
            ('sentinel_last_check', '—'), ('sentinel_total_checked', 0),
            ('sentinel_signals_found', 0), ('sentinel_universe_size', '?'), ('social_cache', {}),
            ('social_last_fetch', 0),
            ('listing_cache', {}), ('listing_last_fetch', 0),
            ('onchain_cache', {}), ('journal_last_autocheck', 0),
            ('prev_results', {}),
            ('alerted_scores', {}),
            ('scan_modes', ['mixed'])]:
    if k not in st.session_state: st.session_state[k]=v

JOURNAL_FILE="trade_journal.csv"; COOLDOWN_FILE="symbol_cooldowns.json"; SETTINGS_FILE="apex_settings.json"

DEFAULT_SETTINGS = {
    "scan_depth": 40,
    "scan_modes": ["mixed"],
    "fast_tf": "15m",
    "slow_tf": "4h",
    "min_score": 10,
    "j_imminent": True,
    "j_building": True,
    "j_early": False,
    "whale_min_usdt": 250000,
    "btc_filter": True,
    "cooldown_on": True,
    "cooldown_hrs": 4,
    "auto_scan": False,
    "auto_interval": 5,
    "alert_min_score": 60,
    "alert_longs": True,
    "alert_shorts": True,
    "alert_squeeze": True,
    "alert_breakout": True,
    "alert_early": False,
    "alert_whale": True,
    "cls_breakout_oi_min":  7,
    "cls_breakout_vol_min": 4,
    "cls_breakout_score_min": 25,
    "cls_squeeze_fund_min": 8,
    "cls_squeeze_ob_min":   6,
    "ob_ratio_low": 1.1,
    "ob_ratio_mid": 1.5,
    "ob_ratio_high": 2.5,
    "funding_low": 0.0002,
    "funding_mid": 0.0005,
    "funding_high": 0.001,
    "oi_price_chg_low": 0.5,
    "oi_price_chg_high": 2.0,
    "vol_surge_low": 1.5,
    "vol_surge_mid": 2.0,
    "vol_surge_high": 3.0,
    "liq_cluster_near": 1.5,
    "liq_cluster_mid": 3.0,
    "liq_cluster_far": 5.0,
    "vol24h_low": 1000000,
    "vol24h_mid": 10000000,
    "vol24h_high": 50000000,
    "rsi_oversold": 40,
    "rsi_overbought": 60,
    "min_reasons": 1,
    "min_rr": 1.5,
    "require_momentum": False,
    "pts_ob_low": 5, "pts_ob_mid": 15, "pts_ob_high": 25,
    "pts_funding_low": 5, "pts_funding_mid": 15, "pts_funding_high": 25,
    "pts_oi_low": 7, "pts_oi_high": 18,
    "pts_vol_low": 3, "pts_vol_mid": 8, "pts_vol_high": 14,
    "pts_liq_near": 15, "pts_liq_mid": 8, "pts_liq_far": 4,
    "pts_vol24_low": 3, "pts_vol24_mid": 6, "pts_vol24_high": 12,
    "pts_macd": 4, "pts_rsi": 5, "pts_bb": 4, "pts_ema": 3,
    "pts_session": 4,
    "pts_sentiment": 20,
    "pts_taker": 10,
    "pts_whale_near": 25, "pts_whale_mid": 15, "pts_whale_far": 8,
    "cmc_key": "",
    "tg_token": "",
    "tg_chat_id": "",
    "discord_webhook": "https://discord.com/api/webhooks/1476606856599179265/74wKbIJEXNJ9h10Ab0Q9Vp7ZmeJ52XY18CP3lKxg3eR1BbpZSdX65IT8hbZjpEIXSqEg",
    "okx_key": "",
    "okx_secret": "",
    "okx_passphrase": "",
    "gate_key": "",
    "gate_secret": "",
    "min_vol_filter":       300000,
    "min_active_signals":   3,
    "spread_max_pct":       0.5,
    "atr_min_pct":          0.2,
    "atr_max_pct":          10.0,
    "mtf_confirm":          True,
    "pts_mtf":              12,
    "pts_divergence":       10,
    "pts_candle_pattern":   8,
    "pts_oi_funding_combo": 10,
    "dedup_symbols":        True,
    "fng_long_threshold":   30,
    "fng_short_threshold":  70,
    "vol_surge_explosive":  5.0,
    "pts_vol_explosive":    20,
    "pts_orderflow":        12,
    "orderflow_lookback":   10,
    "pts_liq_map":          15,
    "listing_alert_pts":    25,
    "onchain_whale_min":    500000,
    "pts_onchain_whale":    15,
    "journal_autocheck_on":   True,
    "journal_autocheck_mins": 15,
    # NEW: late-entry & exhaustion filter settings
    "late_entry_chg_thresh": 8.0,   # % price change that triggers late-entry check
    "late_entry_penalty":    20,    # points to deduct for late entry
    "vol_exhaust_penalty":   15,    # points to deduct for volume exhaustion
    "near_top_penalty":      15,    # points to deduct when near local top with declining momentum
    "use_4h_ob_for_sl":      True,  # use 4h order blocks to set stronger SL
    "sentinel_score_threshold": 70,
    "sentinel_batch_size": 100,
    "sentinel_check_interval": 30,
}

def load_settings():
    s = DEFAULT_SETTINGS.copy()
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f: s.update(json.load(f))
        except: pass
    return s

def save_settings(s):
    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f: json.dump(s,f,indent=2)

S = load_settings()

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def ensure_journal():
    headers = ["ts","symbol","exchange","type","pump_score","class","price","tp","sl","triggers","status"]
    if not os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(headers)
    else:
        try:
            df = pd.read_csv(JOURNAL_FILE)
            updated = False
            if 'status' not in df.columns:
                df['status'] = 'ACTIVE'; updated = True
            if 'exchange' not in df.columns:
                df.insert(2, 'exchange', 'MEXC'); updated = True
            if updated: df.to_csv(JOURNAL_FILE, index=False)
        except: pass

def log_trade(res):
    try:
        ensure_journal()
        with open(JOURNAL_FILE, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                res['symbol'],res.get('exchange','MEXC'),res['type'],res['pump_score'],res.get('cls','—'),
                round(res['price'],8),round(res.get('tp',0),8),round(res.get('sl',0),8),
                " | ".join(res['reasons']), "ACTIVE"])
    except: pass

def _journal_check_hits(df, prices, s):
    updated = False; hits = []
    for i, row in df.iterrows():
        if row.get('status') != 'ACTIVE': continue
        sym   = str(row.get('symbol', ''))
        price = prices.get(sym, 0)
        if not price: continue
        try: tp = float(row['tp']); sl = float(row['sl'])
        except: continue
        sig = row.get('type', 'LONG'); hit = None
        if sig == 'LONG':
            if price >= tp:  hit = 'TP'
            elif price <= sl: hit = 'SL'
        else:
            if price <= tp:  hit = 'TP'
            elif price >= sl: hit = 'SL'
        if hit:
            df.at[i, 'status'] = hit; updated = True
            hits.append({'symbol': sym, 'hit': hit, 'price': price,
                         'tp': tp, 'sl': sl, 'type': sig})
    return df, hits, updated

def _fire_journal_alerts(hits, s, source="Scan"):
    for h in hits:
        em = "✅" if h['hit'] == 'TP' else "🛑"
        st.toast(f"{em} [{source}] {h['symbol']} {h['hit']} @ ${h['price']:.6f}", icon=em)

def process_journal_tracking(tickers_dict, s):
    if not os.path.exists(JOURNAL_FILE): return
    try:
        df = pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns: return
        prices = {}
        for t, data in tickers_dict.items():
            base = t.split('/')[0].split(':')[0]
            px = float(data.get('last') or 0)
            if px: prices[base] = px
        df, hits, updated = _journal_check_hits(df, prices, s)
        if updated: df.to_csv(JOURNAL_FILE, index=False)
        if hits: _fire_journal_alerts(hits, s, "Scan")
    except: pass

def autocheck_journal_background(s):
    # Non-blocking: skip if not enough time has passed
    if not s.get('journal_autocheck_on', True): return
    if not os.path.exists(JOURNAL_FILE): return
    interval = s.get('journal_autocheck_mins', 15) * 60
    if time.time() - st.session_state.get('journal_last_autocheck', 0) < interval: return
    # Mark as checked immediately so it never blocks on next load
    st.session_state.journal_last_autocheck = time.time()
    try:
        df = pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns: return
        active_syms = df[df['status'] == 'ACTIVE']['symbol'].dropna().unique().tolist()
        if not active_syms: return
        prices = {}
        for sym in active_syms[:5]:  # max 5 symbols, short timeout
            try:
                r = requests.get("https://www.okx.com/api/v5/market/ticker",
                    params={"instId": f"{sym}-USDT-SWAP"}, timeout=1)
                if r.status_code == 200:
                    d = r.json().get('data', [])
                    if d: prices[sym] = float(d[0].get('last', 0) or 0)
            except: pass
        df, hits, updated = _journal_check_hits(df, prices, s)
        if updated: df.to_csv(JOURNAL_FILE, index=False)
        if hits: _fire_journal_alerts(hits, s, "Auto-Check")
    except: pass

def is_on_cooldown(sym,hrs):
    if not os.path.exists(COOLDOWN_FILE): return False
    try:
        with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f: cd=json.load(f)
        if sym in cd:
            return (datetime.now()-datetime.fromisoformat(cd[sym])).total_seconds()/3600 < hrs
    except: pass
    return False

def set_cooldown(sym):
    cd={}
    if os.path.exists(COOLDOWN_FILE):
        try:
            with open(COOLDOWN_FILE, 'r', encoding='utf-8') as f: cd=json.load(f)
        except: pass
    cd[sym]=datetime.now().isoformat()
    with open(COOLDOWN_FILE, 'w', encoding='utf-8') as f: json.dump(cd,f)

def get_session():
    h=datetime.now(timezone.utc).hour
    if 12<=h<13: return "London/NY Overlap",1.5,"#7c3aed"
    elif 13<=h<17: return "New York Open",1.4,"#059669"
    elif 8<=h<12: return "London Open",1.3,"#2563eb"
    elif 0<=h<4: return "Asian Session",0.85,"#d97706"
    else: return "Off-Hours",0.7,"#9ca3af"

def fmt(n):
    if abs(n)>=1e9: return f"${n/1e9:.2f}B"
    if abs(n)>=1e6: return f"${n/1e6:.2f}M"
    if abs(n)>=1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"

def pump_color(score, is_sniper=False):
    if is_sniper or score>=90: return "#ff0000"
    if score>=70: return "#dc2626"
    if score>=45: return "#d97706"
    if score>=25: return "#2563eb"
    return "#6b7280"

def pump_label(score, sig, is_sniper=False):
    if is_sniper or score>=90: return "🎯 GOD-TIER SETUP"
    if score>=70: return "🔥 PUMP IMMINENT" if sig=="LONG" else "🩸 DUMP IMMINENT"
    if score>=45: return "⚡ BUILDING PUMP" if sig=="LONG" else "⚡ BUILDING DUMP"
    if score>=25: return "📡 EARLY LONG" if sig=="LONG" else "📡 EARLY SHORT"
    return "— WEAK"

def classify(res):
    bd  = res.get('signal_breakdown', {})
    sc  = res['pump_score']
    cfg = res.get('_cls_cfg', {})
    br_oi  = cfg.get('breakout_oi_min',  7)
    br_vol = cfg.get('breakout_vol_min', 4)
    br_sc  = cfg.get('breakout_sc_min',  25)
    sq_fd  = cfg.get('squeeze_fund_min', 8)
    sq_ob  = cfg.get('squeeze_ob_min',   6)
    squeeze_score  = bd.get('funding', 0) + bd.get('funding_hist', 0) + bd.get('ob_imbalance', 0)
    breakout_score = bd.get('oi_spike', 0) + bd.get('vol_surge', 0) + bd.get('orderflow', 0)
    whale_score    = bd.get('whale_wall', 0) + bd.get('liq_cluster', 0)
    if bd.get('funding', 0) >= sq_fd and bd.get('ob_imbalance', 0) >= sq_ob and sc >= 25:
        return 'squeeze'
    if bd.get('oi_spike', 0) >= br_oi and bd.get('vol_surge', 0) >= br_vol and sc >= br_sc:
        return 'breakout'
    if bd.get('vol_surge', 0) >= 8 and bd.get('orderflow', 0) >= 6 and sc >= 30:
        return 'breakout'
    if bd.get('whale_wall', 0) >= 8 or (bd.get('liq_cluster', 0) >= 8 and bd.get('vol_surge', 0) >= 3):
        return 'whale_driven'
    if sc >= 90:
        return max({'squeeze': squeeze_score, 'breakout': breakout_score, 'whale_driven': whale_score},
                   key=lambda k: {'squeeze': squeeze_score, 'breakout': breakout_score, 'whale_driven': whale_score}[k])
    if sc >= 70:
        if squeeze_score >= breakout_score and squeeze_score >= whale_score: return 'squeeze'
        if breakout_score >= squeeze_score: return 'breakout'
    return 'early'

def send_tg(token, cid, msg):
    if not token or not cid: return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=8)
        if r.status_code != 200:
            st.toast(f"⚠️ Telegram failed: {r.status_code} — {r.text[:80]}", icon="⚠️")
    except Exception as _e:
        st.toast(f"⚠️ Telegram error: {_e}", icon="⚠️")

def send_discord(webhook_url, embed_dict):
    if not webhook_url: return
    try:
        r = requests.post(webhook_url, json={"embeds": [embed_dict]}, timeout=8)
        if r.status_code not in (200, 204):
            st.toast(f"⚠️ Discord failed: {r.status_code} — {r.text[:80]}", icon="⚠️")
    except Exception as _e:
        st.toast(f"⚠️ Discord error: {_e}", icon="⚠️")

# ─── CSS ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* fonts loaded locally */
:root{
  --bg:#f7f8fc;--surface:#ffffff;--panel:#f0f2f8;--border:#e2e5f0;--border2:#c8cde0;
  --text:#0f1117;--text2:#3d4461;--muted:#7a82a0;
  --green:#059669;--green-bg:#ecfdf5;--green-bd:#a7f3d0;
  --red:#dc2626;--red-bg:#fef2f2;--red-bd:#fecaca;
  --amber:#d97706;--amber-bg:#fffbeb;--amber-bd:#fde68a;
  --blue:#2563eb;--blue-bg:#eff6ff;--blue-bd:#bfdbfe;
  --purple:#7c3aed;--purple-bg:#f5f3ff;--purple-bd:#ddd6fe;
  --sh:0 1px 4px rgba(15,17,23,.06),0 4px 16px rgba(15,17,23,.04);
  --sh-lg:0 8px 32px rgba(15,17,23,.10),0 2px 8px rgba(15,17,23,.06);
}
*,*::before,*::after{box-sizing:border-box;}
html,body,.stApp{background:var(--bg)!important;font-family:sans-serif!important;color:var(--text)!important;}
#MainMenu,footer,.stDeployButton{display:none!important;}
header {background-color: transparent !important;}
section[data-testid="stSidebar"]{background:var(--surface)!important;border-right:1px solid var(--border)!important;}
section[data-testid="stSidebar"] *{color:var(--text)!important;}
section[data-testid="stSidebar"] .stMarkdown h3{font-family:monospace!important;font-size:0.58rem!important;letter-spacing:.15em!important;color:var(--muted)!important;text-transform:uppercase!important;border-bottom:1px solid var(--border)!important;padding-bottom:5px!important;margin-bottom:10px!important;}
div.stButton>button:first-child{background:var(--text)!important;color:#fff!important;font-family:monospace!important;font-size:0.72rem!important;font-weight:600!important;letter-spacing:.1em!important;text-transform:uppercase!important;border:none!important;border-radius:6px!important;padding:13px 24px!important;transition:all .18s ease!important;}
div.stButton>button:first-child:hover{background:var(--blue)!important;transform:translateY(-1px)!important;box-shadow:0 4px 16px rgba(37,99,235,.28)!important;}
div[data-testid="metric-container"]{background:var(--surface)!important;border:1px solid var(--border)!important;border-radius:8px!important;padding:12px!important;}
div[data-testid="metric-container"] label{font-family:monospace!important;font-size:.55rem!important;letter-spacing:.1em!important;text-transform:uppercase!important;color:var(--muted)!important;}
div[data-testid="metric-container"] div[data-testid="metric-value"]{font-family:monospace!important;font-size:1rem!important;font-weight:600!important;color:var(--text)!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:2px solid var(--border)!important;gap:0!important;}
.stTabs [data-baseweb="tab"]{font-family:monospace!important;font-size:.65rem!important;letter-spacing:.08em!important;font-weight:600!important;color:var(--muted)!important;padding:11px 20px!important;border-bottom:2px solid transparent!important;text-transform:uppercase!important;}
.stTabs [aria-selected="true"]{color:var(--text)!important;border-bottom:2px solid var(--text)!important;background:transparent!important;}
.stProgress>div>div{background:var(--text)!important;}
.stAlert{border-radius:6px!important;font-size:.8rem!important;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px;}
.ticker-bar{background:#0f1117;color:#fff;border-radius:8px;padding:10px 20px;
  display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;
  margin-bottom:18px;font-family:monospace;font-size:.68rem;}
.t-lbl{color:rgba(255,255,255,.38);font-size:.54rem;letter-spacing:.1em;text-transform:uppercase;}
.t-val{color:#fff;font-weight:600;}
.pump-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;
  padding:18px 20px;margin-bottom:10px;transition:box-shadow .18s,border-color .18s;position:relative;overflow:hidden;}
.pump-card:hover{box-shadow:var(--sh-lg);border-color:var(--border2);}
.pump-card::before{content:'';position:absolute;top:0;left:0;width:4px;height:100%;border-radius:10px 0 0 10px;}
.pc-long::before{background:var(--green);}
.pc-short::before{background:var(--red);}
.score-ring{width:54px;height:54px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-family:monospace;font-size:.95rem;font-weight:700;border:3px solid;flex-shrink:0;}
.px-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0;}
.px-cell{background:var(--panel);border-radius:6px;padding:8px 10px;text-align:center;}
.px-lbl{font-family:monospace;font-size:.52rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:3px;}
.px-val{font-family:monospace;font-size:.8rem;font-weight:600;color:var(--text);}
.sig-pips{display:flex;flex-wrap:wrap;gap:10px;margin:8px 0;}
.pip-item{display:flex;align-items:center;gap:5px;font-family:monospace;font-size:.6rem;color:var(--muted);}
.pip{width:7px;height:7px;border-radius:2px;}
.pip-on{background:var(--text);}
.pip-half{background:var(--border2);}
.pip-off{background:var(--panel);border:1px solid var(--border);}
.reasons-list .r{font-size:.74rem;color:var(--text2);padding:4px 0;border-bottom:1px solid var(--border);}
.tab-desc{background:var(--panel);border:1px solid var(--border);border-radius:8px;
  padding:12px 16px;margin-bottom:14px;font-size:.78rem;color:var(--text2);line-height:1.5;}
.stat-strip{background:var(--surface);border:1px solid var(--border);border-radius:8px;
  padding:12px 18px;display:flex;justify-content:space-between;align-items:center;
  flex-wrap:wrap;gap:10px;margin-bottom:14px;}
.ss-val{font-family:monospace;font-size:1.3rem;font-weight:700;color:var(--text);line-height:1;}
.ss-lbl{font-family:monospace;font-size:.52rem;letter-spacing:.1em;color:var(--muted);text-transform:uppercase;margin-top:3px;}
.empty-st{text-align:center;padding:50px 20px;color:var(--muted);font-family:monospace;font-size:.68rem;letter-spacing:.1em;}
.section-h{font-family:monospace;font-size:.58rem;letter-spacing:.18em;text-transform:uppercase;
  color:var(--muted);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.stg-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;margin-bottom:14px;}
.stg-title{font-family:monospace;font-size:.62rem;font-weight:700;letter-spacing:.15em;
  text-transform:uppercase;color:var(--text);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border);}
.hint{font-size:.72rem;color:var(--muted);line-height:1.4;margin-top:2px;padding:6px 10px;
  background:var(--panel);border-radius:5px;border-left:3px solid var(--border2);}
.hint b{color:var(--text2);}
.setting-help{font-size:.65rem;color:#7c3aed;background:#f5f3ff;border-left:3px solid #7c3aed;
  padding:4px 8px;border-radius:4px;margin-top:3px;line-height:1.4;}
.sentiment-bar{display:flex;align-items:center;gap:10px;margin:8px 0;padding:8px 12px;
  background:var(--panel);border-radius:6px;font-family:monospace;font-size:.62rem;}
.sbar-label{color:var(--muted);width:90px;flex-shrink:0;}
.sbar-track{flex:1;height:8px;background:var(--border);border-radius:4px;overflow:hidden;position:relative;}
.sbar-fill{height:100%;border-radius:4px;transition:width .3s;}
.sbar-val{color:var(--text);font-weight:600;width:40px;text-align:right;}
.whale-wall-card{border-radius:6px;padding:8px 12px;font-family:monospace;font-size:.72rem;margin:4px 0;
  display:flex;justify-content:space-between;align-items:center;}
.momentum-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;
  font-family:monospace;font-size:.58rem;font-weight:700;letter-spacing:.06em;}
.warning-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;
  font-family:monospace;font-size:.58rem;font-weight:700;letter-spacing:.06em;
  background:#fef2f2;color:#dc2626;border:1px solid #fecaca;}
</style>
""", unsafe_allow_html=True)


# ─── ENGINE ──────────────────────────────────────────────────────────────────
class PrePumpScreener:
    def __init__(self, cmc_key="",
                 okx_key="", okx_secret="", okx_passphrase="",
                 gate_key="", gate_secret=""):
        self.cmc_key = cmc_key
        okx_params = {
            'enableRateLimit': True, 'rateLimit': 50,
            'timeout': 8000, 'options': {'defaultType': 'swap'}
        }
        if okx_key and okx_secret and okx_passphrase:
            okx_params['apiKey']   = okx_key
            okx_params['secret']   = okx_secret
            okx_params['password'] = okx_passphrase
        self.okx = ccxt.okx(okx_params)
        self.mexc = ccxt.mexc({
            'enableRateLimit': True, 'rateLimit': 60,
            'timeout': 8000, 'options': {'defaultType': 'swap'}
        })
        gate_params = {
            'enableRateLimit': True, 'rateLimit': 50,
            'timeout': 8000, 'options': {'defaultType': 'swap'}
        }
        if gate_key and gate_secret:
            gate_params['apiKey'] = gate_key
            gate_params['secret'] = gate_secret
        self.gate = ccxt.gateio(gate_params)

    async def fetch_ohlcv(self, exch, sym, tf, n=200):
        try:
            raw = await exch.fetch_ohlcv(sym, tf, limit=n)
            if not raw: return pd.DataFrame()
            df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
            df['ts'] = pd.to_datetime(df['ts'],unit='ms')
            return df
        except: return pd.DataFrame()

    async def fetch_btc(self):
        try:
            df = await self.fetch_ohlcv(self.okx, "BTC/USDT:USDT", "1h", 80)
            if df.empty:
                df = await self.fetch_ohlcv(self.gate, "BTC/USDT:USDT", "1h", 80)
            if df.empty:
                df = await self.fetch_ohlcv(self.mexc, "BTC/USDT:USDT", "1h", 80)
            if df.empty: return "NEUTRAL",0,50
            df.ta.ema(length=20,append=True); df.ta.ema(length=50,append=True); df.ta.rsi(length=14,append=True)
            e20=[c for c in df.columns if 'EMA_20' in c]
            e50=[c for c in df.columns if 'EMA_50' in c]
            rc=[c for c in df.columns if 'RSI' in c]
            if not e20 or not e50 or not rc: return "NEUTRAL",df['close'].iloc[-1],50
            p=df['close'].iloc[-1]; r=df[rc[0]].iloc[-1]
            if p<df[e20[0]].iloc[-1] and p<df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]<df[e50[0]].iloc[-1] and r<45:
                return "BEARISH",p,r
            elif p>df[e20[0]].iloc[-1] and p>df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]>df[e50[0]].iloc[-1] and r>55:
                return "BULLISH",p,r
            return "NEUTRAL",p,r
        except: return "NEUTRAL",0,50

    async def safe_fetch(self, exch, sym):
        fi={}; tick={}; ob={'bids':[],'asks':[]}
        try: fi = await exch.fetch_funding_rate(sym)
        except: fi={'fundingRate':0}
        try: tick = await exch.fetch_ticker(sym)
        except: pass
        try: ob = await exch.fetch_order_book(sym,limit=100)
        except: pass
        return fi, tick, ob

    async def fetch_oi(self, exch, sym):
        try:
            oi = await exch.fetch_open_interest(sym)
            v = oi.get('openInterestValue') or oi.get('openInterest') or 0
            return float(v)
        except: return 0.0

    async def fetch_funding_history(self, exch, sym):
        try:
            history = await exch.fetch_funding_rate_history(sym, limit=24)
            rates = [float(h.get('fundingRate', 0)) for h in history if h.get('fundingRate') is not None]
            return rates
        except:
            return []

    async def fetch_oi_history(self, exch, sym):
        try:
            oi_hist = await exch.fetch_open_interest_history(sym, "1h", limit=24)
            values = [float(h.get('openInterestValue', 0) or h.get('openInterest', 0)) for h in oi_hist]
            return values
        except:
            return []

    async def fetch_recent_trades(self, exch, sym, min_usdt=50000):
        try:
            trades = await exch.fetch_trades(sym, limit=100)
            whale_buys = []
            whale_sells = []
            for t in trades:
                cost = float(t.get('cost', 0) or 0)
                side = t.get('side', '')
                price = float(t.get('price', 0) or 0)
                if cost >= min_usdt:
                    if side == 'buy':
                        whale_buys.append({'cost': cost, 'price': price})
                    elif side == 'sell':
                        whale_sells.append({'cost': cost, 'price': price})
            return whale_buys, whale_sells
        except:
            return [], []

    async def fetch_orderflow_imbalance(self, exch, sym, lookback=10):
        try:
            df = await self.fetch_ohlcv(exch, sym, "5m", lookback + 5)
            if df.empty or len(df) < lookback: return 0.0, 'NEUTRAL'
            r = df.tail(lookback)
            buy_vol  = float(r[r['close'] > r['open']]['volume'].sum())
            sell_vol = float(r[r['close'] <= r['open']]['volume'].sum())
            total = buy_vol + sell_vol
            if total == 0: return 0.0, 'NEUTRAL'
            buy_pct = buy_vol / total * 100
            direction = 'BUY' if buy_pct > 55 else ('SELL' if buy_pct < 45 else 'NEUTRAL')
            return buy_pct, direction
        except: return 0.0, 'NEUTRAL'

    async def fetch_4h_order_blocks(self, exch, sym, price):
        """
        Detect significant 4h order blocks (OB) for use in SL placement.
        An order block is a 4h candle where:
        - It was a strong momentum candle (body > 60% of range)
        - Followed by a significant move away (>1% in next 2 candles)
        - Price hasn't returned to it since
        Returns bullish OBs (potential SL support) and bearish OBs (potential SL resistance)
        """
        bull_obs = []  # bullish OBs = support zones (for LONG SL)
        bear_obs = []  # bearish OBs = resistance zones (for SHORT SL)
        try:
            df = await self.fetch_ohlcv(exch, sym, "4h", 50)
            if df.empty or len(df) < 10: return bull_obs, bear_obs

            for i in range(len(df) - 5, 5, -1):
                candle = df.iloc[i]
                c_open  = float(candle['open'])
                c_close = float(candle['close'])
                c_high  = float(candle['high'])
                c_low   = float(candle['low'])
                c_range = c_high - c_low
                if c_range <= 0: continue
                body    = abs(c_close - c_open)
                body_pct = body / c_range

                # Must be strong momentum candle
                if body_pct < 0.55: continue

                # Check next 2 candles confirm the move
                if i + 2 >= len(df): continue
                next1 = df.iloc[i+1]; next2 = df.iloc[i+2]

                if c_close > c_open:  # Bullish OB
                    # Price moved up significantly after
                    move_after = (float(next2['high']) - c_close) / c_close * 100
                    if move_after >= 0.8:
                        ob_zone_hi = c_high
                        ob_zone_lo = max(c_open, c_low)  # top of candle body / low
                        # Check if price is ABOVE this OB (it's potential support)
                        if price > ob_zone_lo and price > ob_zone_hi * 0.98:
                            dist_pct = (price - ob_zone_hi) / price * 100
                            if dist_pct <= 15:  # Only nearby OBs useful for SL
                                bull_obs.append({
                                    'zone_hi': ob_zone_hi,
                                    'zone_lo': ob_zone_lo,
                                    'dist_pct': dist_pct,
                                    'body_pct': round(body_pct, 2),
                                    'tf': '4H'
                                })
                else:  # Bearish OB
                    move_after = (c_close - float(next2['low'])) / c_close * 100
                    if move_after >= 0.8:
                        ob_zone_lo = c_low
                        ob_zone_hi = min(c_open, c_high)
                        if price < ob_zone_hi and price < ob_zone_lo * 1.02:
                            dist_pct = (ob_zone_lo - price) / price * 100
                            if dist_pct <= 15:
                                bear_obs.append({
                                    'zone_hi': ob_zone_hi,
                                    'zone_lo': ob_zone_lo,
                                    'dist_pct': dist_pct,
                                    'body_pct': round(body_pct, 2),
                                    'tf': '4H'
                                })

            # Sort by proximity
            bull_obs.sort(key=lambda x: x['dist_pct'])
            bear_obs.sort(key=lambda x: x['dist_pct'])
        except: pass
        return bull_obs, bear_obs

    async def fetch_liquidation_map(self, exch, sym, price):
        clusters = []
        try:
            dsym_c = sym.split(':')[0].replace('/USDT', '')
            r = requests.get(
                "https://www.okx.com/api/v5/public/liquidation-orders",
                params={"instType": "SWAP", "instId": f"{dsym_c}-USDT-SWAP",
                        "state": "unfilled", "limit": "100"}, timeout=5)
            if r.status_code == 200:
                raw   = r.json().get('data', [])
                items = raw[0] if raw else []
                buckets = {}
                for item in items:
                    try:
                        liq_px = float(item.get('bkPx', 0))
                        liq_sz = float(item.get('sz', 0)) * liq_px
                        side   = 'SHORT_LIQ' if item.get('side') == 'sell' else 'LONG_LIQ'
                        if liq_px <= 0 or liq_sz < 10_000: continue
                        bucket = round(liq_px / price, 3)
                        key    = (bucket, side)
                        if key not in buckets:
                            buckets[key] = {'price': liq_px, 'side': side,
                                            'size_usd': 0, 'count': 0}
                        buckets[key]['size_usd'] += liq_sz
                        buckets[key]['count'] += 1
                    except: pass
                for (bkt, side), data in buckets.items():
                    if data['size_usd'] < 50_000: continue
                    dist = abs(data['price'] - price) / price * 100
                    clusters.append({**data, 'dist_pct': dist})
                clusters.sort(key=lambda x: x['dist_pct'])
        except: pass
        return clusters

    def fetch_new_listings(self, s):
        cache = st.session_state.get('listing_cache', {})
        if cache.get('ts') and time.time() - cache['ts'] < 1800:
            return cache.get('data', [])
        listings = []; now = time.time()
        try:
            r = requests.get("https://www.okx.com/api/v5/public/instruments",
                params={"instType": "SWAP"}, timeout=7)
            if r.status_code == 200:
                cutoff_ms = (now - 7 * 86400) * 1000
                for inst in r.json().get('data', []):
                    try:
                        lt = float(inst.get('listTime', 0))
                        if lt >= cutoff_ms:
                            sym_c = inst['instId'].replace('-USDT-SWAP', '')
                            listings.append({'symbol': sym_c, 'exchange': 'OKX',
                                             'listed_ts': lt,
                                             'listed_ago_h': (now * 1000 - lt) / 3_600_000})
                    except: pass
        except: pass
        try:
            r2 = requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/contracts",
                timeout=7)
            if r2.status_code == 200:
                cutoff_s = now - 7 * 86400
                existing = {l['symbol'] for l in listings}
                for c in r2.json():
                    try:
                        ct = float(c.get('create_time', 0))
                        if ct >= cutoff_s:
                            sym_c = c['name'].replace('_USDT', '')
                            if sym_c not in existing:
                                listings.append({'symbol': sym_c, 'exchange': 'GATE',
                                                 'listed_ts': ct * 1000,
                                                 'listed_ago_h': (now - ct) / 3600})
                    except: pass
        except: pass
        st.session_state.listing_cache = {'ts': now, 'data': listings}
        return listings

    def fetch_onchain_whale(self, sym, s):
        min_usd = s.get('onchain_whale_min', 500_000)
        cache   = st.session_state.get('onchain_cache', {})
        now     = time.time()
        if sym in cache and now - cache[sym].get('ts', 0) < 600:
            return cache[sym]
        result = {'available': False, 'signal': 'NEUTRAL', 'detail': '',
                  'inflow': 0, 'outflow': 0, 'ts': now}
        try:
            r = requests.get("https://api.whale-alert.io/v1/transactions",
                params={"api_key": "free", "min_value": str(int(min_usd)),
                        "currency": sym.lower().replace('1000', ''),
                        "limit": "20", "start": str(int(now - 3600))}, timeout=5)
            if r.status_code == 200:
                txns    = r.json().get('transactions', [])
                inflow  = sum(t.get('amount_usd', 0) for t in txns
                              if t.get('to', {}).get('owner_type') == 'exchange')
                outflow = sum(t.get('amount_usd', 0) for t in txns
                              if t.get('from', {}).get('owner_type') == 'exchange')
                if inflow + outflow >= min_usd:
                    net = outflow - inflow
                    result = {'available': True, 'ts': now,
                              'signal': 'BULLISH' if net > 0 else 'BEARISH',
                              'detail': f"ExchIn ${inflow/1e6:.1f}M | ExchOut ${outflow/1e6:.1f}M",
                              'inflow': inflow, 'outflow': outflow}
        except: pass
        if not result['available']:
            try:
                CG = {'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin',
                      'XRP':'ripple','ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2',
                      'LINK':'chainlink','DOT':'polkadot','MATIC':'matic-network',
                      'OP':'optimism','ARB':'arbitrum','SUI':'sui','APT':'aptos',
                      'PEPE':'pepe','WIF':'dogwifcoin','TON':'the-open-network'}
                cg_id = CG.get(sym.upper(), sym.lower())
                r2 = requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization": "false", "tickers": "false",
                            "market_data": "true", "developer_data": "false"}, timeout=6)
                if r2.status_code == 200:
                    md  = r2.json().get('market_data', {})
                    vol = float(md.get('total_volume', {}).get('usd', 0) or 0)
                    mcp = float(md.get('market_cap', {}).get('usd', 0) or 0)
                    c1h = float(md.get('price_change_percentage_1h_in_currency',
                                       {}).get('usd', 0) or 0)
                    vr  = vol / mcp if mcp > 0 else 0
                    if vr > 0.08 and abs(c1h) > 0.8:
                        sig = 'BULLISH' if c1h > 0 else 'BEARISH'
                        result = {'available': True, 'ts': now, 'signal': sig,
                                  'detail': f"Vol/MCap {vr:.2f}x | 1h {c1h:+.1f}% (CG proxy)",
                                  'inflow': 0, 'outflow': 0}
            except: pass
        if 'onchain_cache' not in st.session_state:
            st.session_state.onchain_cache = {}
        st.session_state.onchain_cache[sym] = result
        return result

    def fetch_sentiment_data(self, sym_base, exch_name):
        result = {
            'top_long_pct': 50.0, 'top_short_pct': 50.0,
            'retail_long_pct': 50.0, 'taker_buy_pct': 50.0,
            'available': False, 'source': ''
        }
        try:
            if exch_name == "GATE":
                ccy = sym_base.replace("-USDT-SWAP","").replace("USDT","").replace("_USDT","")
                r1 = requests.get(
                    f"https://fx-api.gateio.ws/api/v4/futures/usdt/contract_stats",
                    params={"contract": f"{ccy}_USDT", "interval": "1h", "limit": 8},
                    timeout=4)
                if r1.status_code == 200:
                    rows = r1.json()
                    if rows:
                        latest = rows[-1]
                        lsr = float(latest.get('lsr_account', 1.0) or 1.0)
                        long_pct = (lsr / (1 + lsr)) * 100
                        result['top_long_pct'] = long_pct
                        result['top_short_pct'] = 100 - long_pct
                        result['retail_long_pct'] = long_pct
                        result['available'] = True
                        result['source'] = 'Gate'
                r2 = requests.get(
                    f"https://fx-api.gateio.ws/api/v4/futures/usdt/trades",
                    params={"contract": f"{ccy}_USDT", "limit": 100}, timeout=4)
                if r2.status_code == 200:
                    trades = r2.json()
                    buy_vol = sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0)) > 0)
                    sell_vol = sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0)) < 0)
                    total = buy_vol + sell_vol
                    if total > 0:
                        result['taker_buy_pct'] = (buy_vol / total) * 100
            elif exch_name == "OKX":
                r1 = requests.get(
                    "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
                    params={"ccy": sym_base.replace("-USDT-SWAP", "").replace("USDT", ""),
                            "period": "1H"}, timeout=4)
                if r1.status_code == 200:
                    d = r1.json()
                    rows = d.get('data', [])
                    if rows:
                        latest = rows[0]
                        ls_ratio = float(latest[1])
                        long_pct = (ls_ratio / (ls_ratio + 1)) * 100
                        result['top_long_pct'] = long_pct
                        result['top_short_pct'] = 100 - long_pct
                        result['retail_long_pct'] = long_pct
                        result['available'] = True
                        result['source'] = 'OKX'
                r2 = requests.get(
                    "https://www.okx.com/api/v5/rubik/stat/taker-volume",
                    params={"ccy": sym_base.replace("-USDT-SWAP", "").replace("USDT", ""),
                            "instType": "CONTRACTS", "period": "5m"}, timeout=4)
                if r2.status_code == 200:
                    d = r2.json()
                    rows = d.get('data', [])[:12]
                    if rows:
                        buy_vol  = sum(float(r[1]) for r in rows)
                        sell_vol = sum(float(r[2]) for r in rows)
                        total = buy_vol + sell_vol
                        if total > 0:
                            result['taker_buy_pct'] = (buy_vol / total) * 100
        except: pass
        return result

    def fetch_reddit_buzz(self, coin_sym, s):
        if not s.get('social_enabled', True):
            return {'mentions': 0, 'score': 0, 'available': False, 'source': ''}
        sym = coin_sym.upper().replace('1000','').replace('10000','')
        cache_key = sym
        now = time.time()
        cache = st.session_state.get('social_cache', {})
        if cache_key in cache and (now - cache[cache_key].get('ts', 0)) < 300:
            return cache[cache_key]
        result = {'mentions': 0, 'score': 0, 'upvote_avg': 0,
                  'available': False, 'source': '', 'sentiment': 'NEUTRAL',
                  'top_post': '', 'ts': now}
        max_pts   = s.get('social_reddit_weight', 8)
        min_ment  = s.get('social_min_mentions', 3)
        buzz_thr  = s.get('social_buzz_threshold', 10)
        try:
            subs = "CryptoCurrency+CryptoMoonShots+SatoshiStreetBets+altcoin+CryptoMarkets"
            rr = requests.get(
                f"https://www.reddit.com/r/{subs}/search.json",
                params={"q": sym, "sort": "new", "t": "hour", "limit": 25, "restrict_sr": "1"},
                headers={"User-Agent": "Mozilla/5.0 APEX/3.0"}, timeout=6)
            if rr.status_code == 200:
                posts = rr.json().get('data', {}).get('children', [])
                if posts:
                    mentions  = len(posts)
                    upvotes   = [p['data'].get('score', 0) for p in posts]
                    avg_up    = sum(upvotes) / max(1, len(upvotes))
                    bull_kw   = ['moon','pump','buy','bullish','launch','listing','breakout','gem','surge','ath']
                    bear_kw   = ['dump','crash','sell','bearish','scam','rug','dead','rekt','fraud','fail']
                    bull_h = 0; bear_h = 0
                    for p in posts:
                        txt = (p['data'].get('title','') + ' ' + p['data'].get('selftext','')).lower()
                        bull_h += sum(1 for w in bull_kw if w in txt)
                        bear_h += sum(1 for w in bear_kw if w in txt)
                    sent = ('BULLISH' if bull_h > bear_h + 1
                            else 'BEARISH' if bear_h > bull_h + 1 else 'NEUTRAL')
                    tp   = posts[0]['data'].get('title','')[:60]
                    sc   = min(max_pts, int((mentions/buzz_thr)*max_pts)) if mentions >= min_ment else 0
                    result = {'mentions': mentions, 'score': sc, 'upvote_avg': avg_up,
                              'available': True, 'source': 'Reddit', 'sentiment': sent,
                              'top_post': tp, 'ts': now}
        except: pass
        if not result['available']:
            try:
                cg_map = {
                    'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin',
                    'XRP':'ripple','ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2',
                    'LINK':'chainlink','DOT':'polkadot','MATIC':'matic-network','LTC':'litecoin',
                    'UNI':'uniswap','ATOM':'cosmos','XLM':'stellar','ALGO':'algorand',
                    'NEAR':'near','APT':'aptos','ARB':'arbitrum','OP':'optimism',
                    'INJ':'injective-protocol','SUI':'sui','TIA':'celestia','PEPE':'pepe',
                    'SHIB':'shiba-inu','WIF':'dogwifhat','FIL':'filecoin','FLOKI':'floki',
                    'SEI':'sei-network','JTO':'jito-governance-token','PYTH':'pyth-network',
                    'BONK':'bonk','MEME':'memecoin','ORDI':'ordinals','SATS':'1000-sats-ordinals',
                }
                cg_id = cg_map.get(sym, sym.lower())
                cg = requests.get(
                    f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization":"false","tickers":"false","market_data":"true",
                            "community_data":"true","developer_data":"false"}, timeout=6)
                if cg.status_code == 200:
                    data = cg.json()
                    cd   = data.get('community_data', {})
                    md   = data.get('market_data', {})
                    subs_count = cd.get('reddit_subscribers', 0) or 0
                    active  = cd.get('reddit_accounts_active_48h', 0) or 0
                    tw_foll = cd.get('twitter_followers', 0) or 0
                    chg_1h  = md.get('price_change_percentage_1h_in_currency', {}).get('usd', 0) or 0
                    sent = ('BULLISH' if chg_1h > 1.5
                            else 'BEARISH' if chg_1h < -1.5 else 'NEUTRAL')
                    mentions_proxy = min(50, int(active / 50)) if active else (2 if subs_count > 50000 else 0)
                    sc   = min(max_pts, int((mentions_proxy/buzz_thr)*max_pts)) if mentions_proxy >= min_ment else 0
                    top  = (f"r/ {subs_count:,} subs | {active:,} active 48h"
                            + (f" | Twitter {tw_foll:,}" if tw_foll else "")
                            + (f" | 1h chg {chg_1h:+.2f}%" if chg_1h else ""))
                    result = {'mentions': mentions_proxy, 'score': sc, 'upvote_avg': 0,
                              'available': True, 'source': 'CoinGecko', 'sentiment': sent,
                              'top_post': top[:80], 'ts': now}
            except: pass
        if not result['available'] and s.get('apify_token'):
            try:
                r3 = requests.post(
                    "https://api.apify.com/v2/acts/trudax~reddit-scraper-lite/run-sync-get-dataset-items",
                    json={"searches":[{"term":sym,"sort":"new","time":"hour"}],"maxItems":20},
                    headers={"Authorization": f"Bearer {s['apify_token']}"}, timeout=15)
                if r3.status_code == 200:
                    items    = r3.json()
                    mentions = len(items)
                    avg_up   = sum(i.get('score',0) for i in items) / max(1, mentions)
                    sc       = min(max_pts, int((mentions/buzz_thr)*max_pts)) if mentions >= min_ment else 0
                    result   = {'mentions': mentions, 'score': sc, 'upvote_avg': avg_up,
                                'available': True, 'source': 'Apify/Reddit',
                                'sentiment': 'NEUTRAL', 'top_post': '', 'ts': now}
            except: pass
        if 'social_cache' not in st.session_state:
            st.session_state.social_cache = {}
        st.session_state.social_cache[cache_key] = result
        return result

    def cmc_data(self, sym):
        if not self.cmc_key: return None
        try:
            r = requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                headers={"X-CMC_PRO_API_KEY":self.cmc_key},
                params={"symbol":sym.replace("1000","")},timeout=3)
            if r.status_code==200:
                d=r.json()['data']; coin=d[list(d.keys())[0]]; q=coin['quote']['USD']
                return {'rank':coin.get('cmc_rank',9999),'mcap':q.get('market_cap') or 0,
                        'vol24':q.get('volume_24h') or 0,'change24':q.get('percent_change_24h') or 0}
        except: pass
        return None

    def ob_score_calc(self, bids, asks, sig, s):
        try:
            def valid(levels):
                out = []
                for lv in levels:
                    try:
                        p,q = float(lv[0]),float(lv[1])
                        if p>0 and q>0: out.append((p,q))
                    except: pass
                return out
            b = valid(bids); a = valid(asks)
            if not b or not a: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}
            bv=sum(p*q for p,q in b); av=sum(p*q for p,q in a)
            tot=bv+av; bid_pct=(bv/tot*100) if tot>0 else 50
            ratio=bv/av if av>0 else 1.0
            wb=max(b,key=lambda x:x[0]*x[1]); wa=max(a,key=lambda x:x[0]*x[1])
            wbv=wb[0]*wb[1]; wav=wa[0]*wa[1]
            score=0; msg=""
            if sig=="LONG":
                r=ratio
                if r>=s['ob_ratio_high']:   score=s['pts_ob_high']; msg=f"⚖️ OB {r:.2f}× bid-heavy — strong buy pressure ({bid_pct:.0f}% bid)"
                elif r>=s['ob_ratio_mid']:  score=s['pts_ob_mid'];  msg=f"⚖️ OB {r:.2f}× bid-heavy ({bid_pct:.0f}% bid)"
                elif r>=s['ob_ratio_low']:  score=s['pts_ob_low'];  msg=f"⚖️ OB {r:.2f}× slight bid pressure ({bid_pct:.0f}% bid)"
            else:
                inv=1/ratio if ratio>0 else 1
                if inv>=s['ob_ratio_high']:  score=s['pts_ob_high']; msg=f"⚖️ OB {inv:.2f}× ask-heavy — strong sell pressure ({100-bid_pct:.0f}% ask)"
                elif inv>=s['ob_ratio_mid']: score=s['pts_ob_mid'];  msg=f"⚖️ OB {inv:.2f}× ask-heavy ({100-bid_pct:.0f}% ask)"
                elif inv>=s['ob_ratio_low']: score=s['pts_ob_low'];  msg=f"⚖️ OB {inv:.2f}× slight ask pressure"
            return score, msg, {'bid_pct':bid_pct,'ratio':ratio,'whale_bid_val':wbv,'whale_ask_val':wav,'whale_bid_px':wb[0],'whale_ask_px':wa[0]}
        except: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}

    def find_whale_walls(self, bids, asks, price, sig, s):
        wmin = s['whale_min_usdt']
        whale_sc = 0
        whale_details = []
        reasons_out = []
        def parse_walls(levels, side):
            walls = []
            for lv in levels:
                try:
                    p, q = float(lv[0]), float(lv[1])
                    val = p * q
                    if val >= wmin:
                        dist = abs(p - price) / price * 100
                        walls.append({'price': p, 'value': val, 'dist_pct': dist, 'side': side})
                except: pass
            return sorted(walls, key=lambda x: x['value'], reverse=True)
        bid_walls = parse_walls(bids, 'BID')
        ask_walls = parse_walls(asks, 'ASK')
        near = s.get('pts_whale_near', 25)
        mid  = s.get('pts_whale_mid', 15)
        far  = s.get('pts_whale_far', 8)
        if sig == "LONG" and bid_walls:
            w = bid_walls[0]
            if w['dist_pct'] <= 0.5:   whale_sc = near
            elif w['dist_pct'] <= 1.5: whale_sc = mid
            elif w['dist_pct'] <= 3.0: whale_sc = far
            if whale_sc > 0:
                whale_details.append({'side': 'BUY', 'value': w['value'], 'price': w['price'], 'dist_pct': w['dist_pct']})
                reasons_out.append(f"🐋 BUY WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% below)")
            if len(bid_walls) >= 3:
                whale_sc += 5
                reasons_out.append(f"🐋 {len(bid_walls)} stacked bid walls — layered buy support")
        elif sig == "SHORT" and ask_walls:
            w = ask_walls[0]
            if w['dist_pct'] <= 0.5:   whale_sc = near
            elif w['dist_pct'] <= 1.5: whale_sc = mid
            elif w['dist_pct'] <= 3.0: whale_sc = far
            if whale_sc > 0:
                whale_details.append({'side': 'SELL', 'value': w['value'], 'price': w['price'], 'dist_pct': w['dist_pct']})
                reasons_out.append(f"🐋 SELL WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% above)")
            if len(ask_walls) >= 3:
                whale_sc += 5
                reasons_out.append(f"🐋 {len(ask_walls)} stacked ask walls — layered sell resistance")
        whale_str = ""
        if whale_details:
            w = whale_details[0]
            whale_str = f"{'BUY' if w['side']=='BUY' else 'SELL'} {fmt(w['value'])} @ ${w['price']:.6f}"
        return whale_sc, whale_str, whale_details, reasons_out

    async def analyze(self, exch_name, exch_obj, sym, s, btc_trend):
        dsym = sym.split(':')[0].replace('/USDT','')
        if s['cooldown_on'] and is_on_cooldown(dsym, s['cooldown_hrs']): return None

        df_f = await self.fetch_ohlcv(exch_obj, sym, s['fast_tf'], 200)
        df_slow = await self.fetch_ohlcv(exch_obj, sym, s['slow_tf'], 100)
        fi, tick, ob = await self.safe_fetch(exch_obj, sym)
        oi = await self.fetch_oi(exch_obj, sym)

        if df_f.empty or df_slow.empty: return None
        price = float(tick.get('last',0) or 0)
        if price <= 0: return None

        # ─── Indicators ───────────────────────────────────────────────────
        try:
            df_slow.ta.ema(length=50,append=True)
            df_f.ta.rsi(length=14,append=True)
            df_f.ta.atr(length=14,append=True)
            df_f.ta.macd(fast=12,slow=26,signal=9,append=True)
            df_f.ta.bbands(length=20,std=2,append=True)

            e50_cols=[c for c in df_slow.columns if 'EMA_50' in c or ('EMA' in c and '50' in c)]
            rsi_cols=[c for c in df_f.columns if 'RSI_14' in c or ('RSI' in c)]
            atr_cols=[c for c in df_f.columns if 'ATRr_14' in c or ('ATRr' in c)]
            macd_cols=[c for c in df_f.columns if c.startswith('MACD_') and not c.startswith('MACDh') and not c.startswith('MACDs')]
            macds_cols=[c for c in df_f.columns if c.startswith('MACDs_')]
            bbl_cols=[c for c in df_f.columns if c.startswith('BBL_')]
            bbu_cols=[c for c in df_f.columns if c.startswith('BBU_')]

            if not e50_cols or not rsi_cols or not atr_cols: return None

            e50   = float(df_slow[e50_cols[0]].iloc[-1])
            rsi   = float(df_f[rsi_cols[0]].iloc[-1])
            atr   = float(df_f[atr_cols[0]].iloc[-1])
            macd  = float(df_f[macd_cols[0]].iloc[-1]) if macd_cols else 0
            macds = float(df_f[macds_cols[0]].iloc[-1]) if macds_cols else 0
            bbl   = float(df_f[bbl_cols[0]].iloc[-1]) if bbl_cols else price*0.97
            bbu   = float(df_f[bbu_cols[0]].iloc[-1]) if bbu_cols else price*1.03
            vma   = float(df_f['volume'].rolling(20).mean().iloc[-1])
            lvol  = float(df_f['volume'].iloc[-1])
            if vma == 0: vma = 1

            # RSI series for direction tracking
            rsi_series_raw = df_f[rsi_cols[0]].dropna().values
        except:
            return None

        sig = "LONG" if float(df_slow['close'].iloc[-1]) > e50 else "SHORT"
        if s['btc_filter'] and btc_trend=="BEARISH" and sig=="LONG": return None

        # ─── ACCURACY GATE 1: Minimum liquidity filter ───────────────────────
        qv_now = float(tick.get('quoteVolume', 0) or 0)
        min_vol = s.get('min_vol_filter', 300_000)
        if min_vol > 0 and qv_now < min_vol: return None

        # ─── ACCURACY GATE 2: ATR / volatility sanity check ─────────────────
        atr_pct = (atr / price * 100) if price > 0 else 0
        if atr_pct < s.get('atr_min_pct', 0.2): return None
        if atr_pct > s.get('atr_max_pct', 10.0): return None

        # ─── ACCURACY GATE 3: Bid-ask spread filter ──────────────────────────
        spread_max = s.get('spread_max_pct', 0.5)
        if spread_max > 0:
            bids_raw = ob.get('bids', []); asks_raw = ob.get('asks', [])
            if bids_raw and asks_raw:
                best_bid = float(bids_raw[0][0]); best_ask = float(asks_raw[0][0])
                if best_bid > 0:
                    spread_pct = (best_ask - best_bid) / best_bid * 100
                    if spread_pct > spread_max: return None

        pump_score=0; reasons=[]; bd={}
        # Track warnings for card display
        warnings = []

        # ═══════════════════════════════════════════════════════════════════
        # NEW IMPROVEMENT 1: "LATE TO THE PARTY" DETECTOR
        # ═══════════════════════════════════════════════════════════════════
        try:
            pc20 = ((price - float(df_f['close'].iloc[-20])) / float(df_f['close'].iloc[-20])) * 100
        except: pc20 = 0

        late_entry_penalty = 0
        late_entry_flag = False
        late_chg_thresh = s.get('late_entry_chg_thresh', 8.0)
        if abs(pc20) > late_chg_thresh and sig == "LONG":
            # Find which candle caused the main move
            closes = df_f['close'].values
            try:
                base_price = float(df_f['close'].iloc[-21]) if len(df_f) >= 21 else float(df_f['close'].iloc[0])
                # Find the candle index where we crossed the threshold
                crossed_at = None
                for ci in range(len(closes) - 1, max(len(closes) - 25, 0), -1):
                    chg_from_base = abs((float(closes[ci]) - base_price) / base_price * 100)
                    if chg_from_base < late_chg_thresh * 0.5:
                        crossed_at = ci
                        break
                candles_since_move = (len(closes) - 1) - (crossed_at or (len(closes) - 15))
                if candles_since_move > 10:
                    late_entry_penalty = s.get('late_entry_penalty', 20)
                    late_entry_flag = True
                    warnings.append(f"⚠️ LATE ENTRY: +{pc20:.1f}% move started {candles_since_move} candles ago — entry window likely closed")
            except: pass
        elif abs(pc20) > late_chg_thresh and sig == "SHORT":
            # For shorts: big drop that happened long ago = rally might follow
            try:
                base_price = float(df_f['close'].iloc[-21]) if len(df_f) >= 21 else float(df_f['close'].iloc[0])
                crossed_at = None
                closes = df_f['close'].values
                for ci in range(len(closes) - 1, max(len(closes) - 25, 0), -1):
                    chg_from_base = abs((float(closes[ci]) - base_price) / base_price * 100)
                    if chg_from_base < late_chg_thresh * 0.5:
                        crossed_at = ci
                        break
                candles_since_move = (len(closes) - 1) - (crossed_at or (len(closes) - 15))
                if candles_since_move > 10:
                    late_entry_penalty = s.get('late_entry_penalty', 20)
                    late_entry_flag = True
                    warnings.append(f"⚠️ LATE SHORT: -{abs(pc20):.1f}% drop was {candles_since_move} candles ago — dead-cat bounce risk")
            except: pass
        pump_score -= late_entry_penalty
        bd['late_entry_penalty'] = -late_entry_penalty

        # ═══════════════════════════════════════════════════════════════════
        # NEW IMPROVEMENT 2: VOLUME EXHAUSTION CHECK
        # ═══════════════════════════════════════════════════════════════════
        vol_exhaust_penalty = 0
        vol_exhaust_flag = False
        try:
            vol5_avg = float(df_f['volume'].rolling(5).mean().iloc[-1])
            high20   = float(df_f['high'].rolling(20).max().iloc[-1])
            price_near_top = (high20 > 0 and abs(price - high20) / high20 * 100 < 3.0)
            vol_dropping   = (vol5_avg > 0 and lvol < vol5_avg * 0.60)
            if price_near_top and vol_dropping and sig == "LONG":
                vol_exhaust_penalty = s.get('vol_exhaust_penalty', 15)
                vol_exhaust_flag = True
                ratio_display = lvol / vol5_avg if vol5_avg > 0 else 0
                warnings.append(f"⚠️ DISTRIBUTION PHASE: Volume {ratio_display:.1f}× avg near 20c high — smart money selling into rally")
        except: pass
        pump_score -= vol_exhaust_penalty
        bd['vol_exhaust_penalty'] = -vol_exhaust_penalty

        # ═══════════════════════════════════════════════════════════════════
        # NEW IMPROVEMENT 3: RSI DIRECTION (rising vs falling matters more than level)
        # ═══════════════════════════════════════════════════════════════════
        rsi_direction = 'FLAT'
        rsi_dir_pts = 0
        rsi_dir_reason = ""
        try:
            if len(rsi_series_raw) >= 4:
                rsi_last3 = rsi_series_raw[-3:]
                rsi_prev3 = rsi_series_raw[-6:-3] if len(rsi_series_raw) >= 6 else rsi_series_raw[:3]
                rsi_now_avg  = float(rsi_last3.mean())
                rsi_prev_avg = float(rsi_prev3.mean())
                rsi_slope = rsi_now_avg - rsi_prev_avg  # positive = rising, negative = falling

                if sig == "LONG":
                    if rsi_slope > 3:
                        rsi_direction = 'RISING'
                        rsi_dir_pts = 8
                        rsi_dir_reason = f"📈 RSI rising ({rsi_prev_avg:.1f} → {rsi_now_avg:.1f}) — momentum building UP"
                    elif rsi_slope < -3:
                        rsi_direction = 'FALLING'
                        rsi_dir_pts = -8  # penalize: RSI falling on a LONG = momentum fading
                        rsi_dir_reason = f"⚠️ RSI declining ({rsi_prev_avg:.1f} → {rsi_now_avg:.1f}) — LONG momentum fading"
                        if rsi_slope < -8:
                            warnings.append(rsi_dir_reason)
                else:  # SHORT
                    if rsi_slope < -3:
                        rsi_direction = 'FALLING'
                        rsi_dir_pts = 8
                        rsi_dir_reason = f"📉 RSI falling ({rsi_prev_avg:.1f} → {rsi_now_avg:.1f}) — momentum building DOWN"
                    elif rsi_slope > 3:
                        rsi_direction = 'RISING'
                        rsi_dir_pts = -8  # penalize: RSI rising on a SHORT = momentum fading
                        rsi_dir_reason = f"⚠️ RSI rising ({rsi_prev_avg:.1f} → {rsi_now_avg:.1f}) — SHORT momentum fading"
                        if rsi_slope > 8:
                            warnings.append(rsi_dir_reason)
        except: pass
        if rsi_dir_pts != 0:
            pump_score += rsi_dir_pts
            if rsi_dir_reason and rsi_dir_pts > 0:
                reasons.append(rsi_dir_reason)
        bd['rsi_direction'] = rsi_dir_pts

        # ═══════════════════════════════════════════════════════════════════
        # NEW IMPROVEMENT 4: "NEAR LOCAL TOP" PENALTY
        # ═══════════════════════════════════════════════════════════════════
        near_top_penalty = 0
        near_top_flag = False
        try:
            high20_val = float(df_f['high'].rolling(20).max().iloc[-1])
            if high20_val > 0:
                dist_from_top = abs(price - high20_val) / high20_val * 100
                # Check if MACD and RSI are both declining
                macd_declining = False
                rsi_declining  = False
                if macd_cols and len(df_f) >= 3:
                    macd_vals = df_f[macd_cols[0]].values
                    macd_declining = float(macd_vals[-1]) < float(macd_vals[-2]) < float(macd_vals[-3])
                if len(rsi_series_raw) >= 3:
                    rsi_declining = float(rsi_series_raw[-1]) < float(rsi_series_raw[-2]) < float(rsi_series_raw[-3])
                if dist_from_top < 3.0 and macd_declining and rsi_declining and sig == "LONG":
                    near_top_penalty = s.get('near_top_penalty', 15)
                    near_top_flag = True
                    warnings.append(f"⚠️ NEAR LOCAL TOP: Price {dist_from_top:.1f}% from 20c high, MACD+RSI declining — distribution risk")
        except: pass
        pump_score -= near_top_penalty
        bd['near_top_penalty'] = -near_top_penalty

        # 1 ── ORDER BOOK ─────────────────────────────────────────────────────
        ob_sc, ob_msg, ob_data = self.ob_score_calc(ob.get('bids',[]),ob.get('asks',[]),sig,s)
        pump_score+=ob_sc; bd['ob_imbalance']=ob_sc
        if ob_msg: reasons.append(ob_msg)

        # 2 ── WHALE WALLS ────────────────────────────────────────────────────
        whale_sc, whale_str, whale_details, whale_reasons = self.find_whale_walls(
            ob.get('bids',[]), ob.get('asks',[]), price, sig, s)
        pump_score += whale_sc
        bd['whale_wall'] = whale_sc
        reasons.extend(whale_reasons)

        # 3 ── FUNDING RATE ───────────────────────────────────────────────────
        fr=float(fi.get('fundingRate',0) or 0)
        fr_sc=0
        if sig=="LONG":
            if fr<=-s['funding_high']:     fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme neg funding {fr*100:.4f}% — shorts will be squeezed")
            elif fr<=-s['funding_mid']:    fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong neg funding {fr*100:.4f}% — squeeze building")
            elif fr<=-s['funding_low']:    fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Neg funding {fr*100:.4f}% — mild short pressure")
            elif fr<0:                     fr_sc=3;                     reasons.append(f"⚡ Slightly neg funding {fr*100:.4f}%")
        else:
            if fr>=s['funding_high']:      fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme pos funding {fr*100:.4f}% — longs will be squeezed")
            elif fr>=s['funding_mid']:     fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong pos funding {fr*100:.4f}% — squeeze building")
            elif fr>=s['funding_low']:     fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Pos funding {fr*100:.4f}%")
            elif fr>0:                     fr_sc=3;                     reasons.append(f"⚡ Slightly pos funding {fr*100:.4f}%")
        pump_score+=fr_sc; bd['funding']=fr_sc

        # 4 ── FUNDING HISTORY ────────────────────────────────────────────────
        funding_history = await self.fetch_funding_history(exch_obj, sym)
        funding_hist_sc = 0

        # ═══════════════════════════════════════════════════════════════════
        # NEW IMPROVEMENT 5: FUNDING AGE CONTEXT
        # Check if funding was negative BEFORE or AFTER the price surge
        # Negative funding AFTER a pump = shorts may be right, penalize
        # ═══════════════════════════════════════════════════════════════════
        funding_age_penalty = 0
        if len(funding_history) >= 6 and abs(pc20) > 5.0:
            try:
                # If price surged AND funding turned negative after
                # Look at early funding periods vs late ones
                early_funding = funding_history[:6]   # older periods
                late_funding  = funding_history[-6:]  # recent periods
                avg_early = sum(early_funding) / len(early_funding) if early_funding else 0
                avg_late  = sum(late_funding)  / len(late_funding)  if late_funding  else 0
                if sig == "LONG" and pc20 > 5.0:
                    # Negative funding that DEVELOPED after the pump = shorts fading the pump correctly
                    if avg_early > -0.0001 and avg_late < -0.0003:
                        funding_age_penalty = 10
                        warnings.append(f"⚠️ FUNDING TRAP: Negative funding appeared AFTER +{pc20:.1f}% pump — shorts may be right, not squeeze fuel")
                    # True squeeze setup: funding was negative BEFORE the price moved
                    elif avg_early < -0.0003 and avg_late < -0.0001:
                        funding_hist_sc += 8
                        reasons.append(f"⚡ Funding negative BEFORE price surge — classic true squeeze setup")
            except: pass

        if len(funding_history) >= 6:
            last_6 = funding_history[-6:]
            if sig == "LONG" and all(r < 0 for r in last_6) and funding_age_penalty == 0:
                funding_hist_sc = 12
                reasons.append(f"⚡ Funding negative for 6+ consecutive periods — deep squeeze setup")
            elif sig == "SHORT" and all(r > 0 for r in last_6):
                funding_hist_sc = 12
                reasons.append(f"⚡ Funding positive for 6+ consecutive periods — prolonged long squeeze")
            elif sig == "LONG" and sum(1 for r in last_6 if r < 0) >= 4 and funding_age_penalty == 0:
                funding_hist_sc = 6
                reasons.append(f"⚡ Funding mostly negative last 6 periods — squeeze building")
            elif sig == "SHORT" and sum(1 for r in last_6 if r > 0) >= 4:
                funding_hist_sc = 6
                reasons.append(f"⚡ Funding mostly positive last 6 periods — longs trapped")

        pump_score += funding_hist_sc - funding_age_penalty
        bd['funding_hist'] = funding_hist_sc
        bd['funding_age_penalty'] = -funding_age_penalty

        # 5 ── OPEN INTEREST ──────────────────────────────────────────────────
        oi_sc=0
        oi_history = await self.fetch_oi_history(exch_obj, sym)
        oi_change_6h = 0.0
        if len(oi_history) >= 6 and oi_history[-6] > 0:
            oi_change_6h = (oi_history[-1] - oi_history[-6]) / oi_history[-6] * 100

        if oi > 0:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:
                oi_sc=s['pts_oi_high']; reasons.append(f"📈 OI + price up {pc20:.1f}% — confirmed accumulation")
            elif sig=="LONG" and pc20>=s['oi_price_chg_low']:
                oi_sc=s['pts_oi_low'];  reasons.append(f"📈 OI growing, price +{pc20:.1f}%")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']:
                oi_sc=s['pts_oi_high']; reasons.append(f"📉 OI + price down {pc20:.1f}% — confirmed distribution")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_low']:
                oi_sc=s['pts_oi_low'];  reasons.append(f"📉 OI building on drop {pc20:.1f}%")
            if abs(oi_change_6h) >= 20:
                oi_sc += 10; reasons.append(f"📈 OI spiked {oi_change_6h:+.1f}% in last 6h — new money entering NOW")
            elif abs(oi_change_6h) >= 10:
                oi_sc += 5; reasons.append(f"📈 OI up {oi_change_6h:+.1f}% in 6h")
        else:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:
                oi_sc=8; reasons.append(f"📈 Price up {pc20:.1f}% over 20 candles (OI unavailable)")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']:
                oi_sc=8; reasons.append(f"📉 Price down {pc20:.1f}% over 20 candles (OI unavailable)")
        pump_score+=oi_sc; bd['oi_spike']=oi_sc

        # 6 ── VOLUME SURGE ───────────────────────────────────────────────────
        vsurge=lvol/vma
        v_sc=0
        explosive_thresh = s.get('vol_surge_explosive', 5.0)
        if vsurge >= explosive_thresh:
            v_sc = s.get('pts_vol_explosive', 20)
            reasons.append(f"🚀 EXPLOSIVE volume {vsurge:.1f}×avg — institutional move NOW")
        elif vsurge>=s['vol_surge_high']:   v_sc=s['pts_vol_high']; reasons.append(f"🔥 Volume {vsurge:.1f}×avg — major activity NOW")
        elif vsurge>=s['vol_surge_mid']:    v_sc=s['pts_vol_mid'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — elevated")
        elif vsurge>=s['vol_surge_low']:    v_sc=s['pts_vol_low'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — above normal")
        pump_score+=v_sc; bd['vol_surge']=v_sc

        # 7 ── LIQUIDATION CLUSTERS ───────────────────────────────────────────
        liq_sc=0; liq_target=0; liq_detail=""
        try:
            top3=df_f.nlargest(3,'volume')
            for _,row in top3.iterrows():
                mid=(float(row['high'])+float(row['low']))/2
                dist=abs(price-mid)/price*100
                target=mid*1.02 if sig=="LONG" else mid*0.98
                if dist<=s['liq_cluster_near']:
                    liq_sc=s['pts_liq_near']; liq_target=target; liq_detail=f"${target:.6f}"
                    reasons.append(f"🧲 Liq cluster {dist:.1f}% away @ ${mid:.5f} — price magnet"); break
                elif dist<=s['liq_cluster_mid'] and liq_sc<s['pts_liq_mid']:
                    liq_sc=s['pts_liq_mid']
                elif dist<=s['liq_cluster_far'] and liq_sc<s['pts_liq_far']:
                    liq_sc=s['pts_liq_far']
        except: pass
        pump_score+=liq_sc; bd['liq_cluster']=liq_sc

        # 8 ── 24H VOLUME ─────────────────────────────────────────────────────
        cmc=self.cmc_data(dsym); vm_sc=0; vol_mcap_ratio=0
        if cmc and cmc.get('mcap',0)>0:
            vol_mcap_ratio=cmc['vol24']/cmc['mcap']
            if vol_mcap_ratio>1.0:    vm_sc=s['pts_vol24_high']+3; reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}× — extreme vs float")
            elif vol_mcap_ratio>0.5:  vm_sc=s['pts_vol24_high'];   reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}× — very high")
            elif vol_mcap_ratio>0.15: vm_sc=s['pts_vol24_mid'];    reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}×")
            elif vol_mcap_ratio>0.05: vm_sc=s['pts_vol24_low'];    reasons.append(f"🌐 Vol/MCap {vol_mcap_ratio:.2f}×")
        else:
            qv=float(tick.get('quoteVolume',0) or 0)
            if qv>=s['vol24h_high']:   vm_sc=s['pts_vol24_high']; reasons.append(f"📊 24h vol {fmt(qv)} — high activity")
            elif qv>=s['vol24h_mid']:  vm_sc=s['pts_vol24_mid'];  reasons.append(f"📊 24h vol {fmt(qv)}")
            elif qv>=s['vol24h_low']:  vm_sc=s['pts_vol24_low'];  reasons.append(f"📊 24h vol {fmt(qv)}")
        pump_score+=vm_sc; bd['vol_mcap']=vm_sc

        # 9 ── TECHNICALS ─────────────────────────────────────────────────────
        tech_sc=0
        if sig=="LONG" and macd>macds:    tech_sc+=s['pts_macd']; reasons.append("📊 MACD bullish cross")
        elif sig=="SHORT" and macd<macds:  tech_sc+=s['pts_macd']; reasons.append("📊 MACD bearish cross")
        if sig=="LONG" and rsi<s['rsi_oversold']:     tech_sc+=s['pts_rsi']; reasons.append(f"📉 RSI oversold {rsi:.1f}")
        elif sig=="SHORT" and rsi>s['rsi_overbought']: tech_sc+=s['pts_rsi']; reasons.append(f"📈 RSI overbought {rsi:.1f}")
        if sig=="LONG" and float(df_f['close'].iloc[-1])<=bbl:    tech_sc+=s['pts_bb']; reasons.append("🎯 Price at lower BB")
        elif sig=="SHORT" and float(df_f['close'].iloc[-1])>=bbu:  tech_sc+=s['pts_bb']; reasons.append("🎯 Price at upper BB")
        tech_sc+=s['pts_ema']; reasons.append(f"✅ EMA50 trend: {sig}")

        # 9b ── RSI DIVERGENCE ────────────────────────────────────────────────
        try:
            close_series = df_f['close']
            if len(rsi_series_raw) >= 10 and len(close_series) >= 10:
                rsi_arr   = rsi_series_raw[-10:]
                close_arr = close_series.values[-10:]
                p_low_now  = min(close_arr[-3:]);   p_low_prev  = min(close_arr[:5])
                r_low_now  = min(rsi_arr[-3:]);     r_low_prev  = min(rsi_arr[:5])
                p_high_now = max(close_arr[-3:]);   p_high_prev = max(close_arr[:5])
                r_high_now = max(rsi_arr[-3:]);     r_high_prev = max(rsi_arr[:5])
                div_pts = s.get('pts_divergence', 10)
                if sig == "LONG":
                    if p_low_now < p_low_prev and r_low_now > r_low_prev:
                        tech_sc += div_pts
                        reasons.append(f"📐 Bullish RSI divergence — price lower low, RSI higher low (reversal signal)")
                    elif p_low_now > p_low_prev and r_low_now < r_low_prev:
                        tech_sc += div_pts // 2
                        reasons.append(f"📐 Hidden bullish divergence — trend continuation likely")
                elif sig == "SHORT":
                    if p_high_now > p_high_prev and r_high_now < r_high_prev:
                        tech_sc += div_pts
                        reasons.append(f"📐 Bearish RSI divergence — price higher high, RSI lower high (reversal signal)")
                    elif p_high_now < p_high_prev and r_high_now > r_high_prev:
                        tech_sc += div_pts // 2
                        reasons.append(f"📐 Hidden bearish divergence — downtrend continuation likely")
        except: pass

        # 9c ── CANDLE PATTERN ────────────────────────────────────────────────
        try:
            pat_pts = s.get('pts_candle_pattern', 8)
            c0 = df_f.iloc[-1]; c1 = df_f.iloc[-2]; c2 = df_f.iloc[-3]
            body0 = float(c0['close']) - float(c0['open'])
            body1 = float(c1['close']) - float(c1['open'])
            rng0  = float(c0['high'])  - float(c0['low'])
            rng1  = float(c1['high'])  - float(c1['low'])
            if rng0 > 0 and rng1 > 0:
                if sig == "LONG":
                    lower_wick = min(float(c0['open']), float(c0['close'])) - float(c0['low'])
                    if lower_wick / rng0 > 0.6 and abs(body0) / rng0 < 0.3:
                        tech_sc += pat_pts
                        reasons.append("🔨 Hammer candle at support — buyers defended strongly")
                    elif body0 > 0 and body1 < 0 and abs(body0) > abs(body1) * 1.1:
                        tech_sc += pat_pts
                        reasons.append("🕯️ Bullish engulfing — full reversal candle")
                    elif (float(c0['close']) > float(c0['open']) and
                          float(c1['close']) > float(c1['open']) and
                          float(c2['close']) > float(c2['open'])):
                        if float(df_f['volume'].iloc[-1]) > float(df_f['volume'].iloc[-2]) > float(df_f['volume'].iloc[-3]):
                            tech_sc += pat_pts
                            reasons.append("📈 3 consecutive green candles with rising volume — strong accumulation")
                else:
                    upper_wick = float(c0['high']) - max(float(c0['open']), float(c0['close']))
                    if upper_wick / rng0 > 0.6 and abs(body0) / rng0 < 0.3:
                        tech_sc += pat_pts
                        reasons.append("🌠 Shooting star — sellers rejected rally hard")
                    elif body0 < 0 and body1 > 0 and abs(body0) > abs(body1) * 1.1:
                        tech_sc += pat_pts
                        reasons.append("🕯️ Bearish engulfing — full reversal candle")
                    elif (float(c0['close']) < float(c0['open']) and
                          float(c1['close']) < float(c1['open']) and
                          float(c2['close']) < float(c2['open'])):
                        if float(df_f['volume'].iloc[-1]) > float(df_f['volume'].iloc[-2]) > float(df_f['volume'].iloc[-3]):
                            tech_sc += pat_pts
                            reasons.append("📉 3 consecutive red candles with rising volume — strong distribution")
        except: pass

        pump_score+=tech_sc; bd['technicals']=tech_sc

        # 10 ── SESSION ────────────────────────────────────────────────────────
        sname,smult,_=get_session()
        ses_sc=0
        if smult>=1.4: ses_sc=s['pts_session']; reasons.append(f"⏰ {sname} — peak session")
        elif smult>=1.3: ses_sc=max(1,s['pts_session']//2); reasons.append(f"⏰ {sname}")
        pump_score+=ses_sc; bd['session']=ses_sc

        # 11 ── MOMENTUM ──────────────────────────────────────────────────────
        candle_body = float(df_f['close'].iloc[-1]) - float(df_f['open'].iloc[-1])
        recent_momentum = float(df_f['close'].iloc[-1]) - float(df_f['close'].iloc[-3])
        momentum_confirmed = False
        mom_sc = 0
        if sig == "LONG" and candle_body > 0 and recent_momentum > 0:
            momentum_confirmed = True
            mom_sc = 8
            reasons.append(f"✅ Momentum confirmed: price moving UP last 3 candles ({recent_momentum/price*100:+.2f}%)")
        elif sig == "SHORT" and candle_body < 0 and recent_momentum < 0:
            momentum_confirmed = True
            mom_sc = 8
            reasons.append(f"✅ Momentum confirmed: price moving DOWN last 3 candles ({recent_momentum/price*100:+.2f}%)")
        pump_score += mom_sc
        bd['momentum'] = mom_sc

        if s.get('require_momentum', False) and not momentum_confirmed:
            return None

        # 11b ── MULTI-TIMEFRAME CONFIRMATION ─────────────────────────────────
        mtf_sc = 0
        if s.get('mtf_confirm', True):
            try:
                med_tf = "1h"
                df_med = await self.fetch_ohlcv(exch_obj, sym, med_tf, 60)
                if not df_med.empty:
                    df_med.ta.ema(length=50, append=True)
                    ema_cols_med = [c for c in df_med.columns if 'EMA_50' in c]
                    if ema_cols_med:
                        e50_med   = float(df_med[ema_cols_med[0]].iloc[-1])
                        close_med = float(df_med['close'].iloc[-1])
                        med_trend = "LONG" if close_med > e50_med else "SHORT"
                        fast_trend = "LONG" if candle_body > 0 else "SHORT"
                        aligned = sum(1 for t in [sig, med_trend, fast_trend] if t == sig)
                        if aligned == 3:
                            mtf_sc = s.get('pts_mtf', 12)
                            reasons.append(f"🎯 All 3 timeframes aligned {sig} — {s['fast_tf']}+1h+{s['slow_tf']} confirmation")
                        elif aligned == 2:
                            mtf_sc = s.get('pts_mtf', 12) // 2
                            reasons.append(f"🎯 2/3 timeframes aligned {sig}")
                        else:
                            mtf_sc = -5
                            reasons.append(f"⚠️ Timeframe conflict — {s['fast_tf']} vs 1h vs {s['slow_tf']} disagree")
            except: pass
        pump_score += mtf_sc
        bd['mtf'] = mtf_sc

        # 12 ── SENTIMENT ─────────────────────────────────────────────────────
        sentiment = {'top_long_pct':50,'top_short_pct':50,'retail_long_pct':50,'taker_buy_pct':50,'available':False,'source':''}
        sent_sc = 0
        if exch_name in ("GATE", "OKX"):
            if exch_name == "OKX":
                sym_base = dsym + "-USDT-SWAP"
            else:
                sym_base = dsym + "_USDT"
            sentiment = self.fetch_sentiment_data(sym_base, exch_name)
            if sentiment['available']:
                pts_sent = s.get('pts_sentiment', 20)
                pts_taker = s.get('pts_taker', 10)
                if sig == "LONG":
                    if sentiment['top_short_pct'] > 65 and sentiment['retail_long_pct'] > 60:
                        sent_sc = pts_sent
                        reasons.append(f"🧠 Smart money {sentiment['top_short_pct']:.0f}% SHORT vs retail {sentiment['retail_long_pct']:.0f}% LONG — squeeze fuel loaded")
                    elif sentiment['top_short_pct'] > 55:
                        sent_sc = pts_sent // 2
                        reasons.append(f"🧠 Top traders {sentiment['top_short_pct']:.0f}% short — potential squeeze")
                    if sentiment['taker_buy_pct'] > 62:
                        sent_sc += pts_taker
                        reasons.append(f"💚 Taker buy {sentiment['taker_buy_pct']:.0f}% — buyers aggressively entering")
                    elif sentiment['taker_buy_pct'] > 55:
                        sent_sc += pts_taker // 2
                        reasons.append(f"💚 Taker buy slightly dominant {sentiment['taker_buy_pct']:.0f}%")
                else:
                    if sentiment['top_long_pct'] > 65 and sentiment['retail_long_pct'] > 65:
                        sent_sc = pts_sent
                        reasons.append(f"🧠 Smart money {sentiment['top_long_pct']:.0f}% LONG + retail crowded long — distribution likely")
                    elif sentiment['top_long_pct'] > 55:
                        sent_sc = pts_sent // 2
                        reasons.append(f"🧠 Top traders {sentiment['top_long_pct']:.0f}% long — crowded trade")
                    if sentiment['taker_buy_pct'] < 38:
                        sent_sc += pts_taker
                        reasons.append(f"🔴 Taker sell {100-sentiment['taker_buy_pct']:.0f}% — sellers aggressively entering")
                    elif sentiment['taker_buy_pct'] < 45:
                        sent_sc += pts_taker // 2
                        reasons.append(f"🔴 Taker sell slightly dominant")
        pump_score += sent_sc
        bd['sentiment'] = sent_sc

        # 12b ── OI + FUNDING COMBO ───────────────────────────────────────────
        combo_sc = 0
        if (bd.get('oi_spike', 0) >= 10 and
            (bd.get('funding', 0) >= s.get('pts_funding_high', 25) or
             bd.get('funding_hist', 0) >= 12)):
            combo_sc = s.get('pts_oi_funding_combo', 10)
            reasons.append("💥 OI surge + extreme funding combo — maximum squeeze pressure")
        pump_score += combo_sc
        bd['oi_funding_combo'] = combo_sc

        # 13 ── SOCIAL BUZZ ───────────────────────────────────────────────────
        social_data = self.fetch_reddit_buzz(dsym, s)
        social_sc = 0
        if social_data.get('available') and social_data['mentions'] >= s.get('social_min_mentions', 3):
            social_sc = social_data['score']
            reddit_emoji = "🚀" if social_data.get('sentiment') == 'BULLISH' else ("🩸" if social_data.get('sentiment') == 'BEARISH' else "💬")
            reasons.append(f"{reddit_emoji} {social_data.get('source','Reddit')}: {social_data['mentions']} mentions/hr | {social_data.get('sentiment','?')} | avg {social_data.get('upvote_avg',0):.0f} upvotes")
            if social_data.get('top_post'):
                reasons.append(f"📢 Top post: \"{social_data['top_post'][:55]}...\"")
        pump_score += social_sc
        bd['social_buzz'] = social_sc

        # 14 ── ORDER FLOW ─────────────────────────────────────────────────────
        of_pct, of_dir = await self.fetch_orderflow_imbalance(
            exch_obj, sym, s.get('orderflow_lookback', 10))
        of_sc  = 0
        pts_of = s.get('pts_orderflow', 12)
        sell_pct = 100 - of_pct
        if sig == 'LONG':
            if of_dir == 'BUY' and of_pct >= 65:
                of_sc = pts_of
                reasons.append(f"📊 Order flow {of_pct:.0f}% BUY last {s.get('orderflow_lookback',10)} candles — sustained accumulation")
            elif of_dir == 'BUY' and of_pct >= 58:
                of_sc = pts_of // 2
                reasons.append(f"📊 Order flow mildly bullish ({of_pct:.0f}% buy)")
        elif sig == 'SHORT':
            if of_dir == 'SELL' and sell_pct >= 65:
                of_sc = pts_of
                reasons.append(f"📊 Order flow {sell_pct:.0f}% SELL last {s.get('orderflow_lookback',10)} candles — sustained distribution")
            elif of_dir == 'SELL' and sell_pct >= 58:
                of_sc = pts_of // 2
                reasons.append(f"📊 Order flow mildly bearish ({sell_pct:.0f}% sell)")
        pump_score += of_sc
        bd['orderflow'] = of_sc

        # 15 ── LIQUIDATION MAP ────────────────────────────────────────────────
        liq_map    = await self.fetch_liquidation_map(exch_obj, sym, price)
        liq_map_sc = 0
        pts_lm     = s.get('pts_liq_map', 15)
        if liq_map:
            nearest = liq_map[0]
            d       = nearest['dist_pct']
            sz_m    = nearest['size_usd'] / 1_000_000
            slbl    = nearest['side']
            em_lm   = "💥" if ((slbl == 'SHORT_LIQ' and sig == 'LONG') or
                               (slbl == 'LONG_LIQ'  and sig == 'SHORT')) else "🧲"
            if d <= 1.5:
                liq_map_sc = pts_lm
                reasons.append(f"{em_lm} Liq cluster {slbl} ${sz_m:.1f}M @ ${nearest['price']:.4f} ({d:.2f}% away)")
            elif d <= 3.0:
                liq_map_sc = pts_lm // 2
                reasons.append(f"🧲 Liq cluster {slbl} ${sz_m:.1f}M at {d:.1f}%")
        pump_score += liq_map_sc
        bd['liq_map'] = liq_map_sc

        # 16 ── LISTING DETECTOR ───────────────────────────────────────────────
        new_listings = self.fetch_new_listings(s)
        listing_sc   = 0
        listing_info = {}
        pts_lst      = s.get('listing_alert_pts', 25)
        for lst in new_listings:
            if lst['symbol'].upper() == dsym.upper():
                h            = lst.get('listed_ago_h', 999)
                listing_info = lst
                if h <= 24:
                    listing_sc = pts_lst
                    reasons.append(f"🆕 BRAND NEW LISTING on {lst['exchange']} {h:.0f}h ago — high pump probability")
                elif h <= 72:
                    listing_sc = pts_lst // 2
                    reasons.append(f"🆕 Recent listing on {lst['exchange']} {h:.0f}h ago")
                elif h <= 168:
                    listing_sc = pts_lst // 4
                    reasons.append(f"🆕 Listed on {lst['exchange']} this week ({h:.0f}h ago)")
                break
        pump_score += listing_sc
        bd['listing'] = listing_sc

        # 17 ── ON-CHAIN WHALE ─────────────────────────────────────────────────
        onchain    = self.fetch_onchain_whale(dsym, s)
        onchain_sc = 0
        pts_oc     = s.get('pts_onchain_whale', 15)
        if onchain.get('available'):
            if onchain['signal'] == 'BULLISH' and sig == 'LONG':
                onchain_sc = pts_oc
                reasons.append(f"🐋 On-chain: {onchain['detail']} — leaving exchanges (accumulation)")
            elif onchain['signal'] == 'BEARISH' and sig == 'SHORT':
                onchain_sc = pts_oc
                reasons.append(f"🐋 On-chain: {onchain['detail']} — entering exchanges (sell pressure)")
            elif onchain['signal'] == 'BULLISH' and sig == 'SHORT':
                onchain_sc = -(pts_oc // 2)
                reasons.append(f"⚠️ On-chain bullish flow conflicts with SHORT — caution")
            elif onchain['signal'] == 'BEARISH' and sig == 'LONG':
                onchain_sc = -(pts_oc // 2)
                reasons.append(f"⚠️ On-chain bearish flow conflicts with LONG — caution")
        pump_score += onchain_sc
        bd['onchain'] = onchain_sc

        pump_score = max(0, min(pump_score, 100))

        # ─── ACCURACY GATES ──────────────────────────────────────────────────
        active_cats = sum(1 for k, v in bd.items() if v > 0 and k not in ('session', 'mtf'))
        min_cats = s.get('min_active_signals', 3)
        if active_cats < min_cats: return None
        if len(reasons) < s.get('min_reasons', 1): return None

        fng = st.session_state.get('fng_val', 50)
        if sig == "LONG" and fng < s.get('fng_long_threshold', 30):
            if pump_score < 60: return None
        if sig == "SHORT" and fng > s.get('fng_short_threshold', 70):
            if pump_score < 60: return None

        # ═══════════════════════════════════════════════════════════════════
        # IMPROVED SL: USE 4H ORDER BLOCKS FOR STRONGER STOP LOSSES
        # ═══════════════════════════════════════════════════════════════════
        bull_obs_4h, bear_obs_4h = [], []
        if s.get('use_4h_ob_for_sl', True):
            bull_obs_4h, bear_obs_4h = await self.fetch_4h_order_blocks(exch_obj, sym, price)

        try:
            recent_highs = df_f['high'].rolling(5).max().dropna()
            recent_lows  = df_f['low'].rolling(5).min().dropna()
            swing_high = float(recent_highs.iloc[-2]) if len(recent_highs) >= 2 else price * 1.03
            swing_low  = float(recent_lows.iloc[-2])  if len(recent_lows) >= 2 else price * 0.97

            if sig == "LONG":
                # Candidate SL levels from multiple sources
                structure_sl = swing_low * 0.995
                whale_bid_px = whale_details[0]['price'] if whale_details and whale_details[0]['side']=='BUY' else 0
                whale_sl = whale_bid_px * 0.995 if whale_bid_px > 0 and whale_bid_px < price else structure_sl

                # 4H Order Block SL: place below the nearest bullish 4H OB zone
                ob_4h_sl = structure_sl  # default
                ob_4h_used = False
                if bull_obs_4h:
                    nearest_ob = bull_obs_4h[0]
                    ob_sl_candidate = nearest_ob['zone_lo'] * 0.995
                    # Only use if it gives a TIGHTER stop that's still below price
                    if ob_sl_candidate < price and ob_sl_candidate > structure_sl * 0.98:
                        ob_4h_sl = ob_sl_candidate
                        ob_4h_used = True
                        reasons.append(f"🧱 4H OB support zone ${nearest_ob['zone_lo']:.5f}–${nearest_ob['zone_hi']:.5f} — SL anchored below it")

                # Choose the strongest SL (closest to price but with structure confirmation)
                sl_candidates = [sl for sl in [structure_sl, whale_sl, ob_4h_sl] if sl < price]
                sl = max(sl_candidates) if sl_candidates else price - atr * 1.5
                if sl >= price: sl = price - atr * 1.5

                sl_dist = price - sl
                min_rr_tp = price + sl_dist * max(s.get('min_rr', 1.5), 1.5)
                tp = max(swing_high, min_rr_tp)
                if liq_target > price: tp = min(tp, liq_target) if liq_target < tp else tp

            else:  # SHORT
                structure_sl = swing_high * 1.005
                whale_ask_px = whale_details[0]['price'] if whale_details and whale_details[0]['side']=='SELL' else 0
                whale_sl = whale_ask_px * 1.005 if whale_ask_px > 0 and whale_ask_px > price else structure_sl

                # 4H Order Block SL: place above nearest bearish 4H OB zone
                ob_4h_sl = structure_sl
                ob_4h_used = False
                if bear_obs_4h:
                    nearest_ob = bear_obs_4h[0]
                    ob_sl_candidate = nearest_ob['zone_hi'] * 1.005
                    if ob_sl_candidate > price and ob_sl_candidate < structure_sl * 1.02:
                        ob_4h_sl = ob_sl_candidate
                        ob_4h_used = True
                        reasons.append(f"🧱 4H OB resistance zone ${nearest_ob['zone_lo']:.5f}–${nearest_ob['zone_hi']:.5f} — SL anchored above it")

                sl_candidates = [sl for sl in [structure_sl, whale_sl, ob_4h_sl] if sl > price]
                sl = min(sl_candidates) if sl_candidates else price + atr * 1.5
                if sl <= price: sl = price + atr * 1.5

                sl_dist = sl - price
                min_rr_tp = price - sl_dist * max(s.get('min_rr', 1.5), 1.5)
                tp = min(swing_low, min_rr_tp)
                if liq_target > 0 and liq_target < price: tp = max(tp, liq_target)

        except:
            tp = price + atr*3 if sig=="LONG" else price - atr*3
            sl = price - atr*1.5 if sig=="LONG" else price + atr*1.5

        # Multiple TP levels
        try:
            sl_dist = abs(price - sl) if abs(price - sl) > 0 else atr
            if sig == "LONG":
                tp1 = price + sl_dist * 1.5
                tp2 = tp
                if liq_target > swing_high:
                    tp3 = liq_target
                else:
                    tp3 = price + sl_dist * 4.0
                tp3 = max(tp3, price + sl_dist * 3.0)
            else:
                tp1 = price - sl_dist * 1.5
                tp2 = tp
                if liq_target > 0 and liq_target < swing_low:
                    tp3 = liq_target
                else:
                    tp3 = price - sl_dist * 4.0
                tp3 = min(tp3, price - sl_dist * 3.0)
        except:
            sl_dist = atr * 1.5
            tp1 = price + atr*1.5 if sig=="LONG" else price - atr*1.5
            tp2 = price + atr*3   if sig=="LONG" else price - atr*3
            tp3 = price + atr*5   if sig=="LONG" else price - atr*5

        # R:R filter
        try:
            rr = abs(tp - price) / abs(price - sl)
            if rr < s.get('min_rr', 1.5):
                return None
        except: pass

        result = {
            'symbol': dsym, 'exchange': exch_name, 'price': price, 'pump_score': pump_score,
            'tp1': tp1, 'tp2': tp2, 'tp3': tp3,
            'type': sig, 'reasons': reasons, 'tp': tp, 'sl': sl, 'rsi': rsi,
            'funding': fr, 'atr': atr, 'ob': ob_data, 'cmc': cmc, 'oi': oi,
            'oi_change_6h': oi_change_6h,
            'liq_target': liq_target, 'liq_detail': liq_detail,
            'whale_str': whale_str, 'whale_details': whale_details,
            'vol_mcap': vol_mcap_ratio, 'signal_breakdown': bd,
            'session': sname, 'price_chg_20': pc20,
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'quote_vol': float(tick.get('quoteVolume',0) or 0),
            'sentiment': sentiment,
            'momentum_confirmed': momentum_confirmed,
            'funding_history': funding_history[-8:] if funding_history else [],
            'social_data': social_data,
            'atr_pct': round(atr_pct, 2),
            'pct_24h': float(tick.get('percentage') or tick.get('change') or 0),
            'vol_surge_ratio': round(vsurge, 2),
            'liq_map_data':   liq_map[:3] if liq_map else [],
            'listing_data':   listing_info,
            'onchain_data':   onchain,
            'orderflow_data': {'pct': round(of_pct, 1), 'dir': of_dir},
            'entry_lo': round(price - atr * 0.35, 8) if sig == 'LONG' else round(price + atr * 0.1, 8),
            'entry_hi': round(price + atr * 0.1,  8) if sig == 'LONG' else round(price + atr * 0.35, 8),
            'warnings': warnings,
            'late_entry_flag': late_entry_flag,
            'vol_exhaust_flag': vol_exhaust_flag,
            'near_top_flag': near_top_flag,
            'rsi_direction': rsi_direction,
            'bull_obs_4h': bull_obs_4h[:2] if bull_obs_4h else [],
            'bear_obs_4h': bear_obs_4h[:2] if bear_obs_4h else [],
        }
        result['_cls_cfg'] = {
            'breakout_oi_min':  s.get('cls_breakout_oi_min', 7),
            'breakout_vol_min': s.get('cls_breakout_vol_min', 4),
            'breakout_sc_min':  s.get('cls_breakout_score_min', 25),
            'squeeze_fund_min': s.get('cls_squeeze_fund_min', 8),
            'squeeze_ob_min':   s.get('cls_squeeze_ob_min', 6),
        }
        result['cls'] = classify(result)
        if s['cooldown_on']: set_cooldown(dsym)
        return result

    async def run(self, s):
        btc_trend, btc_px, btc_rsi = await self.fetch_btc()
        await asyncio.gather(
            self.okx.load_markets(),
            self.mexc.load_markets(),
            self.gate.load_markets()
        )
        tickers_okx, tickers_mexc, tickers_gate = {}, {}, {}
        try: tickers_okx  = await self.okx.fetch_tickers()
        except: pass
        try: tickers_mexc = await self.mexc.fetch_tickers()
        except: pass
        try: tickers_gate = await self.gate.fetch_tickers()
        except: pass

        combined_tickers = {**tickers_okx, **tickers_mexc, **tickers_gate}
        process_journal_tracking(combined_tickers, s)

        swaps_dict = {}
        for sym, t in tickers_mexc.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'MEXC', 'exch_obj': self.mexc}
        for sym, t in tickers_gate.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'GATE', 'exch_obj': self.gate}
        for sym, t in tickers_okx.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                pct = float(t.get('percentage') or t.get('change') or 0)
                swaps_dict[sym] = {'vol': float(t['quoteVolume'] or 0), 'pct': pct,
                                   'exch_name': 'OKX', 'exch_obj': self.okx}

        # Support multiple scan modes (multi-select)
        scan_modes = s.get('scan_modes', ['mixed'])
        if isinstance(scan_modes, str): scan_modes = [scan_modes]
        depth = s['scan_depth']

        def get_mode_symbols(mode, swaps_dict, n):
            if mode == 'gainers':
                return sorted(swaps_dict.items(), key=lambda x: x[1]['pct'], reverse=True)[:n]
            elif mode == 'losers':
                return sorted(swaps_dict.items(), key=lambda x: x[1]['pct'])[:n]
            elif mode == 'mixed':
                by_gain = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'], reverse=True)
                by_loss = sorted(swaps_dict.items(), key=lambda x: x[1]['pct'])
                half = n // 2
                seen_m = set(); mixed = []
                for item in by_gain[:half] + by_loss[:half]:
                    if item[0] not in seen_m:
                        mixed.append(item); seen_m.add(item[0])
                return mixed
            else:  # volume
                return sorted(swaps_dict.items(), key=lambda x: x[1]['vol'], reverse=True)[:n]

        # Combine symbols from all selected modes, deduplicate
        combined = {}
        per_mode = max(depth // max(1, len(scan_modes)), 10)
        for mode in scan_modes:
            for sym, data in get_mode_symbols(mode, swaps_dict, per_mode):
                if sym not in combined:
                    combined[sym] = data
        symbols = list(combined.items())[:depth]

        results=[]; errors=[]
        pb=st.progress(0); st_=st.empty()
        for i, (sym, data) in enumerate(symbols):
            exch_name = data['exch_name']
            exch_obj  = data['exch_obj']
            st_.markdown(
                f"<span style='font-family:monospace;font-size:.68rem;color:#7a82a0;'>"
                f"Scanning {i+1}/{len(symbols)} — <b>{sym.split(':')[0]}</b> ({exch_name})"
                f" — found so far: <b style='color:#0f1117'>{len(results)}</b></span>",
                unsafe_allow_html=True)
            try:
                r = await self.analyze(exch_name, exch_obj, sym, s, btc_trend)
                if r: results.append(r)
            except Exception as e:
                errors.append(f"{sym} ({exch_name}): {str(e)[:80]}")
            pb.progress((i+1)/len(symbols))
            await asyncio.sleep(0.5)

        st_.empty(); pb.empty()
        try:
            await self.okx.close()
            await self.mexc.close()
            await self.gate.close()
        except: pass

        results.sort(key=lambda x: x['pump_score'], reverse=True)

        if S.get('dedup_symbols', True):
            seen = {}
            for r in results:
                sym_base = r['symbol']
                if sym_base not in seen or r['pump_score'] > seen[sym_base]['pump_score']:
                    seen[sym_base] = r
            results = sorted(seen.values(), key=lambda x: x['pump_score'], reverse=True)
        return results, btc_trend, btc_px, btc_rsi, errors


# ─── CARD ─────────────────────────────────────────────────────────────────────
def render_card(res, is_sniper=False):
    sc=res['pump_score']; col=pump_color(sc, is_sniper); lbl=pump_label(sc, res['type'], is_sniper)
    sig=res['type']; bd=res.get('signal_breakdown',{}); ob=res.get('ob',{})
    cmc=res.get('cmc') or {}; card_cls="pc-long" if sig=="LONG" else "pc-short"
    sig_col="var(--green)" if sig=="LONG" else "var(--red)"
    sentiment = res.get('sentiment', {})
    if not isinstance(sentiment, dict): sentiment = {}
    momentum_confirmed = res.get('momentum_confirmed', False)
    warnings_list = res.get('warnings', [])

    exch = res.get('exchange', 'MEXC')
    exch_colors = {"OKX": "#00bcd4", "GATE": "#e040fb", "MEXC": "#2563eb"}
    exch_col = exch_colors.get(exch, "#2563eb")
    if exch == "OKX":
        trade_link = f"https://www.okx.com/trade-swap/{res['symbol'].lower()}-usdt-swap"
    elif exch == "GATE":
        trade_link = f"https://www.gate.io/futures_trade/USDT/{res['symbol']}_USDT"
    else:
        trade_link = f"https://www.mexc.com/exchange/{res['symbol']}_USDT"

    def pip(v,lo=5,hi=12):
        if v>=hi: return "<span class='pip pip-on'></span>"
        if v>=lo: return "<span class='pip pip-half'></span>"
        return "<span class='pip pip-off'></span>"

    bid_pct = ob.get('bid_pct', 50)
    chg = cmc.get('change24', 0)
    cmc_html = ""
    if cmc:
        chg_c = "var(--green)" if chg>=0 else "var(--red)"
        cmc_html = f"""<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;">
          <span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:var(--muted);">Rank #{cmc.get('rank','?')}</span>
          <span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:var(--muted);">MCap {fmt(cmc.get('mcap',0))}</span>
          <span style="background:{'var(--green-bg)' if chg>=0 else 'var(--red-bg)'};border:1px solid {'var(--green-bd)' if chg>=0 else 'var(--red-bd)'};border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:{chg_c};">{'+' if chg>=0 else ''}{chg:.2f}%</span></div>"""

    # Warnings banner (late entry, volume exhaustion, near top)
    warnings_html = ""
    if warnings_list:
        warn_items = "".join([f'<div style="padding:3px 0;font-size:.68rem;">⚠️ {w}</div>' for w in warnings_list])
        warnings_html = f"""<div style="background:#fef3c7;border:1px solid #f59e0b;border-left:4px solid #f59e0b;border-radius:6px;padding:8px 12px;margin:6px 0;">
          <div style="font-family:monospace;font-size:.6rem;font-weight:700;color:#92400e;margin-bottom:4px;">⚠️ RISK WARNINGS — Read before trading</div>
          <div style="font-family:monospace;color:#92400e;">{warn_items}</div>
        </div>"""

    # 4H Order Block display
    ob4h_html = ""
    bull_obs = res.get('bull_obs_4h', [])
    bear_obs = res.get('bear_obs_4h', [])
    if sig == "LONG" and bull_obs:
        ob = bull_obs[0]
        ob4h_html = f"""<div style="background:#ecfdf5;border-left:3px solid #059669;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#065f46;margin:4px 0;">
          🧱 4H OB Support: ${ob['zone_lo']:.5f}–${ob['zone_hi']:.5f} | body {ob['body_pct']*100:.0f}% | {ob['dist_pct']:.1f}% below — SL anchored here
        </div>"""
    elif sig == "SHORT" and bear_obs:
        ob = bear_obs[0]
        ob4h_html = f"""<div style="background:#fef2f2;border-left:3px solid #dc2626;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#7f1d1d;margin:4px 0;">
          🧱 4H OB Resistance: ${ob['zone_lo']:.5f}–${ob['zone_hi']:.5f} | body {ob['body_pct']*100:.0f}% | {ob['dist_pct']:.1f}% above — SL anchored here
        </div>"""

    social_html = ""
    sd = res.get('social_data', {})
    if not isinstance(sd, dict): sd = {}
    if sd.get('available'):
        sent_cfg = {
            'BULLISH': ('#059669', '#ecfdf5', '#a7f3d0', '📈'),
            'BEARISH': ('#dc2626', '#fef2f2', '#fecaca', '📉'),
            'NEUTRAL': ('#d97706', '#fffbeb', '#fde68a', '💬'),
        }
        sc_color, sc_bg, sc_bd, sc_emoji = sent_cfg.get(sd.get('sentiment','NEUTRAL'), sent_cfg['NEUTRAL'])
        source = sd.get('source', 'Social')
        mentions = sd.get('mentions', 0)
        avg_up = sd.get('upvote_avg', 0)
        social_sentiment = sd.get('sentiment', 'NEUTRAL')
        top_post = sd.get('top_post', '')
        buzz_thr_card = S.get('social_buzz_threshold', 10)
        bar_pct = min(100, int((mentions / max(1, buzz_thr_card)) * 100))
        _tp60 = top_post[:60].replace("{","{{").replace("}","}}")
        top_line = (f'<div style="font-size:.6rem;color:{sc_color};margin-top:4px;opacity:.85;">"{_tp60}..."</div>') if top_post else ""
        avg_line = (f' &nbsp;·&nbsp; avg {avg_up:.0f} upvotes') if avg_up > 0 else ""
        social_html = (
            f'<div style="background:{sc_bg};border:1px solid {sc_bd};border-left:4px solid {sc_color};'
            f'border-radius:6px;padding:8px 12px;margin:6px 0;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'
            f'<span style="font-family:monospace;font-size:.62rem;font-weight:700;color:{sc_color};">'
            f'{sc_emoji} {source} Buzz</span>'
            f'<span style="font-family:monospace;font-size:.7rem;font-weight:700;color:{sc_color};">'
            f'{social_sentiment}</span></div>'
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:5px;">'
            f'<div style="flex:1;height:4px;background:rgba(0,0,0,.08);border-radius:2px;">'
            f'<div style="width:{bar_pct}%;height:100%;background:{sc_color};border-radius:2px;"></div></div>'
            f'<span style="font-family:monospace;font-size:.6rem;color:{sc_color};white-space:nowrap;">'
            f'{mentions} mentions/hr{avg_line}</span></div>{top_line}</div>'
        )

    whale_html = ""
    whale_details = res.get('whale_details', [])
    if whale_details:
        w = whale_details[0]
        wc = "var(--green)" if w['side']=='BUY' else "var(--red)"
        wbg = "var(--green-bg)" if w['side']=='BUY' else "var(--red-bg)"
        dist_badge = f"<span style='color:var(--muted);font-size:.58rem;'>({w['dist_pct']:.2f}% {'below' if w['side']=='BUY' else 'above'})</span>"
        whale_html = f"""<div style="background:{wbg};border-left:3px solid {wc};border-radius:6px;padding:7px 12px;font-family:monospace;font-size:.72rem;color:{wc};margin:6px 0;font-weight:600;display:flex;justify-content:space-between;align-items:center;">
          <span>🐋 {w['side']} WALL {fmt(w['value'])} @ ${w['price']:.6f}</span>
          {dist_badge}
        </div>"""

    liq_html = ""
    if res.get('liq_target', 0):
        liq_html = f"""<div style="background:var(--purple-bg);border-left:3px solid var(--purple);border-radius:6px;padding:7px 12px;font-size:.72rem;color:var(--purple);margin:6px 0;">🧲 Liq magnet @ {res['liq_detail']}</div>"""

    mom_html = ""
    if momentum_confirmed:
        mom_col = "var(--green)" if sig=="LONG" else "var(--red)"
        mom_bg = "var(--green-bg)" if sig=="LONG" else "var(--red-bg)"
        mom_brd = mom_col.replace(")", "-bd)").replace("var(--green","var(--green").replace("var(--red","var(--red")
        mom_arrow = "▲" if sig=="LONG" else "▼"
        mom_html = f'<span class="momentum-badge" style="background:{mom_bg};color:{mom_col};border:1px solid {mom_brd};">{mom_arrow} MOMENTUM LIVE</span>'

    # RSI direction badge
    rsi_dir = res.get('rsi_direction', 'FLAT')
    rsi_dir_html = ""
    if rsi_dir == 'RISING' and sig == "LONG":
        rsi_dir_html = '<span style="background:#ecfdf5;color:#059669;border:1px solid #a7f3d0;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ↑</span>'
    elif rsi_dir == 'FALLING' and sig == "SHORT":
        rsi_dir_html = '<span style="background:#fef2f2;color:#dc2626;border:1px solid #fecaca;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ↓</span>'
    elif (rsi_dir == 'FALLING' and sig == "LONG") or (rsi_dir == 'RISING' and sig == "SHORT"):
        rsi_dir_html = '<span style="background:#fef3c7;color:#92400e;border:1px solid #fde68a;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ⚠</span>'

    sentiment_html = ""
    if sentiment.get('available'):
        tl = sentiment['top_long_pct']
        ts = sentiment['top_short_pct']
        rl = sentiment['retail_long_pct']
        tb = sentiment['taker_buy_pct']
        def sbar(label, pct, color_hi, color_lo):
            c = color_hi if pct > 50 else color_lo
            return f"""<div class="sentiment-bar">
              <div class="sbar-label">{label}</div>
              <div class="sbar-track"><div class="sbar-fill" style="width:{pct:.0f}%;background:{c};"></div></div>
              <div class="sbar-val" style="color:{c};">{pct:.0f}%</div>
            </div>"""
        sentiment_html = f"""
        <div style="margin:8px 0;padding:10px;background:var(--panel);border-radius:8px;border:1px solid var(--border);">
          <div style="font-family:monospace;font-size:.55rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">{sentiment.get('source','Exchange')} Sentiment</div>
          {sbar("Longs", tl, "var(--green)", "var(--red)")}
          {sbar("Retail Long", rl, "var(--green)", "var(--red)")}
          {sbar("Taker Buy Vol", tb, "var(--green)", "var(--red)")}
        </div>"""

    oi_change = res.get('oi_change_6h', 0)
    oi_change_html = ""
    if abs(oi_change) >= 5:
        oi_c  = "var(--green)" if oi_change > 0 else "var(--red)"
        oi_bg = "var(--green-bg)" if oi_change > 0 else "var(--red-bg)"
        oi_bd = "var(--green-bd)" if oi_change > 0 else "var(--red-bd)"
        oi_change_html = (f'<span style="background:{oi_bg};color:{oi_c};border:1px solid {oi_bd};'
                          f'padding:2px 7px;border-radius:4px;font-family:monospace;'
                          f'font-size:.6rem;font-weight:600;">OI {oi_change:+.1f}% 6h</span>')

    def _esc(s): return str(s).replace("{","{"+"{").replace("}","}"+"}")
    reasons_html = "".join([f"<div class='r'><span style='color:var(--muted);margin-right:6px;'>▸</span>{_esc(r)}</div>" for r in res['reasons'][:12]])

    exch_bg   = exch_col + "22"
    exch_bd   = exch_col + "44"
    sig_bg    = sig_col + "18"
    sig_bd    = sig_col + "44"
    score_bg  = col + "14"
    sniper_cls = " sniper" if is_sniper else ""
    ts        = res.get('timestamp', '')
    session   = res.get('session', '')
    sym_cls   = res.get('cls', '—').upper()
    _price = float(res.get('price') or 0)
    _tp    = float(res.get('tp')    or 0)
    _sl    = float(res.get('sl')    or 0)
    _rsi   = float(res.get('rsi')   or 0)
    _tp1   = float(res.get('tp1') or _tp or 0)
    _tp2   = float(res.get('tp2') or _tp or 0)
    _tp3   = float(res.get('tp3') or _tp or 0)
    tp_pct = f"+{abs(_tp-_price)/_price*100:.2f}%" if _price > 0 and _tp > 0 else ""
    sl_pct = f"-{abs(_price-_sl)/_price*100:.2f}%" if _price > 0 and _sl > 0 else ""
    tp1_pct = f"+{abs(_tp1-_price)/_price*100:.2f}%" if _price > 0 and _tp1 > 0 else ""
    tp2_pct = f"+{abs(_tp2-_price)/_price*100:.2f}%" if _price > 0 and _tp2 > 0 else ""
    tp3_pct = f"+{abs(_tp3-_price)/_price*100:.2f}%" if _price > 0 and _tp3 > 0 else ""
    rsi_display   = f"{_rsi:.1f}"
    price_display = f"${_price:.6f}"
    tp_display    = f"${_tp:.6f}"
    tp1_display   = f"${_tp1:.6f}"
    tp2_display   = f"${_tp2:.6f}"
    tp3_display   = f"${_tp3:.6f}"
    sl_display    = f"${_sl:.6f}"
    pct_24h       = float(res.get('pct_24h', 0) or 0)
    pct_col       = "var(--green)" if pct_24h >= 0 else "var(--red)"
    pct_sign      = "+" if pct_24h >= 0 else ""
    pct_display   = f"{pct_sign}{pct_24h:.2f}%"
    new_badge     = '<span style="background:#0ea5e9;color:#fff;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">🆕 NEW</span>' if res.get('is_new') else ''
    jump_sc       = res.get('score_jump', 0)
    jump_badge    = f'<span style="background:#f59e0b22;color:#f59e0b;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">⬆️ +{jump_sc}pt</span>' if jump_sc >= 15 else ''
    _elo_d        = f"${float(res.get('entry_lo') or 0):.6f}"
    _ehi_d        = f"${float(res.get('entry_hi') or 0):.6f}"
    try:
        rr = abs(_tp-_price)/abs(_price-_sl) if abs(_price-_sl) > 0 else 0
        rr_str = f"{rr:.1f}:1"
        rr_col = "var(--green)" if rr >= 2 else ("var(--amber)" if rr >= 1.5 else "var(--red)")
    except:
        rr_str = "N/A"; rr_col = "var(--muted)"

    # Warning count badge
    warn_count_html = ""
    if warnings_list:
        warn_count_html = f'<span style="background:#fef3c7;color:#92400e;border:1px solid #f59e0b;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.58rem;font-weight:700;">⚠️ {len(warnings_list)} RISK</span>'

    try:
      card_html = f"""
<div class="pump-card {card_cls}">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div class="score-ring{sniper_cls}" style="color:{col};border-color:{col};background:{score_bg};">{sc}</div>
      <div>
        <div style="font-family:monospace;font-size:1.2rem;font-weight:700;">{res['symbol']}</div>
        <div style="display:flex;align-items:center;gap:6px;margin-top:3px;flex-wrap:wrap;">
          <span style="background:{exch_bg};border:1px solid {exch_bd};color:{exch_col};padding:1px 7px;border-radius:3px;font-family:monospace;font-size:.58rem;font-weight:700;">{exch}</span>
          <span style="background:{sig_bg};border:1px solid {sig_bd};color:{sig_col};padding:2px 10px;border-radius:3px;font-family:monospace;font-size:.72rem;font-weight:800;letter-spacing:.08em;">{'🟢 LONG' if sig=='LONG' else '🔴 SHORT'}</span>
          <span style="font-family:monospace;font-size:.6rem;color:{col};font-weight:600;">{lbl}</span>
          {mom_html}
          {rsi_dir_html}
          {oi_change_html}
          {warn_count_html}
          {new_badge}{jump_badge}
          <span style="font-family:monospace;font-size:.58rem;color:var(--muted);">{ts}</span>
        </div>
      </div>
    </div>
    <div style="text-align:right;font-family:monospace;font-size:.58rem;color:var(--muted);">
      R:R <span style="color:{rr_col};font-weight:600;">{rr_str}</span><br>
      RSI <span style="color:var(--text);">{rsi_display}</span><br>
      {session}
    </div>
  </div>

  {warnings_html}

  <div class="sig-pips">
    <div class="pip-item">{pip(bd.get('ob_imbalance',0),4,14)} OB</div>
    <div class="pip-item">{pip(bd.get('funding',0)+bd.get('funding_hist',0),3,15)} FUNDING</div>
    <div class="pip-item">{pip(bd.get('oi_spike',0),5,14)} OI</div>
    <div class="pip-item">{pip(bd.get('vol_surge',0),3,10)} VOLUME</div>
    <div class="pip-item">{pip(bd.get('liq_cluster',0),4,12)} LIQ</div>
    <div class="pip-item">{pip(bd.get('whale_wall',0),4,7)} WHALE</div>
    <div class="pip-item">{pip(bd.get('technicals',0),5,12)} TECH</div>
    <div class="pip-item">{pip(bd.get('sentiment',0),8,20)} SENT</div>
    <div class="pip-item">{pip(bd.get('momentum',0),4,8)} MOM</div>
    <div class="pip-item">{pip(bd.get('social_buzz',0),3,7)} SOC</div>
    <div class="pip-item">{pip(max(0,bd.get('mtf',0)),6,12)} MTF</div>
    <div class="pip-item">{pip(bd.get('orderflow',0),6,12)} FLOW</div>
    <div class="pip-item">{pip(bd.get('liq_map',0),7,15)} LIQ-MAP</div>
    <div class="pip-item">{pip(bd.get('listing',0),6,25)} LISTING</div>
    <div class="pip-item">{pip(max(0,bd.get('onchain',0)),7,15)} ONCHAIN</div>
  </div>

  <div style="display:flex;height:5px;border-radius:3px;overflow:hidden;margin:6px 0;background:var(--panel);">
    <div style="width:{bid_pct:.0f}%;background:var(--green);"></div>
    <div style="width:{100-bid_pct:.0f}%;background:var(--red);"></div>
  </div>
  <div style="display:flex;justify-content:space-between;font-family:monospace;font-size:.56rem;color:var(--muted);margin-bottom:8px;">
    <span>BID {bid_pct:.0f}%</span><span>ASK {100-bid_pct:.0f}%</span>
  </div>

  {ob4h_html}

  <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;padding:6px 12px;">
    <span style="font-family:monospace;font-size:.55rem;color:#2563eb;font-weight:700;">📍 BEST ENTRY ZONE</span>
    <span style="font-family:monospace;font-size:.68rem;color:#1d4ed8;font-weight:700;margin-left:10px;">{_elo_d}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#2563eb;margin:0 6px;">–</span>
    <span style="font-family:monospace;font-size:.68rem;color:#1d4ed8;font-weight:700;">{_ehi_d}</span>
    <span style="font-family:monospace;font-size:.5rem;color:#60a5fa;margin-left:8px;">don't chase — wait for pullback into zone</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin:8px 0;">
    <div class="px-cell">
      <div class="px-lbl">Entry</div>
      <div class="px-val" style="color:var(--blue);font-size:.72rem;">{price_display}</div>
      <div style="font-family:monospace;font-size:.5rem;color:{pct_col};">{pct_display} 24h</div>
    </div>
    <div class="px-cell" style="border-top:2px solid #86efac44;">
      <div class="px-lbl">TP 1 <span style="color:#86efac;font-size:.5rem;">scalp</span></div>
      <div class="px-val" style="color:#86efac;font-size:.68rem;">{tp1_display}</div>
      <div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp1_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid var(--green);">
      <div class="px-lbl">TP 2 <span style="color:var(--green);font-size:.5rem;">target</span></div>
      <div class="px-val" style="color:var(--green);font-size:.72rem;">{tp2_display}</div>
      <div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp2_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid #f59e0b;">
      <div class="px-lbl">TP 3 <span style="color:#f59e0b;font-size:.5rem;">max run</span></div>
      <div class="px-val" style="color:#f59e0b;font-size:.68rem;">{tp3_display}</div>
      <div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp3_pct}</div>
    </div>
    <div class="px-cell" style="border-top:2px solid var(--red);">
      <div class="px-lbl">Stop Loss</div>
      <div class="px-val" style="color:var(--red);font-size:.72rem;">{sl_display}</div>
      <div style="font-family:monospace;font-size:.52rem;color:var(--muted);">{sl_pct}</div>
    </div>
  </div>

  {whale_html}{liq_html}{social_html}{cmc_html}
  {sentiment_html}
  <div class="reasons-list" style="margin-top:8px;">{reasons_html}</div>
  <div style="margin-top:10px;display:flex;gap:12px;align-items:center;">
    <a href="{trade_link}" target="_blank"
       style="font-family:monospace;font-size:.62rem;color:var(--blue);text-decoration:none;font-weight:600;">
      Trade {res['symbol']} on {exch} →
    </a>
    <span style="font-family:monospace;font-size:.58rem;color:var(--muted);">
      Class: <b style="color:var(--text);">{sym_cls}</b>
    </span>
  </div>
</div>"""
      card_html = "\n".join(line.lstrip() for line in card_html.splitlines())
      st.markdown(card_html, unsafe_allow_html=True)
    except Exception as _ce:
        _sym = res.get("symbol","?")
        _sc  = res.get("pump_score","?")
        _tp  = res.get("type","?")
        st.error(f"Card render error [{_sym}]: {_ce}")
        st.code(f"{_sym} | Score:{_sc} | {_tp} | Entry:{res.get('price',0):.6f}")


# ─── SIDEBAR NAV ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚡ APEX")
    st.caption("Pump & Dump Intelligence")
    nav = st.radio("Navigation", ["🔥 Scanner","⚙️ Settings","📒 Journal"], label_visibility="collapsed")
    st.divider()

    st.subheader("Quick Controls")
    q_depth = st.slider("Coins to Scan", 10, 100, S['scan_depth'], step=10, key="q_depth")
    q_min   = st.slider("Min Score", 1, 80, S['min_score'], key="q_min")
    q_btc   = st.toggle("BTC Bear blocks LONGs", S['btc_filter'], key="q_btc")
    q_mom   = st.toggle("Require Momentum", S.get('require_momentum', False), key="q_mom")

    st.caption("Scan Focus (select multiple):")
    _saved_modes = S.get('scan_modes', ['mixed'])
    if isinstance(_saved_modes, str): _saved_modes = [_saved_modes]
    q_mode_vol    = st.checkbox("Volume",  value='volume'  in _saved_modes, key="q_vol")
    q_mode_gain   = st.checkbox("Gainers", value='gainers' in _saved_modes, key="q_gain")
    q_mode_loss   = st.checkbox("Losers",  value='losers'  in _saved_modes, key="q_loss")
    q_mode_mixed  = st.checkbox("Mixed",   value='mixed'   in _saved_modes, key="q_mixed")
    selected_modes = []
    if q_mode_vol:   selected_modes.append('volume')
    if q_mode_gain:  selected_modes.append('gainers')
    if q_mode_loss:  selected_modes.append('losers')
    if q_mode_mixed: selected_modes.append('mixed')
    if not selected_modes: selected_modes = ['mixed']

    st.divider()
    st.subheader("Auto-Pilot")
    q_auto     = st.toggle("Continuous Scan", S.get('auto_scan', False))
    q_auto_int = st.number_input("Interval (mins)", 1, 60, S.get('auto_interval', 5))

    st.divider()
    if st.button("Clear Cooldowns"):
        if os.path.exists(COOLDOWN_FILE): os.remove(COOLDOWN_FILE)
        st.success("Done")

    st.subheader("Sentinel")
    st.caption("Scans TOP 100 coins continuously.")
    q_sentinel = st.toggle("Enable Sentinel", st.session_state.get("sentinel_active", False))
    if q_sentinel != st.session_state.get("sentinel_active", False):
        st.session_state.sentinel_active = q_sentinel
        st.session_state.sentinel_total_checked = 0
        st.session_state.sentinel_signals_found = 0
    if st.session_state.get("sentinel_active"):
        chk  = st.session_state.get("sentinel_total_checked", 0)
        sig_s = st.session_state.get("sentinel_signals_found", 0)
        last = st.session_state.get("sentinel_last_check", "?")
        st.write(f"Checked: {chk} | Signals: {sig_s} | Last: {last}")

    sn, sm, sc_ = get_session()
    st.divider()
    st.write(f"Session: **{sn}** ({sm}x)")


# ─── HEADER / TICKER ─────────────────────────────────────────────────────────
st.markdown("""<div style="padding:18px 0 14px;">
  <div style="font-family:monospace;font-size:1.5rem;font-weight:700;color:#0f1117;">APEX</div>
  <div style="font-family:monospace;font-size:.56rem;font-weight:400;letter-spacing:.16em;color:#7a82a0;text-transform:uppercase;margin-top:2px;">Pump & Dump Intelligence Terminal v2.0 — 5 New Risk Filters Active</div>
</div>""", unsafe_allow_html=True)

_fng_last = st.session_state.get('fng_last_fetch', 0)
if time.time() - _fng_last > 300:
    try:
        fg = requests.get("https://api.alternative.me/fng/?limit=1", timeout=2).json()
        st.session_state.fng_val = int(fg['data'][0]['value'])
        st.session_state.fng_txt = fg['data'][0]['value_classification']
        st.session_state.fng_last_fetch = time.time()
    except: pass

fng_v = st.session_state.fng_val; fng_t = st.session_state.fng_txt
fng_c = "#059669" if fng_v>=60 else ("#dc2626" if fng_v<=40 else "#d97706")
btc_c = "#059669" if st.session_state.btc_trend=="BULLISH" else ("#dc2626" if st.session_state.btc_trend=="BEARISH" else "#7a82a0")
sn_, _, sc_now = get_session()
st.markdown(f"""<div class="ticker-bar">
  <div><div class="t-lbl">BTC</div><div class="t-val">${st.session_state.btc_price:,.0f} <span style="color:{btc_c};">{st.session_state.btc_trend}</span></div></div>
  <div><div class="t-lbl">Fear &amp; Greed</div><div class="t-val" style="color:{fng_c};">{fng_v} — {fng_t.upper()}</div></div>
  <div><div class="t-lbl">Session</div><div class="t-val" style="color:{sc_now};">{sn_}</div></div>
  <div><div class="t-lbl">Last Scan</div><div class="t-val">{st.session_state.last_scan}</div></div>
  <div><div class="t-lbl">Raw found</div><div class="t-val">{st.session_state.last_raw_count}</div></div>
  <div><div class="t-lbl">After filter</div><div class="t-val">{len(st.session_state.results)}</div></div>
  <div><div class="t-lbl">Scans</div><div class="t-val">#{st.session_state.scan_count}</div></div>
  <div><div class="t-lbl">UTC</div><div class="t-val">{datetime.now(timezone.utc).strftime('%H:%M:%S')}</div></div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
if nav == "⚙️ Settings":
    st.markdown('<div class="section-h">Settings — all thresholds and scoring weights</div>', unsafe_allow_html=True)
    st.info("💡 **How to read this page:** Each setting shows a purple helper label explaining what INCREASING or DECREASING that value does to your results.")

    with st.form("settings_form"):

        # ── ALERTS ──────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔔 Notification Controls</div>', unsafe_allow_html=True)
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            ns_alert_score = st.slider("Minimum Score for Alerts", 10, 100, S.get('alert_min_score', 60))
            st.markdown('<div class="setting-help">⬆️ Raise = fewer but higher-confidence alerts | ⬇️ Lower = more alerts including weaker setups</div>', unsafe_allow_html=True)
        with ac2:
            st.markdown("**Types to Alert:**")
            ns_al_long  = st.checkbox("Alert on LONGs (Pumps)", S.get('alert_longs', True))
            ns_al_short = st.checkbox("Alert on SHORTs (Dumps)", S.get('alert_shorts', True))
        with ac3:
            st.markdown("**Classes to Alert:**")
            ns_al_sq = st.checkbox("About to Squeeze", S.get('alert_squeeze', True))
            ns_al_br = st.checkbox("Confirmed Breakout", S.get('alert_breakout', True))
            ns_al_wh = st.checkbox("Whale Driven", S.get('alert_whale', True))
            ns_al_ea = st.checkbox("Early Signal", S.get('alert_early', False))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── NEW: RISK FILTER SETTINGS ────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#f59e0b;"><div class="stg-title" style="color:#92400e;">⚠️ New Risk & Anti-False-Positive Filters (v2)</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">These 5 new filters detect <b>late entries, exhausted pumps, fading momentum, distribution tops, and funding traps</b>. They penalize scores when these conditions are detected. Cards will show ⚠️ warnings when triggered.</div>', unsafe_allow_html=True)
        rf1, rf2, rf3 = st.columns(3)
        with rf1:
            ns_late_thresh = st.number_input("Late Entry: price change threshold %", 3.0, 20.0, float(S.get('late_entry_chg_thresh', 8.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">⬆️ Raise = only penalize very large late moves (more signals pass) | ⬇️ Lower = penalize even moderate late moves (stricter filter)</div>', unsafe_allow_html=True)
            ns_late_pen = st.slider("Late Entry penalty (pts)", 5, 30, int(S.get('late_entry_penalty', 20)))
            st.markdown('<div class="setting-help">⬆️ Raise = late entries lose more score | ⬇️ Lower = softer penalty (signals still appear but ranked lower)</div>', unsafe_allow_html=True)
        with rf2:
            ns_exhaust_pen = st.slider("Volume Exhaustion penalty (pts)", 5, 25, int(S.get('vol_exhaust_penalty', 15)))
            st.markdown('<div class="setting-help">When current vol < 60% of 5c avg near top → penalize. ⬆️ Higher = hard block on distribution setups | ⬇️ Lower = softer warning</div>', unsafe_allow_html=True)
            ns_near_top_pen = st.slider("Near Local Top penalty (pts)", 5, 25, int(S.get('near_top_penalty', 15)))
            st.markdown('<div class="setting-help">Price within 3% of 20c high + MACD+RSI declining → penalize. ⬆️ Raise to strongly avoid distribution traps</div>', unsafe_allow_html=True)
        with rf3:
            ns_use_4h_ob = st.toggle("Use 4H Order Blocks for SL", S.get('use_4h_ob_for_sl', True))
            st.markdown('<div class="setting-help">ON = SL is placed below/above nearest 4H OB zone (stronger, less likely to be hit) | OFF = use only swing highs/lows (simpler)</div>', unsafe_allow_html=True)
            st.info("**Funding Age Context** is always on. If negative funding appeared AFTER a pump, it scores -10pts (shorts may be right, not a squeeze setup).")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SCAN ────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔍 Scan Configuration</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_depth = st.slider("Coins to scan", 10, 200, S['scan_depth'], step=10)
            st.markdown('<div class="setting-help">⬆️ More coins = broader coverage but slower scan | ⬇️ Fewer = faster scan, less coverage. Start at 40.</div>', unsafe_allow_html=True)
        with c2:
            ns_fast = st.selectbox("Signal timeframe", ["1m","5m","15m","1h"], index=["1m","5m","15m","1h"].index(S['fast_tf']))
            st.markdown('<div class="setting-help">⬆️ Slower TF (1h) = fewer but higher quality signals | ⬇️ Faster (1m/5m) = more signals but noisier. 15m is optimal.</div>', unsafe_allow_html=True)
        with c3:
            ns_slow = st.selectbox("Trend timeframe", ["1h","4h","1d"], index=["1h","4h","1d"].index(S['slow_tf']))
            st.markdown('<div class="setting-help">⬆️ 1d = only strong multi-day trends qualify | ⬇️ 1h = shorter trends qualify (more signals, less conviction). 4h recommended.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── FILTERS ─────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🎯 Filters & Quality Gates</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:10px;"><b>Journal Score Rules:</b> GOD-TIER (90+) always logged. IMMINENT (70+), BUILDING (45+), EARLY (25+) are toggleable below.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4,c5,c6=st.columns(6)
        with c1:
            ns_min = st.slider("Min score to show", 1, 80, S['min_score'])
            st.markdown('<div class="setting-help">⬆️ Raise to only see strong setups (70+) | ⬇️ Lower to 1 to see everything for debugging</div>', unsafe_allow_html=True)
        with c2:
            st.markdown("**Journal Logging:**")
            ns_ji = st.checkbox("🔥 Log IMMINENT (70+)", S.get('j_imminent', True))
            ns_jb = st.checkbox("⚡ Log BUILDING (45+)", S.get('j_building', True))
            ns_je = st.checkbox("📡 Log EARLY (25+)", S.get('j_early', False))
        with c3:
            ns_minr = st.slider("Min signals", 1, 6, S.get('min_reasons', 1))
            st.markdown('<div class="setting-help">⬆️ Raise = coin must have more individual trigger reasons (fewer but higher quality) | ⬇️ Lower = 1 reason is enough</div>', unsafe_allow_html=True)
        with c4:
            ns_btc  = st.toggle("BTC Bear block", S['btc_filter'])
            ns_mom  = st.toggle("Require momentum", S.get('require_momentum', False))
        with c5:
            ns_cd  = st.toggle("Symbol cooldown", S['cooldown_on'])
            ns_cdh = st.slider("Cooldown hrs", 1, 24, S['cooldown_hrs']) if ns_cd else S['cooldown_hrs']
            st.markdown('<div class="setting-help">Cooldown prevents same coin alerting repeatedly. ⬆️ Longer = less spam | ⬇️ Shorter = re-alerts faster</div>', unsafe_allow_html=True)
        with c6:
            ns_min_rr = st.slider("Min R:R ratio", 1.0, 4.0, float(S.get('min_rr', 1.5)), step=0.1)
            st.markdown('<div class="setting-help">⬆️ Raise to only take high reward/risk setups (fewer signals but better quality) | ⬇️ Lower = accept smaller-reward trades. 1.5 min recommended.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── WHALE WALLS ─────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🐋 Whale Wall Detection</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Whale walls are large order book positions. A wall 0.2% away is more significant than 2.5% away.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        with c1:
            ns_whale = st.selectbox("Min wall size (USDT)",
                [50000,100000,250000,500000,1000000,5000000],
                index=[50000,100000,250000,500000,1000000,5000000].index(S['whale_min_usdt']),
                format_func=lambda x: f"${x:,.0f}")
            st.markdown('<div class="setting-help">⬆️ Raise = only massive walls count (fewer signals, higher conviction) | ⬇️ Lower = smaller walls count (more signals)</div>', unsafe_allow_html=True)
        with c2:
            ns_pts_wh_near = st.slider("Points: wall ≤0.5% away", 10, 35, S.get('pts_whale_near', 25))
            st.markdown('<div class="setting-help">Points for a huge wall very close to price. ⬆️ Raise = whale walls dominate score | ⬇️ Lower = whales contribute less</div>', unsafe_allow_html=True)
        with c3:
            ns_pts_wh_mid  = st.slider("Points: wall 0.5–1.5% away", 5, 25, S.get('pts_whale_mid', 15))
        with c4:
            ns_pts_wh_far  = st.slider("Points: wall 1.5–3% away", 3, 15, S.get('pts_whale_far', 8))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── ORDER BOOK ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">⚖️ Order Book Pressure</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Bid/Ask ratio. 2.5× means 2.5× more buy orders than sell. Normal: 0.9–1.3×.</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_ob_l = st.number_input("Low threshold", 0.5, 3.0, S['ob_ratio_low'], 0.05, format="%.2f")
            ns_pts_ob_l = st.slider("Points for low", 1, 15, S['pts_ob_low'])
            st.markdown('<div class="setting-help">⬆️ Raise threshold = only more extreme imbalances trigger | ⬇️ Lower = mild imbalance counts</div>', unsafe_allow_html=True)
        with c2:
            ns_ob_m = st.number_input("Mid threshold", 0.5, 5.0, S['ob_ratio_mid'], 0.1, format="%.2f")
            ns_pts_ob_m = st.slider("Points for mid", 5, 20, S['pts_ob_mid'])
        with c3:
            ns_ob_h = st.number_input("High threshold", 1.0, 10.0, S['ob_ratio_high'], 0.25, format="%.2f")
            ns_pts_ob_h = st.slider("Points for high", 10, 30, S['pts_ob_high'])
        st.markdown('</div>', unsafe_allow_html=True)

        # ── FUNDING ─────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">⚡ Funding Rate Squeeze</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Negative funding = shorts pay longs = short squeeze pressure. <b>Funding Age Context</b> now checks if funding turned negative BEFORE or AFTER the price move — negative funding appearing after a pump may mean shorts are right.</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_fr_l = st.number_input("Low threshold", 0.00001, 0.005, S['funding_low'], 0.00005, format="%.5f")
            ns_pts_fr_l = st.slider("Points low", 1, 15, S['pts_funding_low'], key="pfrl")
            st.markdown('<div class="setting-help">⬆️ Raise threshold = only more negative funding counts | ⬇️ Lower = even tiny negative funding scores</div>', unsafe_allow_html=True)
        with c2:
            ns_fr_m = st.number_input("Mid threshold", 0.0001, 0.01, S['funding_mid'], 0.0001, format="%.5f")
            ns_pts_fr_m = st.slider("Points mid", 5, 20, S['pts_funding_mid'], key="pfrm")
        with c3:
            ns_fr_h = st.number_input("High threshold", 0.0005, 0.05, S['funding_high'], 0.0005, format="%.5f")
            ns_pts_fr_h = st.slider("Points high", 10, 30, S['pts_funding_high'], key="pfrh")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── OI ──────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📈 Open Interest</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">OI history detects actual spikes (% change in last 6h). A 20%+ OI spike is much more meaningful than current OI level alone.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        with c1:
            ns_oi_l = st.number_input("Price chg low %", 0.1, 5.0, S['oi_price_chg_low'], 0.1, format="%.1f")
            ns_pts_oi_l = st.slider("Points low OI", 1, 15, S['pts_oi_low'], key="poil")
            st.markdown('<div class="setting-help">⬆️ Raise = only larger price moves with OI score | ⬇️ Lower = small moves count</div>', unsafe_allow_html=True)
        with c2:
            ns_oi_h = st.number_input("Price chg high %", 0.5, 10.0, S['oi_price_chg_high'], 0.5, format="%.1f")
            ns_pts_oi_h = st.slider("Points high OI", 5, 25, S['pts_oi_high'], key="poih")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── VOLUME SURGE ────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔥 Volume Surge</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Latest candle vs 20-candle average. 2× = double normal volume. <b>Volume Exhaustion Filter:</b> if current volume drops 40%+ below 5c average near recent high, signals distribution phase.</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_vs_l = st.number_input("Low multiplier", 1.0, 3.0, S['vol_surge_low'], 0.1, format="%.1f")
            ns_pts_vs_l = st.slider("Points low vol", 1, 10, S['pts_vol_low'], key="pvl")
            st.markdown('<div class="setting-help">⬆️ Raise = only significant volume surges score | ⬇️ Lower = even mild vol increases count</div>', unsafe_allow_html=True)
        with c2:
            ns_vs_m = st.number_input("Mid multiplier", 1.5, 5.0, S['vol_surge_mid'], 0.25, format="%.1f")
            ns_pts_vs_m = st.slider("Points mid vol", 3, 15, S['pts_vol_mid'], key="pvm")
        with c3:
            ns_vs_h = st.number_input("High multiplier", 2.0, 10.0, S['vol_surge_high'], 0.5, format="%.1f")
            ns_pts_vs_h = st.slider("Points high vol", 5, 20, S['pts_vol_high'], key="pvh")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── LIQUIDITY CLUSTERS ──────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧲 Liquidation Clusters</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_lq_n = st.number_input("Near % (max dist)", 0.5, 5.0, S['liq_cluster_near'], 0.25, format="%.2f")
            ns_pts_lq_n = st.slider("Points near", 5, 20, S['pts_liq_near'], key="pln")
            st.markdown('<div class="setting-help">⬆️ Raise threshold = allow clusters farther away | ⬇️ Lower = only very close clusters count as "near"</div>', unsafe_allow_html=True)
        with c2:
            ns_lq_m = st.number_input("Mid %", 1.0, 10.0, S['liq_cluster_mid'], 0.5, format="%.1f")
            ns_pts_lq_m = st.slider("Points mid", 3, 15, S['pts_liq_mid'], key="plm")
        with c3:
            ns_lq_f = st.number_input("Far %", 2.0, 20.0, S['liq_cluster_far'], 1.0, format="%.1f")
            ns_pts_lq_f = st.slider("Points far", 1, 10, S['pts_liq_far'], key="plf")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── 24H VOLUME ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🌐 24h Volume Activity</div>', unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_v24_l = st.number_input("Low ($)", 100000, 10000000, S['vol24h_low'], 100000, format="%d")
            ns_pts_v24_l = st.slider("Points", 1, 8, S['pts_vol24_low'], key="pv24l")
        with c2:
            ns_v24_m = st.number_input("Mid ($)", 1000000, 50000000, S['vol24h_mid'], 1000000, format="%d")
            ns_pts_v24_m = st.slider("Points", 2, 12, S['pts_vol24_mid'], key="pv24m")
        with c3:
            ns_v24_h = st.number_input("High ($)", 5000000, 200000000, S['vol24h_high'], 5000000, format="%d")
            ns_pts_v24_h = st.slider("Points", 4, 15, S['pts_vol24_high'], key="pv24h")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── TECHNICALS ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📊 Technical Indicators</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:10px;"><b>RSI Direction (NEW):</b> Now tracks whether RSI is rising or falling over last 3 candles. RSI falling on a LONG setup = -8pts. RSI rising on SHORT = -8pts. RSI in the right direction = +8pts.</div>', unsafe_allow_html=True)
        c1,c2,c3,c4,c5=st.columns(5)
        with c1:
            ns_rsi_os = st.slider("RSI oversold (LONG)", 20, 55, S['rsi_oversold'])
            st.markdown('<div class="setting-help">⬆️ Raise = more coins qualify as oversold (more LONG signals) | ⬇️ Lower = only deeply oversold coins score</div>', unsafe_allow_html=True)
        with c2:
            ns_rsi_ob = st.slider("RSI overbought (SHORT)", 45, 80, S['rsi_overbought'])
            st.markdown('<div class="setting-help">⬇️ Lower = more coins qualify as overbought (more SHORT signals) | ⬆️ Raise = only extreme overbought</div>', unsafe_allow_html=True)
        with c3:
            ns_pts_macd = st.slider("MACD pts", 1, 10, S['pts_macd'])
        with c4:
            ns_pts_rsi = st.slider("RSI pts", 1, 10, S['pts_rsi'])
        with c5:
            ns_pts_bb  = st.slider("BB pts", 1, 10, S['pts_bb'])
            ns_pts_ema = st.slider("EMA pts (base)", 1, 8, S['pts_ema'])
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SENTIMENT ───────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧠 OKX / Gate Sentiment Scoring</div>', unsafe_allow_html=True)
        c1,c2=st.columns(2)
        with c1:
            ns_pts_sent  = st.slider("Smart money divergence pts", 5, 30, S.get('pts_sentiment', 20))
            st.markdown('<div class="setting-help">⬆️ Raise = smart money positioning dominates score | ⬇️ Lower = sentiment is a minor factor</div>', unsafe_allow_html=True)
        with c2:
            ns_pts_taker = st.slider("Taker buy/sell pts", 3, 20, S.get('pts_taker', 10))
            st.markdown('<div class="setting-help">⬆️ Raise = aggressive taker orders dominate score | ⬇️ Lower = taker flow is minor input</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── ACCURACY & FILTERS ──────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">🎯 Accuracy Filters — Cut False Positives</div>', unsafe_allow_html=True)
        af1, af2, af3 = st.columns(3)
        with af1:
            ns_min_vol    = st.number_input("Min 24h volume ($)", 0, 5_000_000, int(S.get('min_vol_filter', 300_000)), 50_000, format="%d")
            st.markdown('<div class="setting-help">⬆️ Raise = only liquid coins pass (fewer, more tradable signals) | ⬇️ Lower = small-cap coins included (more signals, higher slippage risk)</div>', unsafe_allow_html=True)
            ns_min_sigs   = st.slider("Min signal categories", 1, 6, int(S.get('min_active_signals', 3)))
            st.markdown('<div class="setting-help">⬆️ Raise = coin must have signals from more categories simultaneously (fewer but higher conviction) | ⬇️ Lower = 1 category is enough</div>', unsafe_allow_html=True)
            ns_dedup      = st.toggle("Dedup coins across exchanges", S.get('dedup_symbols', True))
        with af2:
            ns_atr_min    = st.number_input("ATR min % (flatline filter)", 0.0, 2.0, float(S.get('atr_min_pct', 0.2)), 0.05, format="%.2f")
            st.markdown('<div class="setting-help">⬆️ Raise = skip slowly-moving coins (fewer but more volatile signals) | ⬇️ Lower = include near-flatliners</div>', unsafe_allow_html=True)
            ns_atr_max    = st.number_input("ATR max % (chaos filter)", 2.0, 20.0, float(S.get('atr_max_pct', 10.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">⬇️ Lower = exclude very volatile/chaotic coins | ⬆️ Raise = include more volatile setups</div>', unsafe_allow_html=True)
            ns_spread_max = st.number_input("Max spread % (0=off)", 0.0, 2.0, float(S.get('spread_max_pct', 0.5)), 0.05, format="%.2f")
        with af3:
            ns_fng_lt     = st.slider("F&G LONG min (extreme fear)", 10, 45, int(S.get('fng_long_threshold', 30)))
            st.markdown('<div class="setting-help">When F&G < this value, LONG signals need score ≥60. ⬆️ Raise = stricter filter in fear markets | ⬇️ Lower = more permissive</div>', unsafe_allow_html=True)
            ns_fng_st     = st.slider("F&G SHORT min (extreme greed)", 55, 90, int(S.get('fng_short_threshold', 70)))
            ns_mtf        = st.toggle("Multi-timeframe confirmation", S.get('mtf_confirm', True))

        af4, af5 = st.columns(2)
        with af4:
            ns_pts_mtf    = st.slider("MTF alignment pts", 0, 20, int(S.get('pts_mtf', 12)))
            st.markdown('<div class="setting-help">⬆️ Raise = reward for all 3 TFs agreeing is bigger (MTF coins dominate results) | 0 = disable MTF</div>', unsafe_allow_html=True)
            ns_pts_div    = st.slider("RSI divergence pts", 0, 20, int(S.get('pts_divergence', 10)))
        with af5:
            ns_pts_cpat   = st.slider("Candle pattern pts", 0, 15, int(S.get('pts_candle_pattern', 8)))
            ns_pts_combo  = st.slider("OI+Funding combo bonus pts", 0, 20, int(S.get('pts_oi_funding_combo', 10)))
            ns_vol_exp_t  = st.number_input("Explosive vol threshold (×avg)", 3.0, 20.0, float(S.get('vol_surge_explosive', 5.0)), 0.5, format="%.1f")
            ns_pts_vol_e  = st.slider("Explosive vol pts", 10, 30, int(S.get('pts_vol_explosive', 20)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SENTINEL ──────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#7c3aed;"><div class="stg-title" style="color:#7c3aed;">🛰️ Sentinel Mode — Always-On Top-100 Scanner</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:12px;">Sentinel scans the <b>top 100 coins by volume</b> continuously. It never scans the full list — only top 100 for speed and reliability. Each rerun checks a small batch (5 coins). Fire and forget.</div>', unsafe_allow_html=True)
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            ns_sent_score = st.slider("Alert threshold score", 40, 95, int(S.get('sentinel_score_threshold', 70)))
            st.markdown('<div class="setting-help">⬆️ Raise = only high-confidence sentinel alerts | ⬇️ Lower = more frequent but weaker signals</div>', unsafe_allow_html=True)
        with sc2:
            ns_sent_batch = st.slider("Coins per rerun batch", 2, 20, int(S.get('sentinel_batch_size', 5)))
        with sc3:
            ns_sent_interval = st.slider("Seconds between batches", 5, 120, int(S.get('sentinel_check_interval', 30)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── SOCIAL MEDIA ───────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">📡 Social Media — Reddit Buzz Scoring</div>', unsafe_allow_html=True)
        so1, so2, so3, so4 = st.columns(4)
        with so1: ns_social_en = st.toggle("Enable Reddit Buzz", S.get('social_enabled', True))
        with so2:
            ns_social_wt = st.slider("Max Reddit score pts", 3, 20, int(S.get('social_reddit_weight', 8)))
            st.markdown('<div class="setting-help">⬆️ Raise = social buzz can push score more | ⬇️ Lower = social is minor factor</div>', unsafe_allow_html=True)
        with so3: ns_social_min = st.slider("Min mentions to score", 1, 10, int(S.get('social_min_mentions', 3)))
        with so4: ns_social_buzz = st.slider("Mentions for max pts", 5, 50, int(S.get('social_buzz_threshold', 10)))
        ns_apify = st.text_input("Apify Token (optional)", S.get('apify_token', ''), type="password")
        st.markdown('</div>', unsafe_allow_html=True)

        # ── CLASSIFIER THRESHOLDS ─────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🏷️ Tab Classifier Thresholds</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint" style="margin-bottom:10px;">Controls which tab coins appear in. Lower thresholds = more coins in each tab. Higher = only the strongest qualify.</div>', unsafe_allow_html=True)
        _ca, _cb, _cc = st.columns(3)
        with _ca:
            st.caption('🟡 Breakout')
            ns_br_oi  = st.slider('Min OI spike pts', 1, 20, int(S.get('cls_breakout_oi_min', 7)), key='br_oi')
            ns_br_vol = st.slider('Min Vol surge pts', 1, 15, int(S.get('cls_breakout_vol_min', 4)), key='br_vol')
            ns_br_sc  = st.slider('Min score', 10, 60, int(S.get('cls_breakout_score_min', 25)), key='br_sc')
        with _cb:
            st.caption('🔴 Squeeze')
            ns_sq_fd  = st.slider('Min Funding pts', 1, 20, int(S.get('cls_squeeze_fund_min', 8)), key='sq_fd')
            ns_sq_ob  = st.slider('Min OB imbalance pts', 1, 15, int(S.get('cls_squeeze_ob_min', 6)), key='sq_ob')
        with _cc:
            st.caption('ℹ️ Note')
            st.info('Coins not matching Squeeze/Breakout/Whale criteria fall into Early tab.')
        st.markdown('</div>', unsafe_allow_html=True)

        # ── AUTO-JOURNAL ───────────────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">📒 Auto-Journal Exit Tracking</div>', unsafe_allow_html=True)
        aj1, aj2 = st.columns(2)
        with aj1:
            ns_aj_on   = st.toggle("Enable Auto-Journal Tracking", S.get('journal_autocheck_on', True))
            ns_aj_mins = st.slider("Check interval (minutes)", 1, 60, int(S.get('journal_autocheck_mins', 15)))
        with aj2:
            _last_j = st.session_state.get('journal_last_autocheck', 0)
            if _last_j:
                _next_j   = _last_j + S.get('journal_autocheck_mins', 15) * 60
                _secs_lft = max(0, int(_next_j - time.time()))
                st.metric("Next auto-check in", f"{_secs_lft//60}m {_secs_lft%60}s")
            ns_aj_force = st.checkbox("🔄 Force check on Save", value=False)
        st.markdown('</div>', unsafe_allow_html=True)

        # ── INTELLIGENCE SIGNALS ───────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🧠 Intelligence Signals — Tier 1 & 2</div>', unsafe_allow_html=True)
        ti1, ti2, ti3 = st.columns(3)
        with ti1:
            ns_of_lb  = st.slider("Order flow lookback (candles)", 5, 30, int(S.get('orderflow_lookback', 10)))
            ns_pts_of = st.slider("Order flow pts", 3, 20, int(S.get('pts_orderflow', 12)))
            st.markdown('<div class="setting-help">⬆️ Raise lookback = smoother/slower signal | ⬇️ Lower = faster but noisier. Points: ⬆️ raise = order flow dominates score</div>', unsafe_allow_html=True)
        with ti2:
            ns_pts_lm  = st.slider("Liq map pts", 3, 25, int(S.get('pts_liq_map', 15)))
            ns_pts_lst = st.slider("New listing bonus pts", 5, 35, int(S.get('listing_alert_pts', 25)))
        with ti3:
            ns_oc_min  = st.number_input("On-chain whale min ($)", 100_000, 5_000_000, int(S.get('onchain_whale_min', 500_000)), 100_000, format="%d")
            ns_pts_oc  = st.slider("On-chain whale pts", 3, 25, int(S.get('pts_onchain_whale', 15)))
        st.markdown('</div>', unsafe_allow_html=True)

        # ── APIs ────────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">🔑 API Keys</div>', unsafe_allow_html=True)
        c1,c2=st.columns(2)
        with c1:
            ns_tg_tok     = st.text_input("Telegram Bot Token", S.get('tg_token',''), type="password")
            ns_tg_cid     = st.text_input("Telegram Chat ID", S.get('tg_chat_id',''))
            ns_okx_key    = st.text_input("OKX API Key (optional)", S.get('okx_key',''), type="password")
            ns_okx_sec    = st.text_input("OKX API Secret (optional)", S.get('okx_secret',''), type="password")
            ns_okx_pass   = st.text_input("OKX Passphrase (optional)", S.get('okx_passphrase',''), type="password")
        with c2:
            ns_dc_web     = st.text_input("Discord Webhook URL", S.get('discord_webhook',''), type="password")
            ns_cmc        = st.text_input("CMC API Key (optional)", S.get('cmc_key',''), type="password")
            ns_gate_key   = st.text_input("Gate.io API Key (optional)", S.get('gate_key',''), type="password")
            ns_gate_sec   = st.text_input("Gate.io API Secret (optional)", S.get('gate_secret',''), type="password")
        st.markdown('</div>', unsafe_allow_html=True)

        submitted = st.form_submit_button("💾  SAVE ALL SETTINGS", use_container_width=True)
        if submitted:
            new_s = {
                'scan_depth': ns_depth, 'fast_tf': ns_fast, 'slow_tf': ns_slow,
                'scan_modes': selected_modes,
                'min_score': ns_min, 'j_imminent': ns_ji, 'j_building': ns_jb, 'j_early': ns_je,
                'min_reasons': ns_minr, 'btc_filter': ns_btc, 'require_momentum': ns_mom,
                'cooldown_on': ns_cd, 'cooldown_hrs': ns_cdh, 'whale_min_usdt': ns_whale,
                'min_rr': ns_min_rr,
                'alert_min_score': ns_alert_score, 'alert_longs': ns_al_long,
                'alert_shorts': ns_al_short, 'alert_squeeze': ns_al_sq,
                'alert_breakout': ns_al_br, 'alert_early': ns_al_ea, 'alert_whale': ns_al_wh,
                'late_entry_chg_thresh': ns_late_thresh, 'late_entry_penalty': ns_late_pen,
                'vol_exhaust_penalty': ns_exhaust_pen, 'near_top_penalty': ns_near_top_pen,
                'use_4h_ob_for_sl': ns_use_4h_ob,
                'ob_ratio_low': ns_ob_l, 'ob_ratio_mid': ns_ob_m, 'ob_ratio_high': ns_ob_h,
                'pts_ob_low': ns_pts_ob_l, 'pts_ob_mid': ns_pts_ob_m, 'pts_ob_high': ns_pts_ob_h,
                'funding_low': ns_fr_l, 'funding_mid': ns_fr_m, 'funding_high': ns_fr_h,
                'pts_funding_low': ns_pts_fr_l, 'pts_funding_mid': ns_pts_fr_m, 'pts_funding_high': ns_pts_fr_h,
                'oi_price_chg_low': ns_oi_l, 'oi_price_chg_high': ns_oi_h,
                'pts_oi_low': ns_pts_oi_l, 'pts_oi_high': ns_pts_oi_h,
                'vol_surge_low': ns_vs_l, 'vol_surge_mid': ns_vs_m, 'vol_surge_high': ns_vs_h,
                'pts_vol_low': ns_pts_vs_l, 'pts_vol_mid': ns_pts_vs_m, 'pts_vol_high': ns_pts_vs_h,
                'liq_cluster_near': ns_lq_n, 'liq_cluster_mid': ns_lq_m, 'liq_cluster_far': ns_lq_f,
                'pts_liq_near': ns_pts_lq_n, 'pts_liq_mid': ns_pts_lq_m, 'pts_liq_far': ns_pts_lq_f,
                'vol24h_low': ns_v24_l, 'vol24h_mid': ns_v24_m, 'vol24h_high': ns_v24_h,
                'pts_vol24_low': ns_pts_v24_l, 'pts_vol24_mid': ns_pts_v24_m, 'pts_vol24_high': ns_pts_v24_h,
                'rsi_oversold': ns_rsi_os, 'rsi_overbought': ns_rsi_ob,
                'pts_macd': ns_pts_macd, 'pts_rsi': ns_pts_rsi, 'pts_bb': ns_pts_bb, 'pts_ema': ns_pts_ema,
                'pts_session': S['pts_session'],
                'pts_sentiment': ns_pts_sent, 'pts_taker': ns_pts_taker,
                'pts_whale_near': ns_pts_wh_near, 'pts_whale_mid': ns_pts_wh_mid, 'pts_whale_far': ns_pts_wh_far,
                'min_vol_filter': ns_min_vol, 'min_active_signals': ns_min_sigs, 'dedup_symbols': ns_dedup,
                'atr_min_pct': ns_atr_min, 'atr_max_pct': ns_atr_max, 'spread_max_pct': ns_spread_max,
                'fng_long_threshold': ns_fng_lt, 'fng_short_threshold': ns_fng_st, 'mtf_confirm': ns_mtf,
                'pts_mtf': ns_pts_mtf, 'pts_divergence': ns_pts_div, 'pts_candle_pattern': ns_pts_cpat,
                'pts_oi_funding_combo': ns_pts_combo, 'vol_surge_explosive': ns_vol_exp_t, 'pts_vol_explosive': ns_pts_vol_e,
                'sentinel_score_threshold': ns_sent_score, 'sentinel_batch_size': ns_sent_batch, 'sentinel_check_interval': ns_sent_interval,
                'cls_breakout_oi_min': ns_br_oi, 'cls_breakout_vol_min': ns_br_vol,
                'cls_breakout_score_min': ns_br_sc, 'cls_squeeze_fund_min': ns_sq_fd,
                'cls_squeeze_ob_min': ns_sq_ob,
                'journal_autocheck_on': ns_aj_on, 'journal_autocheck_mins': ns_aj_mins,
                'orderflow_lookback': ns_of_lb, 'pts_orderflow': ns_pts_of,
                'pts_liq_map': ns_pts_lm, 'listing_alert_pts': ns_pts_lst,
                'onchain_whale_min': ns_oc_min, 'pts_onchain_whale': ns_pts_oc,
                'social_enabled': ns_social_en, 'social_reddit_weight': ns_social_wt, 'social_min_mentions': ns_social_min, 'social_buzz_threshold': ns_social_buzz, 'apify_token': ns_apify,
                'cmc_key': ns_cmc, 'tg_token': ns_tg_tok, 'tg_chat_id': ns_tg_cid,
                'discord_webhook': ns_dc_web,
                'okx_key': ns_okx_key, 'okx_secret': ns_okx_sec, 'okx_passphrase': ns_okx_pass,
                'gate_key': ns_gate_key, 'gate_secret': ns_gate_sec,
                'auto_scan': q_auto, 'auto_interval': q_auto_int,
            }
            save_settings(new_s)
            if ns_aj_force:
                st.session_state.journal_last_autocheck = 0
                st.success("✅ Settings saved! Journal check will run on next page load.")
            else:
                st.success("✅ Settings saved!")
            st.balloons()
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: JOURNAL
# ═══════════════════════════════════════════════════════════════════════════
if nav == "📒 Journal":
    ensure_journal()
    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown('<div class="section-h">Trade Journal & Tracking Dashboard</div>', unsafe_allow_html=True)
    with col2:
        if st.button("🗑️ Clear Journal"):
            if os.path.exists(JOURNAL_FILE): os.remove(JOURNAL_FILE)
            st.rerun()

    try: df_j = pd.read_csv(JOURNAL_FILE)
    except: df_j = pd.DataFrame()

    if df_j.empty:
        st.markdown('<div class="empty-st"><div style="font-size:2rem;opacity:.2;margin-bottom:10px;">📒</div>No signals logged yet</div>', unsafe_allow_html=True)
    else:
        df_j['ts'] = pd.to_datetime(df_j['ts'], errors='coerce')
        now = datetime.now()
        df_24h = df_j[df_j['ts'] >= (now - timedelta(hours=24))]

        longs    = len(df_24h[df_24h['type']=='LONG'])
        shorts   = len(df_24h[df_24h['type']=='SHORT'])
        tps      = len(df_24h[df_24h['status']=='TP'])
        sls      = len(df_24h[df_24h['status']=='SL'])
        active   = len(df_24h[df_24h['status']=='ACTIVE'])
        win_rate = (tps / (tps + sls) * 100) if (tps+sls) > 0 else 0

        j_imm = S.get('j_imminent', True)
        j_bld = S.get('j_building', True)
        j_ear = S.get('j_early', False)
        st.info(f"**Journal Logging Rules:** GOD-TIER (90+) always | IMMINENT (70+): {'✅' if j_imm else '❌'} | BUILDING (45+): {'✅' if j_bld else '❌'} | EARLY (25+): {'✅' if j_ear else '❌'} — Change in ⚙️ Settings")

        st.markdown(f"""<div class="stat-strip">
          <div><div class="ss-val">{len(df_24h)}</div><div class="ss-lbl">24h Total</div></div>
          <div><div class="ss-val">{longs} / {shorts}</div><div class="ss-lbl">Longs / Shorts</div></div>
          <div><div class="ss-val" style="color:var(--amber);">{active}</div><div class="ss-lbl">Active</div></div>
          <div><div class="ss-val" style="color:var(--green);">{tps}</div><div class="ss-lbl">TP Hits</div></div>
          <div><div class="ss-val" style="color:var(--red);">{sls}</div><div class="ss-lbl">SL Hits</div></div>
          <div><div class="ss-val" style="color:var(--blue);">{win_rate:.1f}%</div><div class="ss-lbl">Win Rate</div></div>
        </div>""", unsafe_allow_html=True)

        fc1,fc2,fc3,fc4,fc5 = st.columns(5)
        with fc1: jt      = st.selectbox("Type", ["ALL","LONG","SHORT"])
        with fc2: jc      = st.selectbox("Class", ["ALL","squeeze","breakout","whale_driven","early"])
        with fc3: js_stat = st.selectbox("Status", ["ALL","ACTIVE","TP","SL"])
        with fc4: jx      = st.selectbox("Exchange", ["ALL","OKX","GATE","MEXC"])
        with fc5: js      = st.selectbox("Sort By", ["ts","pump_score","symbol"])

        dv = df_j.copy()
        if jt != "ALL" and 'type' in dv.columns:     dv = dv[dv['type']==jt]
        if jc != "ALL" and 'class' in dv.columns:    dv = dv[dv['class']==jc]
        if js_stat != "ALL" and 'status' in dv.columns: dv = dv[dv['status']==js_stat]
        if jx != "ALL" and 'exchange' in dv.columns: dv = dv[dv['exchange']==jx]
        if js in dv.columns: dv = dv.sort_values(js, ascending=(js!='pump_score'))

        st.dataframe(dv, use_container_width=True, height=500)
        st.download_button("⬇️ Export CSV", dv.to_csv(index=False).encode(),
            file_name=f"apex_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
    st.stop()


# ─── AUTO-JOURNAL CHECK ─────────────────────────────────────────────────────
autocheck_journal_background(S)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SCANNER (continued)
# ═══════════════════════════════════════════════════════════════════════════
eff_s = S.copy()
eff_s['scan_depth']       = q_depth
eff_s['min_score']        = q_min
eff_s['btc_filter']       = q_btc
eff_s['require_momentum'] = q_mom
eff_s['auto_scan']        = q_auto
eff_s['auto_interval']    = q_auto_int
eff_s['scan_modes']       = selected_modes

col_btn, _ = st.columns([2, 5])
with col_btn: do_scan = st.button("⚡  RUN PUMP/DUMP SCAN", use_container_width=True)
if eff_s.get('auto_scan'): do_scan = True; time.sleep(0.3)

if do_scan:
    try:
        screener = PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''),
            okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''),
            okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''),
            gate_secret=eff_s.get('gate_secret','')
        )
        raw_results, btc_t, btc_p, btc_r, scan_errs = asyncio.run(screener.run(eff_s))
        st.session_state.last_raw_count = len(raw_results)
        st.session_state.prev_results = {
            f"{r['symbol']}_{r['type']}": r['pump_score']
            for r in st.session_state.results}
        st.session_state.results = [r for r in raw_results if r['pump_score'] >= eff_s['min_score']]
        st.session_state.last_scan  = datetime.now().strftime('%H:%M:%S')
        st.session_state.scan_count += 1
        st.session_state.btc_price  = btc_p
        st.session_state.btc_trend  = btc_t
        st.session_state.scan_errors = scan_errs

        for r in st.session_state.results:
            ak   = f"{r['symbol']}_{r['type']}_{datetime.now().hour}"
            lbl  = pump_label(r['pump_score'], r['type'])
            prev = st.session_state.prev_results
            sym_key = f"{r['symbol']}_{r['type']}"
            is_new_signal = sym_key not in prev
            prev_score    = prev.get(sym_key, 0)
            score_jumped  = (r['pump_score'] - prev_score) >= 15
            r['is_new']   = is_new_signal
            r['score_jump'] = r['pump_score'] - prev_score if prev_score else 0
            score_bracket = r['pump_score'] // 15
            ak_recheck = f"{r['symbol']}_{r['type']}_{datetime.now().hour}_{score_bracket}"

            # ── JOURNAL LOGGING ────────────────────────────────────────────
            if ak not in st.session_state.logged_sigs:
                log_it = False
                if "GOD-TIER" in lbl: log_it = True
                elif "IMMINENT" in lbl and eff_s.get('j_imminent', True): log_it = True
                elif "BUILDING" in lbl and eff_s.get('j_building', True): log_it = True
                elif "EARLY" in lbl and eff_s.get('j_early', False): log_it = True
                if log_it: log_trade(r)
                st.session_state.logged_sigs.add(ak)

            # ── NOTIFICATION CHECK ─────────────────────────────────────────
            send_alert = False
            if r['pump_score'] >= eff_s.get('alert_min_score', 60):
                type_ok  = (r['type']=='LONG' and eff_s.get('alert_longs',True)) or \
                           (r['type']=='SHORT' and eff_s.get('alert_shorts',True))
                cls_ok   = (r['cls']=='squeeze'     and eff_s.get('alert_squeeze',True))  or \
                           (r['cls']=='breakout'     and eff_s.get('alert_breakout',True)) or \
                           (r['cls']=='whale_driven' and eff_s.get('alert_whale',True))    or \
                           (r['cls']=='early'        and eff_s.get('alert_early',False))   or \
                           r['cls'] not in ('squeeze','breakout','whale_driven','early')
                if type_ok and cls_ok: send_alert = True

            if send_alert:
                if 'alerted_sigs' not in st.session_state: st.session_state.alerted_sigs = set()
                if ak_recheck not in st.session_state.alerted_sigs:
                    bd_r = r.get('signal_breakdown', {})
                    sentiment = r.get('sentiment', {})

                    def epip(v,lo=5,hi=12):
                        if v>=hi: return "🟩"
                        if v>=lo: return "🟨"
                        return "⬛"

                    pips_str = (f"OB {epip(bd_r.get('ob_imbalance',0),4,14)} | FD {epip(bd_r.get('funding',0)+bd_r.get('funding_hist',0),3,15)} | OI {epip(bd_r.get('oi_spike',0),5,14)}\n"
                                f"VOL {epip(bd_r.get('vol_surge',0),3,10)} | LQ {epip(bd_r.get('liq_cluster',0),4,12)} | WH {epip(bd_r.get('whale_wall',0),4,7)} | SENT {epip(bd_r.get('sentiment',0),8,20)}")
                    sent_line = ""
                    if sentiment.get('available'):
                        sent_line = f"\n📊 {sentiment.get('source','Exchange')} L/S: {sentiment['top_long_pct']:.0f}% / {sentiment['top_short_pct']:.0f}% | Taker Buy: {sentiment['taker_buy_pct']:.0f}%"
                    mom_line = "\n✅ MOMENTUM CONFIRMED — price already moving" if r.get('momentum_confirmed') else ""
                    penalty_line = ("\n⚠️ PENALTIES: " + " | ".join(r['penalty_flags'])) if r.get('penalty_flags') else ""
                    reasons_str = "\n".join([f"▸ {rsn}" for rsn in r['reasons'][:6]])
                    rr_ratio = f"{abs(r['tp']-r['price'])/abs(r['price']-r['sl']):.1f}:1" if r.get('sl') else "N/A"
                    oi_ch = r.get('oi_change_6h', 0)
                    oi_line = f"\n📈 OI Change 6h: {oi_ch:+.1f}%" if abs(oi_ch) >= 5 else ""
                    _tp1 = float(r.get('tp1') or r.get('tp') or 0)
                    _tp2 = float(r.get('tp2') or r.get('tp') or 0)
                    _tp3 = float(r.get('tp3') or r.get('tp') or 0)
                    _elo = float(r.get('entry_lo') or r['price'])
                    _ehi = float(r.get('entry_hi') or r['price'])
                    _new_flag  = '🆕 NEW SIGNAL\n' if is_new_signal else ''
                    _jump_flag = f'⬆️ Score jumped +{r["score_jump"]}pts\n' if score_jumped and prev_score else ''

                    # SL method label
                    _ob4h = r.get('ob4h_data', {})
                    _sl_method = "4H OB" if (_ob4h.get('bid_ob') if r['type']=='LONG' else _ob4h.get('ask_ob')) else "Structure"

                    # ── TELEGRAM — LONG/SHORT bold at top with score ──────
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        sig_line = "📗 <b>LONG</b>" if r['type']=='LONG' else "📕 <b>SHORT</b>"
                        rsi_arrow = {"RISING": "↑", "FALLING": "↓", "FLAT": "→"}.get(r.get('rsi_direction',''), "")
                        msg_tg = (
                            f'{_new_flag}{_jump_flag}'
                            f'{sig_line}  |  <b>Score: {r["pump_score"]}/100</b>  |  {lbl}\n'
                            f'<b>═══ {r["symbol"]} — {r["cls"].upper()} ({r.get("exchange","MEXC")}) ═══</b>\n'
                            f'━━━━━━━━━━━━━━━━━━\n'
                            f'RSI: {r.get("rsi",0):.1f}{rsi_arrow}  |  R:R: {rr_ratio}\n'
                            f'📍 <b>Entry Zone:</b> ${_elo:.6f} – ${_ehi:.6f}\n'
                            f'🎯 <b>TP1 (scalp):</b> ${_tp1:.6f}\n'
                            f'✅ <b>TP2 (target):</b> ${_tp2:.6f}\n'
                            f'🚀 <b>TP3 (max run):</b> ${_tp3:.6f}\n'
                            f'🛑 <b>SL [{_sl_method}]:</b> ${r["sl"]:.6f}\n'
                            f'{oi_line}{sent_line}{mom_line}{penalty_line}\n\n'
                            f'📊 <b>Signals:</b>\n{pips_str}\n\n'
                            f'📝 <b>Key Drivers:</b>\n{reasons_str}'
                        )
                        send_tg(eff_s['tg_token'], eff_s['tg_chat_id'], msg_tg)

                    # ── DISCORD — LONG/SHORT bold at top with score ───────
                    if eff_s.get('discord_webhook'):
                        dc_color = 0x059669 if r['type']=='LONG' else 0xdc2626
                        sig_header = ("📗 **LONG**" if r['type']=='LONG' else "📕 **SHORT**") + f"  |  Score: **{r['pump_score']}/100**  |  {lbl}"
                        rsi_arrow_dc = {"RISING": "↑", "FALLING": "↓", "FLAT": "→"}.get(r.get('rsi_direction',''), "")
                        penalty_dc = ("\n⚠️ **PENALTIES:** " + " | ".join(r['penalty_flags'])) if r.get('penalty_flags') else ""
                        stats_text = (
                            f"{sig_header}\n"
                            f"{'🆕 **NEW SIGNAL**' + chr(10) if is_new_signal else ''}"
                            f"{'⬆️ **Score jumped +' + str(r['score_jump']) + 'pts**' + chr(10) if score_jumped and prev_score else ''}"
                            f"**Exchange:** {r.get('exchange','MEXC')} | **RSI:** {r.get('rsi',0):.1f}{rsi_arrow_dc} | **R:R:** {rr_ratio}\n"
                            f"📍 **Entry Zone:** `${_elo:.6f}` – `${_ehi:.6f}`\n"
                            f"🎯 **TP1:** `${_tp1:.6f}` | ✅ **TP2:** `${_tp2:.6f}` | 🚀 **TP3:** `${_tp3:.6f}`\n"
                            f"🛑 **SL [{_sl_method}]:** `${r['sl']:.6f}`"
                            + (f"\n**OI 6h:** {oi_ch:+.1f}%" if abs(oi_ch) >= 5 else "")
                            + (f"\n**{sentiment.get('source','Exchange')} L/S:** {sentiment['top_long_pct']:.0f}% Long / {sentiment['top_short_pct']:.0f}% Short" if sentiment.get('available') else "")
                            + penalty_dc
                        )
                        dc_embed = {
                            'title': f'{r["symbol"]} ({r["cls"].upper()}) — {r.get("exchange","MEXC")}',
                            'color': dc_color,
                            'description': stats_text,
                            'fields': [
                                {'name': '📊 Signal Breakdown', 'value': pips_str, 'inline': False},
                                {'name': '📝 Top Reasons', 'value': reasons_str, 'inline': False}
                            ],
                            'footer': {'text': f'APEX Intelligence Terminal • {datetime.now(timezone.utc).strftime("%H:%M:%S")} UTC'}
                        }
                        send_discord(eff_s['discord_webhook'], dc_embed)

                    st.session_state.alerted_sigs.add(ak_recheck)
                    _ch = []
                    if eff_s.get('discord_webhook'): _ch.append('Discord')
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'): _ch.append('Telegram')
                    _jump_note = f' ⬆️ +{r["score_jump"]}pt jump' if score_jumped and prev_score else ''
                    _new_note  = ' 🆕 NEW SIGNAL' if is_new_signal else ''
                    if _ch:
                        st.toast(f'📨{_new_note}{_jump_note} {r["symbol"]} → {"+".join(_ch)}', icon='📨')
                    else:
                        st.toast(f'⚠️ {r["symbol"]} scored {r["pump_score"]} but no Discord/Telegram configured!', icon='⚠️')

    except Exception as e:
        if any(x in str(e) for x in ["510","429","too frequent"]): st.error("🚦 Rate limited — wait 60s")
        else: st.error(f"Error: {e}")


# ─── SENTINEL RUNNER ─────────────────────────────────────────────────────────
# Sentinel always scans TOP 100 coins by volume only
if st.session_state.get('sentinel_active') and do_scan and st.session_state.scan_count > 0:
    sent_ph = st.empty()
    chk_now  = st.session_state.get('sentinel_total_checked', 0)
    sig_now  = st.session_state.get('sentinel_signals_found', 0)
    last_now = st.session_state.get('sentinel_last_check', 'starting…')
    sent_ph.markdown(
        f'<div style="background:#0f1117;border-radius:8px;padding:10px 18px;'
        f'margin-bottom:10px;border:1px solid #7c3aed44;">'
        f'<span style="color:#7c3aed;font-family:monospace;font-size:.65rem;font-weight:700;">🛰️ SENTINEL — TOP 100 SCANNING…</span>'
        f'<span style="font-family:monospace;font-size:.6rem;color:#9ca3af;margin-left:12px;">{chk_now} checked</span>'
        f'<span style="font-family:monospace;font-size:.62rem;color:#ff4444;font-weight:700;margin-left:12px;">{sig_now} signals</span>'
        f'<span style="font-family:monospace;font-size:.58rem;color:#9ca3af;margin-left:12px;">Last: {last_now}</span>'
        '</div>',
        unsafe_allow_html=True)

    try:
        try:
            import nest_asyncio
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            try:
                _loop = asyncio.get_event_loop()
                if _loop.is_closed():
                    _loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(_loop)
            except RuntimeError:
                _loop = asyncio.new_event_loop()
                asyncio.set_event_loop(_loop)
            nest_asyncio.apply(_loop)
        except Exception:
            pass

        s_screener = PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''), okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''), okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''), gate_secret=eff_s.get('gate_secret',''))

        async def sentinel_tick(screener, s):
            btc_trend, btc_px, _ = await screener.fetch_btc()
            await asyncio.gather(
                screener.okx.load_markets(),
                screener.mexc.load_markets(),
                screener.gate.load_markets(),
                return_exceptions=True)
            tk_okx, tk_mexc, tk_gate = {}, {}, {}
            results_tickers = await asyncio.gather(
                screener.okx.fetch_tickers(),
                screener.mexc.fetch_tickers(),
                screener.gate.fetch_tickers(),
                return_exceptions=True)
            if not isinstance(results_tickers[0], Exception): tk_okx  = results_tickers[0]
            if not isinstance(results_tickers[1], Exception): tk_mexc = results_tickers[1]
            if not isinstance(results_tickers[2], Exception): tk_gate = results_tickers[2]

            # Build universe — TOP 100 by volume only (Sentinel always uses volume ranking)
            all_swaps = {}
            for sym, t in tk_mexc.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'MEXC', 'exch_obj': screener.mexc}
            for sym, t in tk_gate.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'GATE', 'exch_obj': screener.gate}
            for sym, t in tk_okx.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    all_swaps[sym] = {'vol': float(t['quoteVolume'] or 0), 'exch_name': 'OKX', 'exch_obj': screener.okx}

            # SENTINEL: ALWAYS top 100 by volume only
            sorted_swaps = sorted(all_swaps.items(), key=lambda x: x[1]['vol'], reverse=True)[:100]
            uni_size     = len(sorted_swaps)
            checked      = st.session_state.get('sentinel_total_checked', 0)
            batch_sz     = max(1, s.get('sentinel_batch_size', 5))
            start        = checked % max(1, uni_size)
            end          = min(start + batch_sz, uni_size)
            coin_batch   = sorted_swaps[start:end]

            new_sigs = []
            for sym, data in coin_batch:
                try:
                    r = await screener.analyze(data['exch_name'], data['exch_obj'], sym, s, btc_trend)
                    if r and r['pump_score'] >= s.get('sentinel_score_threshold', 70):
                        new_sigs.append(r)
                except Exception:
                    pass

            for exch in [screener.okx, screener.mexc, screener.gate]:
                try: await exch.close()
                except: pass

            return new_sigs, btc_px, uni_size

        new_sigs, s_btc, total_uni = asyncio.run(sentinel_tick(s_screener, eff_s))

        st.session_state.sentinel_total_checked = (
            st.session_state.get('sentinel_total_checked', 0) +
            eff_s.get('sentinel_batch_size', 5))
        st.session_state.sentinel_last_check    = datetime.now().strftime('%H:%M:%S')
        st.session_state.sentinel_universe_size = f"TOP {total_uni}"
        st.session_state.btc_price              = s_btc

        if new_sigs:
            st.session_state.sentinel_signals_found = (
                st.session_state.get('sentinel_signals_found', 0) + len(new_sigs))
            if 'alerted_sigs' not in st.session_state:
                st.session_state.alerted_sigs = set()
            for r in new_sigs:
                st.toast(f"🛰️ {r['symbol']} — {r['pump_score']}/100 {r['type']}", icon="🚨")
                ak = f"{r['symbol']}_{r['type']}_{datetime.now().strftime('%Y%m%d%H')}_sent"
                if 'sentinel_results' not in st.session_state:
                    st.session_state.sentinel_results = []
                sent_syms = {x['symbol'] for x in st.session_state.sentinel_results}
                if r['symbol'] not in sent_syms:
                    st.session_state.sentinel_results.insert(0, r)
                    sent_lbl = pump_label(r['pump_score'], r['type'])
                    if "GOD-TIER" in sent_lbl or "IMMINENT" in sent_lbl:
                        log_trade(r)
                if ak not in st.session_state.alerted_sigs:
                    _p = float(r.get('price') or 0)
                    _t = float(r.get('tp') or 0)
                    _s = float(r.get('sl') or 0)
                    rr_r = f"{abs(_t-_p)/abs(_p-_s):.1f}:1" if abs(_p-_s) > 0 else "N/A"
                    rsns = "\n".join([f"• {x}" for x in r['reasons'][:5]])
                    sig_line_sent = "📗 LONG" if r['type']=='LONG' else "📕 SHORT"
                    _ob4h_s = r.get('ob4h_data', {})
                    _sl_m_s = "4H OB" if (_ob4h_s.get('bid_ob') if r['type']=='LONG' else _ob4h_s.get('ask_ob')) else "Structure"
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        send_tg(eff_s['tg_token'], eff_s['tg_chat_id'],
                            f"<b>🛰️ [SENTINEL] {sig_line_sent} | Score: {r['pump_score']}/100</b>\n"
                            f"<b>{r['symbol']} — {r.get('cls','').upper()} | {r.get('exchange','')}</b>\n"
                            f"R:R {rr_r}\n"
                            f"Entry: ${_p:.6f}  TP: ${_t:.6f}  SL [{_sl_m_s}]: ${_s:.6f}\n{rsns}")
                    if eff_s.get('discord_webhook'):
                        dc_color_s = 0x059669 if r['type']=='LONG' else 0xdc2626
                        send_discord(eff_s['discord_webhook'], {
                            "title": f"🛰️ SENTINEL: {r['symbol']} ({r['pump_score']}/100)",
                            "color": dc_color_s,
                            "description": (
                                f"{'📗 **LONG**' if r['type']=='LONG' else '📕 **SHORT**'}  |  Score: **{r['pump_score']}/100**\n"
                                f"**Exchange:** {r.get('exchange','')} | **Class:** {r.get('cls','').upper()} | "
                                f"**R:R:** {rr_r}\n"
                                f"Entry: `${_p:.6f}` | TP: `${_t:.6f}` | SL [{_sl_m_s}]: `${_s:.6f}`\n\n"
                                f"{rsns}"),
                            "footer": {"text": f"APEX Sentinel — Top 100 | {datetime.now(timezone.utc).strftime('%H:%M UTC')}"}})
                    st.session_state.alerted_sigs.add(ak)

        chk_f    = st.session_state.get('sentinel_total_checked', 0)
        sig_f    = st.session_state.get('sentinel_signals_found', 0)
        last_f   = st.session_state.get('sentinel_last_check', '?')
        pct_done = int((chk_f % max(1, total_uni)) / max(1, total_uni) * 100)
        sent_ph.markdown(
            f'<div style="background:#0f1117;border-radius:8px;padding:10px 18px;'
            f'margin-bottom:10px;border:1px solid #7c3aed66;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">'
            f'<span style="color:#7c3aed;font-family:monospace;font-size:.65rem;font-weight:700;">🛰️ SENTINEL LIVE — TOP 100</span>'
            f'<span style="font-family:monospace;font-size:.6rem;color:#9ca3af;">{chk_f} checked / {total_uni} universe ({pct_done}% cycle)</span>'
            f'<span style="font-family:monospace;font-size:.62rem;color:#ff4444;font-weight:700;">{sig_f} signals</span>'
            f'<span style="font-family:monospace;font-size:.58rem;color:#9ca3af;">Last: {last_f}</span>'
            f'</div>'
            f'<div style="height:3px;background:#1f2937;border-radius:2px;margin-top:6px;">'
            f'<div style="width:{pct_done}%;height:100%;background:#7c3aed;border-radius:2px;transition:width .3s;"></div>'
            f'</div>'
            '</div>',
            unsafe_allow_html=True)

    except Exception as e:
        import traceback
        st.session_state.sentinel_active = False
        st.error(f"Sentinel error (auto-disabled): {e}")
        st.code(traceback.format_exc(), language="text")


# ─── RESULTS DISPLAY ─────────────────────────────────────────────────────────
results = st.session_state.results
errs    = getattr(st.session_state, 'scan_errors', [])
btc_t   = getattr(st.session_state, 'btc_trend', "NEUTRAL")

if not results:
    st.markdown('<div class="empty-st"><div style="font-size:2.5rem;opacity:.2;margin-bottom:12px;">🔥</div>Run a scan — or lower Min Score in sidebar</div>', unsafe_allow_html=True)
    if st.session_state.scan_count > 0:
        with st.expander("🔍 Debug Info", expanded=True):
            raw = st.session_state.last_raw_count
            st.markdown(f"""
**Scan #{st.session_state.scan_count}** at **{st.session_state.last_scan}**

| Check | Status |
|---|---|
| Raw coins found (before filter) | **{raw}** |
| Min score filter | **{eff_s['min_score']}** — set to 1 to see everything |
| Min R:R filter | **{eff_s.get('min_rr', 1.5)}** — lowering shows more results |
| Momentum required | **{'YES — turn off in sidebar' if eff_s.get('require_momentum') else 'NO'}** |
| BTC filter | **{btc_t}** — {"⚠️ BEARISH blocking LONGs! Turn off." if btc_t=='BEARISH' else "✅ OK"} |
| Cooldown | **{'ON' if eff_s['cooldown_on'] else 'OFF'}** |
| Scan modes | **{", ".join(eff_s.get('scan_modes', ['volume']))}** |
| Errors | **{len(errs)}** out of {eff_s['scan_depth']} coins |

**Fix steps:**
1. Set **Min Score = 1** in sidebar
2. Turn off **Require Momentum** in sidebar
3. Set **Min R:R = 1.0** in Settings
4. Turn **BTC Bear filter OFF**
5. If still 0 raw — exchange throttling. Wait 60s.
            """)
            if errs: st.code("\n".join(errs[:15]), language="text")
else:
    sq = [r for r in results if r['cls']=="squeeze"]
    br = [r for r in results if r['cls']=="breakout"]
    wh = [r for r in results if r['cls']=="whale_driven"]
    ea = [r for r in results if r['cls']=="early"]
    top = results[0]

    mom_count   = sum(1 for r in results if r.get('momentum_confirmed'))
    sent_count  = sum(1 for r in results if r.get('sentiment',{}).get('available'))
    penalty_count = sum(1 for r in results if r.get('penalty_flags'))

    st.markdown(f"""<div class="stat-strip">
      <div><div class="ss-val" style="color:var(--red);">{len(sq)}</div><div class="ss-lbl">Squeeze</div></div>
      <div><div class="ss-val" style="color:var(--amber);">{len(br)}</div><div class="ss-lbl">Breakout</div></div>
      <div><div class="ss-val" style="color:var(--purple);">{len(wh)}</div><div class="ss-lbl">Whale Driven</div></div>
      <div><div class="ss-val" style="color:var(--blue);">{len(ea)}</div><div class="ss-lbl">Early</div></div>
      <div><div class="ss-val">{len(results)}</div><div class="ss-lbl">Total</div></div>
      <div><div class="ss-val">{st.session_state.last_raw_count}</div><div class="ss-lbl">Raw Scanned</div></div>
      <div><div class="ss-val" style="color:var(--green);">{mom_count}</div><div class="ss-lbl">Momentum Live</div></div>
      <div><div class="ss-val" style="color:var(--red);">{penalty_count}</div><div class="ss-lbl">Penalised</div></div>
      <div><div class="ss-val" style="color:{pump_color(top['pump_score'])};">{top['symbol']}</div><div class="ss-lbl">Hottest ({top['pump_score']})</div></div>
    </div>""", unsafe_allow_html=True)

    # ── SENTINEL RESULTS ─────────────────────────────────────────────────
    sent_res = st.session_state.get('sentinel_results', [])
    if sent_res:
        st.markdown(
            f'<div style="background:linear-gradient(135deg,#1a0a2e,#0f1117);'
            f'border:1px solid #7c3aed66;border-radius:10px;padding:10px 16px;margin-bottom:12px;">'
            f'<span style="font-family:monospace;font-size:.65rem;font-weight:700;color:#a78bfa;">'
            f'🛰️ SENTINEL LIVE (Top 100) — {len(sent_res)} signal{"s" if len(sent_res)>1 else ""} found</span>'
            '</div>',
            unsafe_allow_html=True)
        for _sr in sent_res[:10]:
            render_card(_sr, _sr.get('pump_score', 0) >= 90)
        if st.button("🗑️ Clear Sentinel Results", key="clr_sent"):
            st.session_state.sentinel_results = []
            st.rerun()
        st.markdown("---")

    tab_s, tab_b, tab_w, tab_e = st.tabs([
        f"🔴  SQUEEZE  ({len(sq)})",
        f"🟡  BREAKOUT  ({len(br)})",
        f"🐋  WHALE DRIVEN  ({len(wh)})",
        f"🔵  EARLY  ({len(ea)})"
    ])
    with tab_s:
        st.markdown('<div class="tab-desc"><b>About to Squeeze</b> — Funding extreme + OB heavily imbalanced. Trapped shorts/longs about to be liquidated. Highest urgency. ⚠️ Cards with penalty flags = late entry risk, trade with caution.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in sq] if sq else st.markdown('<div class="empty-st"><div>⚡</div>No squeeze setups this scan</div>', unsafe_allow_html=True)
    with tab_b:
        st.markdown('<div class="tab-desc"><b>Confirmed Breakout</b> — OI spiking with volume. New money entering. Enter on first pullback into entry zone.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in br] if br else st.markdown('<div class="empty-st"><div>📈</div>No confirmed breakouts</div>', unsafe_allow_html=True)
    with tab_w:
        st.markdown('<div class="tab-desc"><b>Whale Driven</b> — Large wall + volume surge. A single large actor is driving price. Follow the whale, not the crowd.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in wh] if wh else st.markdown('<div class="empty-st"><div>🐋</div>No whale-driven setups</div>', unsafe_allow_html=True)
    with tab_e:
        st.markdown('<div class="tab-desc"><b>Early Signal</b> — Pre-pump alignment. Watchlist these — often move into Squeeze/Breakout within 1–4h.</div>', unsafe_allow_html=True)
        [render_card(r, r.get('is_sniper', False)) for r in ea] if ea else st.markdown('<div class="empty-st"><div>📡</div>No early signals</div>', unsafe_allow_html=True)

if eff_s.get('auto_scan'):
    st.markdown(f'<div style="text-align:center;font-family:monospace;font-size:.62rem;color:#9ca3af;padding:16px 0;">🤖 Auto-Pilot Active — Next scan in {eff_s["auto_interval"]} minute(s)</div>', unsafe_allow_html=True)
    time.sleep(eff_s['auto_interval']*60)
    st.rerun()
