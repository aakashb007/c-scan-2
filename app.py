import streamlit as st
import asyncio
import pandas as pd
import numpy as np
import requests, time, csv, os, json
from datetime import datetime, timezone, timedelta

@st.cache_resource(show_spinner=False)
def _load_heavy():
    import ccxt.async_support as ccxt_mod
    import pandas_ta as ta_mod
    return ccxt_mod, ta_mod

ccxt, ta = _load_heavy()

try:
    import nest_asyncio
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    nest_asyncio.apply(loop)
except: pass

st.set_page_config(page_title="APEX5 // Pump & Dump Scanner", page_icon="🔥", layout="wide", initial_sidebar_state="expanded")

_SS_DEFAULTS = {
    'results':[], 'last_scan':"—", 'scan_count':0, 'btc_price':0, 'btc_trend':"—",
    'fng_val':50, 'fng_txt':"Neutral", 'scan_errors':[], 'last_raw_count':0,
    'logged_sigs':set(), 'sentinel_active':False, 'sentinel_results':[],
    'sentinel_last_check':'—', 'sentinel_total_checked':0, 'sentinel_signals_found':0,
    'sentinel_universe_size':'?', 'social_cache':{}, 'social_last_fetch':0,
    'listing_cache':{}, 'listing_last_fetch':0, 'onchain_cache':{},
    'journal_last_autocheck':0, 'prev_results':{}, 'alerted_scores':{},
    'scan_modes':['mixed'], 'fng_last_fetch':0, 'alerted_sigs':set(),
    'last_daily_summary':0, 'nav_state':'🔥 Scanner',
    'dst_results':[],'dst_last_scan':'-','dst_scan_count':0,
    'gl_results':[],'gl_last_scan':'-','gl_last_ts':0,'gl_history':[],
    'ms_last_ts':0,'ms_data':None,'ms_last_update':'–',
}
for k,v in _SS_DEFAULTS.items():
    if k not in st.session_state: st.session_state[k] = v

JOURNAL_FILE="trade_journal.csv"; COOLDOWN_FILE="symbol_cooldowns.json"; SETTINGS_FILE="APEX5_settings.json"; GL_PERF_FILE="gl_performance.csv"

DEFAULT_SETTINGS = {
    "scan_depth":40,"scan_modes":["mixed"],"fast_tf":"15m","slow_tf":"4h","min_score":10,
    "j_imminent":True,"j_building":True,"j_early":False,
    "j_filter_classes":["squeeze","breakout","whale_driven","early","god_tier"],
    "j_min_score":25,"j_require_technicals":[],
    "req_4h_fvg":False,"req_4h_ob":False,"req_5m_align":False,
    "req_ob":False,"req_whale":False,"req_funding":False,"req_oi":False,
    "req_volume":False,"req_liq":False,"req_technicals":False,"req_session":False,
    "req_momentum":False,"req_mtf":False,"req_sentiment":False,"req_social":False,
    "req_orderflow":False,"req_liq_map":False,"req_onchain":False,
    "req_wyckoff":False,"req_cvd":False,"req_stophunt":False,
    "req_rel_strength":False,"req_atr_expansion":False,
    "kill_switch_on":False,
    "kill_zones":[{"start":16,"end":19,"label":"Late NY / Asia Transition"}],
    "rs_btc_filter":False,"rs_btc_min":0.0,
    "atr_expansion_filter":False,"atr_expansion_mult":1.5,
    "whale_min_usdt":250000,"btc_filter":True,"cooldown_on":True,"cooldown_hrs":4,
    "auto_scan":False,"auto_interval":5,"alert_min_score":60,
    "alert_longs":True,"alert_shorts":True,"alert_squeeze":True,
    "alert_breakout":True,"alert_early":False,"alert_whale":True,
    "cls_breakout_oi_min":7,"cls_breakout_vol_min":4,"cls_breakout_score_min":25,
    "cls_squeeze_fund_min":8,"cls_squeeze_ob_min":6,
    "ob_ratio_low":1.1,"ob_ratio_mid":1.5,"ob_ratio_high":2.5,
    "funding_low":0.0002,"funding_mid":0.0005,"funding_high":0.001,
    "oi_price_chg_low":0.5,"oi_price_chg_high":2.0,
    "vol_surge_low":1.5,"vol_surge_mid":2.0,"vol_surge_high":3.0,
    "liq_cluster_near":1.5,"liq_cluster_mid":3.0,"liq_cluster_far":5.0,
    "vol24h_low":1000000,"vol24h_mid":10000000,"vol24h_high":50000000,
    "rsi_oversold":40,"rsi_overbought":60,"min_reasons":1,"min_rr":1.5,
    "require_momentum":False,
    "pts_ob_low":5,"pts_ob_mid":15,"pts_ob_high":25,
    "pts_funding_low":5,"pts_funding_mid":15,"pts_funding_high":25,
    "pts_oi_low":7,"pts_oi_high":18,"pts_vol_low":3,"pts_vol_mid":8,"pts_vol_high":14,
    "pts_liq_near":15,"pts_liq_mid":8,"pts_liq_far":4,
    "pts_vol24_low":3,"pts_vol24_mid":6,"pts_vol24_high":12,
    "pts_macd":4,"pts_rsi":5,"pts_bb":4,"pts_ema":3,"pts_session":4,
    "pts_sentiment":20,"pts_taker":10,
    "pts_whale_near":25,"pts_whale_mid":15,"pts_whale_far":8,
    "cmc_key":"","tg_token":"","tg_chat_id":"",
    "discord_webhook":"https://discord.com/api/webhooks/1476606856599179265/74wKbIJEXNJ9h10Ab0Q9Vp7ZmeJ52XY18CP3lKxg3eR1BbpZSdX65IT8hbZjpEIXSqEg",
    "okx_key":"","okx_secret":"","okx_passphrase":"","gate_key":"","gate_secret":"",
    "min_vol_filter":300000,"min_active_signals":3,"spread_max_pct":0.5,
    "atr_min_pct":0.2,"atr_max_pct":10.0,"mtf_confirm":True,
    "pts_mtf":12,"pts_divergence":10,"pts_candle_pattern":8,"pts_oi_funding_combo":10,
    "dedup_symbols":True,"fng_long_threshold":30,"fng_short_threshold":70,
    "vol_surge_explosive":5.0,"pts_vol_explosive":20,"pts_orderflow":12,
    "orderflow_lookback":10,"pts_liq_map":15,"listing_alert_pts":25,
    "onchain_whale_min":500000,"pts_onchain_whale":15,
    "pts_wyckoff":30,"pts_cvd":20,"pts_stophunt":25,
    "dst_enabled":True,"dst_dema_len":200,"dst_st_period":10,"dst_st_mult":3.0,
    "dst_vol_mult":1.2,"dst_rsi_ob":65,"dst_rsi_os":35,"dst_mom_bars":3,
    "dst_use_vol":True,"dst_use_rsi":True,"dst_use_mom":True,"dst_use_dema_dist":True,
    "dst_min_dema_dist":0.1,"dst_fresh_bars":5,"dst_sl_atr":1.5,"dst_tp_atr":3.0,
    "dst_confirm_boost":8,"dst_conflict_penalty":5,
    "dst_auto_scan":False,"dst_interval":5,"dst_timeframe":"5m","dst_min_rr":1.5,"dst_min_score":3,
    "gl_enabled":True,"gl_interval":5,"gl_min_gain_pct":3.0,"gl_min_loss_pct":3.0,
    "gl_pullback_min":1.5,"gl_pullback_max":8.0,"gl_rsi_ob":75,"gl_rsi_os":30,
    "gl_vol_expansion":1.5,"gl_min_rr":1.5,"gl_top_n":20,
    "gl_alert_pullback":True,"gl_alert_breakout":True,
    "gl_alert_bounce":True,"gl_alert_breakdown":True,
    "gl_alert_pregainer":True,"gl_alert_preloser":True,
    "symbol_blacklist":"","ms_refresh_interval":5,"groq_key":"",
    "dst_watchlist":[],"dst_use_watchlist":False,
    "dst_use_trail":False,"dst_trail_atr":2.5,"dst_partial":True,
    "dst_partial_pct":50,"dst_partial_rr":2.0,"dst_breakeven":True,"dst_be_rr":1.5,
    "journal_autocheck_on":True,"journal_autocheck_mins":15,
    "late_entry_chg_thresh":8.0,"late_entry_penalty":20,"vol_exhaust_penalty":15,
    "near_top_penalty":15,"use_4h_ob_for_sl":True,
    "sentinel_score_threshold":70,"sentinel_batch_size":5,"sentinel_check_interval":30,
    "daily_summary_hour":8,"daily_summary_on":True,
    "social_enabled":True,"social_reddit_weight":8,"social_min_mentions":3,
    "social_buzz_threshold":10,"apify_token":"",
    "backtest_min_score":50,"backtest_days":30,
}

def load_settings():
    s=DEFAULT_SETTINGS.copy()
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE,'r',encoding='utf-8') as f: s.update(json.load(f))
        except: pass
    return s

def save_settings(s):
    with open(SETTINGS_FILE,'w',encoding='utf-8') as f: json.dump(s,f,indent=2)

S = load_settings()

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def _pkt_time(utc_dt=None):
    """Convert UTC datetime to Pakistan Time (UTC+5) string."""
    if utc_dt is None:
        utc_dt = datetime.now(timezone.utc)
    pkt = utc_dt.replace(tzinfo=timezone.utc) + __import__('datetime').timedelta(hours=5)
    return pkt.strftime('%Y-%m-%d %H:%M:%S PKT')

def _dual_time():
    """Returns tuple of (utc_str, pkt_str) for current time."""
    now = datetime.now(timezone.utc)
    utc_str = now.strftime('%Y-%m-%d %H:%M:%S UTC')
    pkt = now + __import__('datetime').timedelta(hours=5)
    pkt_str = pkt.strftime('%Y-%m-%d %H:%M:%S PKT')
    return utc_str, pkt_str

def ensure_gl_performance():
    """Create GL performance CSV if not exists."""
    headers = ['signal_id','timestamp_utc','timestamp_pkt','symbol','type','direction',
               'entry','tp','sl','rr','chg_4h','chg_1h','rsi','vol_ratio',
               'outcome','outcome_time','pnl_pct','bars_to_outcome']
    if not os.path.exists(GL_PERF_FILE):
        with open(GL_PERF_FILE,'w',newline='',encoding='utf-8') as f:
            csv.writer(f).writerow(headers)

def log_gl_signal(sig):
    """Log a new G/L signal to performance tracker with PENDING outcome."""
    try:
        ensure_gl_performance()
        signal_id = f"{sig['symbol']}_{sig['type']}_{sig.get('scan_time','')}"
        # Check if already logged
        if os.path.exists(GL_PERF_FILE):
            df = pd.read_csv(GL_PERF_FILE)
            if signal_id in df['signal_id'].values: return
        with open(GL_PERF_FILE,'a',newline='',encoding='utf-8') as f:
            csv.writer(f).writerow([
                signal_id,
                sig.get('scan_time',''),
                sig.get('scan_time_pkt',''),
                sig.get('symbol',''),
                sig.get('type',''),
                sig.get('direction',''),
                sig.get('close',''),
                sig.get('tp',''),
                sig.get('sl',''),
                sig.get('rr',''),
                sig.get('chg_4h',''),
                sig.get('chg_1h',''),
                sig.get('rsi',''),
                sig.get('vol_ratio',''),
                'PENDING','','',''
            ])
    except Exception as e:
        print(f"[GL PERF] Log error: {e}")

def check_gl_outcomes():
    """Check all PENDING signals and update outcomes."""
    try:
        if not os.path.exists(GL_PERF_FILE): return
        df = pd.read_csv(GL_PERF_FILE)
        pending = df[df['outcome'] == 'PENDING']
        if pending.empty: return

        import ccxt as _ccxt
        ex = _ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})

        for idx, row in pending.iterrows():
            try:
                sym = row['symbol']
                entry = float(row['entry'])
                tp = float(row['tp']) if row['tp'] and str(row['tp']) != 'nan' else None
                sl = float(row['sl']) if row['sl'] and str(row['sl']) != 'nan' else None
                if not tp or not sl: continue

                # Check signal age — expire after 24H
                try:
                    sig_time = pd.to_datetime(row['timestamp_utc'])
                    age_hours = (pd.Timestamp.utcnow() - sig_time.replace(tzinfo=None)).total_seconds() / 3600
                    if age_hours > 24:
                        df.at[idx, 'outcome'] = 'EXPIRED'
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                        continue
                except: pass

                # Fetch current price
                ticker = ex.fetch_ticker(sym)
                current = float(ticker['last'])

                direction = str(row['direction'])
                if direction == 'WATCH':
                    # Pre-signals — check if coin moved 5%+ from entry
                    move_pct = (current - entry) / entry * 100
                    sig_type = str(row['type'])
                    if sig_type == 'PRE_GAINER' and move_pct >= 5.0:
                        df.at[idx, 'outcome'] = 'WIN'
                        df.at[idx, 'pnl_pct'] = round(move_pct, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                    elif sig_type == 'PRE_LOSER' and move_pct <= -5.0:
                        df.at[idx, 'outcome'] = 'WIN'
                        df.at[idx, 'pnl_pct'] = round(abs(move_pct), 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                    elif sig_type == 'PRE_GAINER' and move_pct <= -5.0:
                        df.at[idx, 'outcome'] = 'LOSS'
                        df.at[idx, 'pnl_pct'] = round(move_pct, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                    elif sig_type == 'PRE_LOSER' and move_pct >= 5.0:
                        df.at[idx, 'outcome'] = 'LOSS'
                        df.at[idx, 'pnl_pct'] = round(-move_pct, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                elif direction == 'LONG':
                    if current >= tp:
                        pnl = (tp - entry) / entry * 100
                        df.at[idx, 'outcome'] = 'WIN'
                        df.at[idx, 'pnl_pct'] = round(pnl, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                    elif current <= sl:
                        pnl = (sl - entry) / entry * 100
                        df.at[idx, 'outcome'] = 'LOSS'
                        df.at[idx, 'pnl_pct'] = round(pnl, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                elif direction == 'SHORT':
                    if current <= tp:
                        pnl = (entry - tp) / entry * 100
                        df.at[idx, 'outcome'] = 'WIN'
                        df.at[idx, 'pnl_pct'] = round(pnl, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
                    elif current >= sl:
                        pnl = (entry - sl) / entry * 100
                        df.at[idx, 'outcome'] = 'LOSS'
                        df.at[idx, 'pnl_pct'] = round(pnl, 2)
                        df.at[idx, 'outcome_time'] = _dual_time()[0]
            except: continue

        df.to_csv(GL_PERF_FILE, index=False)
    except Exception as e:
        print(f"[GL PERF] Outcome check error: {e}")

def get_gl_stats():
    """Get win/loss stats by signal type."""
    try:
        if not os.path.exists(GL_PERF_FILE): return {}
        df = pd.read_csv(GL_PERF_FILE)
        stats = {}
        for sig_type in ['GAINER_PULLBACK','GAINER_BREAKOUT','LOSER_BOUNCE','LOSER_BREAKDOWN','PRE_GAINER','PRE_LOSER']:
            subset = df[df['type'] == sig_type]
            wins   = len(subset[subset['outcome'] == 'WIN'])
            losses = len(subset[subset['outcome'] == 'LOSS'])
            pending= len(subset[subset['outcome'] == 'PENDING'])
            expired= len(subset[subset['outcome'] == 'EXPIRED'])
            total  = wins + losses
            wr     = round(wins/total*100) if total > 0 else 0
            avg_pnl= round(subset[subset['outcome'].isin(['WIN','LOSS'])]['pnl_pct'].astype(float).mean(), 2) if total > 0 else 0
            stats[sig_type] = {
                'wins': wins, 'losses': losses,
                'pending': pending, 'expired': expired,
                'total': total, 'wr': wr, 'avg_pnl': avg_pnl
            }
        return stats
    except: return {}

def ensure_journal():
    headers=["ts","symbol","exchange","type","pump_score","class","price","tp","sl","triggers","status","entry_touched","tp1","tp2","tp3"]
    if not os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE,'w',newline='',encoding='utf-8') as f: csv.writer(f).writerow(headers)
    else:
        try:
            df=pd.read_csv(JOURNAL_FILE); updated=False
            for col,default in [('status','ACTIVE'),('exchange','MEXC'),('entry_touched','0'),
                                  ('tp1','0'),('tp2','0'),('tp3','0')]:
                if col not in df.columns: df[col]=default; updated=True
            if updated: df.to_csv(JOURNAL_FILE,index=False)
        except: pass

def log_trade(res, force=False):
    """Log trade — respects journal filter settings unless force=True"""
    s = load_settings()
    if not force:
        cls = res.get('cls','early')
        score = res.get('pump_score',0)
        j_classes = s.get('j_filter_classes', list(DEFAULT_SETTINGS['j_filter_classes']))
        j_min = s.get('j_min_score', 25)
        j_req_tech = s.get('j_require_technicals', [])
        # Class filter
        if 'god_tier' in j_classes and score >= 90: pass
        elif cls not in j_classes: return
        # Score filter
        if score < j_min: return
        # Technical checklist
        bd = res.get('signal_breakdown', {})
        tech_map = {
            'ob': 'ob_imbalance', 'funding': 'funding', 'oi': 'oi_spike',
            'volume': 'vol_surge', 'whale': 'whale_wall', 'sentiment': 'sentiment',
            'mtf': 'mtf', 'orderflow': 'orderflow'
        }
        for req in j_req_tech:
            key = tech_map.get(req)
            if key and bd.get(key, 0) <= 0: return

       # ── Required Technicals (professional filters) ────────────────────
        if s.get('req_4h_fvg', False):
            if not any('FVG' in str(r) for r in res.get('reasons', [])): return
        if s.get('req_4h_ob', False):
            if bd.get('ob_imbalance', 0) <= 0: return
        if s.get('req_5m_align', False):
            if bd.get('mtf', 0) <= 0: return
        if s.get('req_ob', False):
            if bd.get('ob_imbalance', 0) <= 0: return
        if s.get('req_whale', False):
            if bd.get('whale_wall', 0) <= 0: return
        if s.get('req_funding', False):
            if bd.get('funding', 0) <= 0: return
        if s.get('req_oi', False):
            if bd.get('oi_spike', 0) <= 0: return
        if s.get('req_volume', False):
            if bd.get('vol_surge', 0) <= 0: return
        if s.get('req_liq', False):
            if bd.get('liq_cluster', 0) <= 0: return
        if s.get('req_technicals', False):
            if bd.get('technicals', 0) <= 0: return
        if s.get('req_session', False):
            if bd.get('session', 0) <= 0: return
        if s.get('req_momentum', False):
            if bd.get('momentum', 0) <= 0: return
        if s.get('req_mtf', False):
            if bd.get('mtf', 0) <= 0: return
        if s.get('req_sentiment', False):
            if bd.get('sentiment', 0) <= 0: return
        if s.get('req_social', False):
            if bd.get('social_buzz', 0) <= 0: return
        if s.get('req_orderflow', False):
            if bd.get('orderflow', 0) <= 0: return
        if s.get('req_liq_map', False):
            if bd.get('liq_map', 0) <= 0: return
        if s.get('req_onchain', False):
            if bd.get('onchain', 0) <= 0: return
        if s.get('req_wyckoff', False):
            if bd.get('wyckoff_spring', 0) <= 0: return
        if s.get('req_cvd', False):
            if bd.get('cvd_divergence', 0) <= 0: return
        if s.get('req_stophunt', False):
            if bd.get('stop_hunt', 0) <= 0: return
    try:
        ensure_journal()
        with open(JOURNAL_FILE,'a',newline='',encoding='utf-8') as f:
            entry_lo = res.get('entry_lo', res['price'])
            entry_hi = res.get('entry_hi', res['price'])
            already_in_zone = entry_lo <= res['price'] <= entry_hi
            entry_touched_val = "1" if already_in_zone else "0"
            csv.writer(f).writerow([
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                res['symbol'], res.get('exchange','MEXC'), res['type'],
                res['pump_score'], res.get('cls','—'), round(res['price'],8),
                round(res.get('tp',0),8), round(res.get('sl',0),8),
                " | ".join(res['reasons']), "ACTIVE", entry_touched_val,
                round(res.get('tp1',res.get('tp',0)),8),
                round(res.get('tp2',res.get('tp',0)),8),
                round(res.get('tp3',res.get('tp',0)),8),
            ])
    except: pass

def _journal_check_hits(df, prices, s):
    updated=False; hits=[]
    for i, row in df.iterrows():
        if row.get('status') != 'ACTIVE': continue
        sym=str(row.get('symbol','')); price=prices.get(sym,0)
        if not price: continue
        try: tp=float(row['tp']); sl=float(row['sl']); entry_px=float(row['price'])
        except: continue
        # FIX: Only start tracking after entry zone is touched
        entry_touched = str(row.get('entry_touched','0')) == '1'
        if not entry_touched:
            # Check if price is within 0.5% of entry price
            if abs(price - entry_px) / entry_px * 100 <= 0.5:
                df.at[i,'entry_touched'] = '1'; updated=True; entry_touched=True
            else: continue
        sig=row.get('type','LONG'); hit=None
        if sig=='LONG':
            if price>=tp: hit='TP'
            elif price<=sl: hit='SL'
        else:
            if price<=tp: hit='TP'
            elif price>=sl: hit='SL'
        if hit:
            df.at[i,'status']=hit; updated=True
            hits.append({'symbol':sym,'hit':hit,'price':price,'tp':tp,'sl':sl,'type':sig})
    return df,hits,updated

def _fire_journal_alerts(hits, s, source="Scan"):
    for h in hits:
        em="✅" if h['hit']=='TP' else "🛑"
        st.toast(f"{em} [{source}] {h['symbol']} {h['hit']} @ ${h['price']:.6f}", icon=em)

def process_journal_tracking(tickers_dict, s):
    if not os.path.exists(JOURNAL_FILE): return
    try:
        df=pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns: return
        prices={}
        for t,data in tickers_dict.items():
            base=t.split('/')[0].split(':')[0]; px=float(data.get('last') or 0)
            if px: prices[base]=px
        df,hits,updated=_journal_check_hits(df,prices,s)
        if updated: df.to_csv(JOURNAL_FILE,index=False)
        if hits: _fire_journal_alerts(hits,s,"Scan")
    except: pass

def autocheck_journal_background(s):
    if not s.get('journal_autocheck_on',True): return
    if not os.path.exists(JOURNAL_FILE): return
    interval=s.get('journal_autocheck_mins',15)*60
    if time.time()-st.session_state.get('journal_last_autocheck',0)<interval: return
    st.session_state.journal_last_autocheck=time.time()
    try:
        df=pd.read_csv(JOURNAL_FILE)
        if df.empty or 'status' not in df.columns: return
        active_syms=df[df['status']=='ACTIVE']['symbol'].dropna().unique().tolist()
        if not active_syms: return
        prices={}
        for sym in active_syms[:5]:
            try:
                r=requests.get("https://www.okx.com/api/v5/market/ticker",
                    params={"instId":f"{sym}-USDT-SWAP"},timeout=1)
                if r.status_code==200:
                    d=r.json().get('data',[])
                    if d: prices[sym]=float(d[0].get('last',0) or 0)
            except: pass
        df,hits,updated=_journal_check_hits(df,prices,s)
        if updated: df.to_csv(JOURNAL_FILE,index=False)
        if hits: _fire_journal_alerts(hits,s,"Auto-Check")
    except: pass

def check_daily_summary(s):
    """Fire daily journal summary to Discord/Telegram once per 24h"""
    if not s.get('daily_summary_on', True): return
    last=st.session_state.get('last_daily_summary',0)
    if time.time()-last < 82800: return  # ~23h guard
    now=datetime.now(timezone.utc)
    target_h=s.get('daily_summary_hour',8)
    if now.hour != target_h: return
    st.session_state.last_daily_summary=time.time()
    try:
        if not os.path.exists(JOURNAL_FILE): return
        df=pd.read_csv(JOURNAL_FILE)
        if df.empty: return
        df['ts']=pd.to_datetime(df['ts'],errors='coerce')
        df24=df[df['ts']>=(datetime.now()-timedelta(hours=24))]
        total=len(df24); longs=len(df24[df24['type']=='LONG']); shorts=len(df24[df24['type']=='SHORT'])
        tps=len(df24[df24['status']=='TP']); sls=len(df24[df24['status']=='SL'])
        active=len(df24[df24['status']=='ACTIVE'])
        wr=(tps/(tps+sls)*100) if (tps+sls)>0 else 0
        msg=(f"📊 **APEX5 24h Journal Summary**\n"
             f"🕐 {now.strftime('%Y-%m-%d %H:%M UTC')} | {(now + __import__('datetime').timedelta(hours=5)).strftime('%Y-%m-%d %H:%M PKT')}\n"
             f"━━━━━━━━━━━━━━━━━\n"
             f"Total Signals: **{total}** | 📗 Long: **{longs}** | 📕 Short: **{shorts}**\n"
             f"✅ TP Hits: **{tps}** | 🛑 SL Hits: **{sls}** | 🔄 Active: **{active}**\n"
             f"🎯 Win Rate: **{wr:.1f}%**\n"
             f"━━━━━━━━━━━━━━━━━\n"
             f"*Powered by APEX5 Intelligence Terminal*")
        tg_msg=msg.replace("**","<b>").replace("**","</b>")
        if s.get('tg_token') and s.get('tg_chat_id'):
            send_tg(s['tg_token'],s['tg_chat_id'],tg_msg.replace("**","").replace("*",""))
        if s.get('discord_webhook'):
            send_discord(s['discord_webhook'],{
                "title":"📊 APEX5 24h Journal Summary",
                "color":0x2563eb,
                "description":msg,
                "footer":{"text":f"APEX5 Terminal • {now.strftime('%H:%M UTC')} | {(now + __import__('datetime').timedelta(hours=5)).strftime('%H:%M PKT')}"}
            })
    except: pass

def is_on_cooldown(sym,hrs):
    if not os.path.exists(COOLDOWN_FILE): return False
    try:
        with open(COOLDOWN_FILE,'r',encoding='utf-8') as f: cd=json.load(f)
        if sym in cd: return (datetime.now()-datetime.fromisoformat(cd[sym])).total_seconds()/3600<hrs
    except: pass
    return False

def set_cooldown(sym):
    cd={}
    if os.path.exists(COOLDOWN_FILE):
        try:
            with open(COOLDOWN_FILE,'r',encoding='utf-8') as f: cd=json.load(f)
        except: pass
    cd[sym]=datetime.now().isoformat()
    with open(COOLDOWN_FILE,'w',encoding='utf-8') as f: json.dump(cd,f)

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
    bd=res.get('signal_breakdown',{}); sc=res['pump_score']; cfg=res.get('_cls_cfg',{})
    br_oi=cfg.get('breakout_oi_min',7); br_vol=cfg.get('breakout_vol_min',4)
    br_sc=cfg.get('breakout_sc_min',25); sq_fd=cfg.get('squeeze_fund_min',8); sq_ob=cfg.get('squeeze_ob_min',6)
    squeeze_score=bd.get('funding',0)+bd.get('funding_hist',0)+bd.get('ob_imbalance',0)
    breakout_score=bd.get('oi_spike',0)+bd.get('vol_surge',0)+bd.get('orderflow',0)
    whale_score=bd.get('whale_wall',0)+bd.get('liq_cluster',0)
    if bd.get('funding',0)>=sq_fd and bd.get('ob_imbalance',0)>=sq_ob and sc>=25: return 'squeeze'
    if bd.get('oi_spike',0)>=br_oi and bd.get('vol_surge',0)>=br_vol and sc>=br_sc: return 'breakout'
    if bd.get('vol_surge',0)>=8 and bd.get('orderflow',0)>=6 and sc>=30: return 'breakout'
    if bd.get('whale_wall',0)>=8 or (bd.get('liq_cluster',0)>=8 and bd.get('vol_surge',0)>=3): return 'whale_driven'
    if sc>=90:
        return max({'squeeze':squeeze_score,'breakout':breakout_score,'whale_driven':whale_score},
                   key=lambda k:{'squeeze':squeeze_score,'breakout':breakout_score,'whale_driven':whale_score}[k])
    if sc>=70:
        if squeeze_score>=breakout_score and squeeze_score>=whale_score: return 'squeeze'
        if breakout_score>=squeeze_score: return 'breakout'
    return 'early'

def send_tg(token,cid,msg):
    if not token or not cid: return
    try:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id":cid,"text":msg,"parse_mode":"HTML"},timeout=8)
        if r.status_code!=200: st.toast(f"⚠️ Telegram failed: {r.status_code}",icon="⚠️")
    except Exception as e: st.toast(f"⚠️ Telegram error: {e}",icon="⚠️")

def send_discord(webhook_url,embed_dict):
    if not webhook_url: return
    try:
        r=requests.post(webhook_url,json={"embeds":[embed_dict]},timeout=8)
        if r.status_code not in (200,204): st.toast(f"⚠️ Discord failed: {r.status_code}",icon="⚠️")
    except Exception as e: st.toast(f"⚠️ Discord error: {e}",icon="⚠️")

# ─── CSS (condensed) ──────────────────────────────────────────────────────────
st.markdown("""<style>
:root{--bg:#f7f8fc;--surface:#ffffff;--panel:#f0f2f8;--border:#e2e5f0;--border2:#c8cde0;
--text:#0f1117;--text2:#3d4461;--muted:#7a82a0;--green:#059669;--green-bg:#ecfdf5;
--green-bd:#a7f3d0;--red:#dc2626;--red-bg:#fef2f2;--red-bd:#fecaca;--amber:#d97706;
--amber-bg:#fffbeb;--amber-bd:#fde68a;--blue:#2563eb;--blue-bg:#eff6ff;--blue-bd:#bfdbfe;
--purple:#7c3aed;--purple-bg:#f5f3ff;--purple-bd:#ddd6fe;
--sh:0 1px 4px rgba(15,17,23,.06),0 4px 16px rgba(15,17,23,.04);
--sh-lg:0 8px 32px rgba(15,17,23,.10),0 2px 8px rgba(15,17,23,.06);}
*,*::before,*::after{box-sizing:border-box;}
html,body,.stApp{background:var(--bg)!important;font-family:sans-serif!important;color:var(--text)!important;}
#MainMenu,footer,.stDeployButton{display:none!important;}
header{background-color:transparent!important;}
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
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px;}
.ticker-bar{background:#0f1117;color:#fff;border-radius:8px;padding:10px 20px;display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:18px;font-family:monospace;font-size:.68rem;}
.t-lbl{color:rgba(255,255,255,.38);font-size:.54rem;letter-spacing:.1em;text-transform:uppercase;}
.t-val{color:#fff;font-weight:600;}
.pump-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;margin-bottom:10px;transition:box-shadow .18s,border-color .18s;position:relative;overflow:hidden;}
.pump-card:hover{box-shadow:var(--sh-lg);border-color:var(--border2);}
.pump-card::before{content:'';position:absolute;top:0;left:0;width:4px;height:100%;border-radius:10px 0 0 10px;}
.pc-long::before{background:var(--green);}.pc-short::before{background:var(--red);}
.score-ring{width:54px;height:54px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:monospace;font-size:.95rem;font-weight:700;border:3px solid;flex-shrink:0;}
.px-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0;}
.px-cell{background:var(--panel);border-radius:6px;padding:8px 10px;text-align:center;}
.px-lbl{font-family:monospace;font-size:.52rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:3px;}
.px-val{font-family:monospace;font-size:.8rem;font-weight:600;color:var(--text);}
.sig-pips{display:flex;flex-wrap:wrap;gap:10px;margin:8px 0;}
.pip-item{display:flex;align-items:center;gap:5px;font-family:monospace;font-size:.6rem;color:var(--muted);}
.pip{width:7px;height:7px;border-radius:2px;}.pip-on{background:var(--text);}.pip-half{background:var(--border2);}.pip-off{background:var(--panel);border:1px solid var(--border);}
.reasons-list .r{font-size:.74rem;color:var(--text2);padding:4px 0;border-bottom:1px solid var(--border);}
.tab-desc{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:12px 16px;margin-bottom:14px;font-size:.78rem;color:var(--text2);line-height:1.5;}
.stat-strip{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px 18px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:14px;}
.ss-val{font-family:monospace;font-size:1.3rem;font-weight:700;color:var(--text);line-height:1;}
.ss-lbl{font-family:monospace;font-size:.52rem;letter-spacing:.1em;color:var(--muted);text-transform:uppercase;margin-top:3px;}
.empty-st{text-align:center;padding:50px 20px;color:var(--muted);font-family:monospace;font-size:.68rem;letter-spacing:.1em;}
.section-h{font-family:monospace;font-size:.58rem;letter-spacing:.18em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border);}
.stg-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;margin-bottom:14px;}
.stg-title{font-family:monospace;font-size:.62rem;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:var(--text);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border);}
.hint{font-size:.72rem;color:var(--muted);line-height:1.4;margin-top:2px;padding:6px 10px;background:var(--panel);border-radius:5px;border-left:3px solid var(--border2);}
.hint b{color:var(--text2);}
.setting-help{font-size:.65rem;color:#7c3aed;background:#f5f3ff;border-left:3px solid #7c3aed;padding:4px 8px;border-radius:4px;margin-top:3px;line-height:1.4;}
.sentiment-bar{display:flex;align-items:center;gap:10px;margin:8px 0;padding:8px 12px;background:var(--panel);border-radius:6px;font-family:monospace;font-size:.62rem;}
.sbar-label{color:var(--muted);width:90px;flex-shrink:0;}
.sbar-track{flex:1;height:8px;background:var(--border);border-radius:4px;overflow:hidden;position:relative;}
.sbar-fill{height:100%;border-radius:4px;transition:width .3s;}
.sbar-val{color:var(--text);font-weight:600;width:40px;text-align:right;}
.momentum-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;font-family:monospace;font-size:.58rem;font-weight:700;letter-spacing:.06em;}
.dual-confirm{background:linear-gradient(135deg,#ff6b0015,#dc262615);border:2px solid #dc2626;border-radius:8px;padding:6px 14px;font-family:monospace;font-size:.7rem;font-weight:700;color:#dc2626;margin:6px 0;text-align:center;}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════
# DEMA + SUPERTREND — indicator helpers
# ═══════════════════════════════════════════════════════════════════════════

def _dst_ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

def _dst_dema(s, p):
    e1 = _dst_ema(s, p)
    return 2 * e1 - _dst_ema(e1, p)

def _dst_rsi(s, p=14):
    d    = s.diff()
    gain = d.clip(lower=0).ewm(com=p-1, min_periods=p).mean()
    loss = (-d.clip(upper=0)).ewm(com=p-1, min_periods=p).mean()
    return 100 - (100 / (1 + gain / loss))

def _dst_atr(h, l, c, p=14):
    pc = c.shift(1)
    tr = pd.concat([h-l, (h-pc).abs(), (l-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(com=p-1, min_periods=p).mean()

def _dst_supertrend(h, l, c, mult=3.0, period=10):
    atr_v     = _dst_atr(h, l, c, period)
    hl2       = (h + l) / 2
    upper_raw = hl2 + mult * atr_v
    lower_raw = hl2 - mult * atr_v
    upper     = upper_raw.copy()
    lower     = lower_raw.copy()
    direction = pd.Series(1.0, index=c.index)
    for i in range(1, len(c)):
        lower.iloc[i] = lower_raw.iloc[i] if (lower_raw.iloc[i] > lower.iloc[i-1] or c.iloc[i-1] < lower.iloc[i-1]) else lower.iloc[i-1]
        upper.iloc[i] = upper_raw.iloc[i] if (upper_raw.iloc[i] < upper.iloc[i-1] or c.iloc[i-1] > upper.iloc[i-1]) else upper.iloc[i-1]
        if   c.iloc[i] > upper.iloc[i-1]: direction.iloc[i] =  1
        elif c.iloc[i] < lower.iloc[i-1]: direction.iloc[i] = -1
        else:                              direction.iloc[i] = direction.iloc[i-1]
    st_line = pd.Series(np.where(direction == 1, lower, upper), index=c.index)
    return st_line, direction

def check_dst_signal(df, symbol, s=None):
    if df is None or len(df) < 100:
        return None
    if s is None:
        s = DEFAULT_SETTINGS
    if not s.get('dst_enabled', True):
        return None
    try:
        c = df["close"]; o = df["open"]
        h = df["high"];  l = df["low"]
        v = df["volume"]

        DEMA_LEN   = int(s.get('dst_dema_len',   21))
        ST_PERIOD  = int(s.get('dst_st_period',  10))
        ST_MULT    = float(s.get('dst_st_mult',   3.0))
        RSI_OB     = float(s.get('dst_rsi_ob',   65))
        RSI_OS     = float(s.get('dst_rsi_os',   35))
        MOM_BARS   = int(s.get('dst_mom_bars',    3))
        MIN_DEMA_D = float(s.get('dst_min_dema_dist', 0.1))
        FRESH_BARS = int(s.get('dst_fresh_bars',  5))
        SL_ATR     = float(s.get('dst_sl_atr',   1.5))
        TP_ATR     = float(s.get('dst_tp_atr',   3.0))
        VOL_MULT   = float(s.get('dst_vol_mult', 1.2))

        dema_line       = _dst_dema(c, DEMA_LEN)
        st_line, st_dir = _dst_supertrend(h, l, c, ST_MULT, ST_PERIOD)
        rsi             = _dst_rsi(c, 14)
        atr             = _dst_atr(h, l, c, 14)
        vol_ma          = v.rolling(20).mean()

        close_now = float(c.iloc[-1]); open_now  = float(o.iloc[-1])
        dema_now  = float(dema_line.iloc[-1])
        dir_now   = float(st_dir.iloc[-1])
        rsi_now   = float(rsi.iloc[-1])
        atr_now   = float(atr.iloc[-1])
        vol_now   = float(v.iloc[-1])
        volma_now = float(vol_ma.iloc[-1]) if float(vol_ma.iloc[-1]) > 0 else 1

        vol_ok    = (vol_now > volma_now * VOL_MULT) if s.get('dst_use_vol', True) else True
        rsi_long  = (40 < rsi_now < RSI_OB) if s.get('dst_use_rsi', True) else True
        rsi_short = (RSI_OS < rsi_now < 60) if s.get('dst_use_rsi', True) else True
        mom_long  = (close_now > float(c.iloc[-1 - MOM_BARS]) and close_now > open_now) if s.get('dst_use_mom', True) else True
        mom_short = (close_now < float(c.iloc[-1 - MOM_BARS]) and close_now < open_now) if s.get('dst_use_mom', True) else True
        dema_ok   = (abs(close_now - dema_now) / close_now * 100 > MIN_DEMA_D) if s.get('dst_use_dema_dist', True) else True

        changed    = st_dir != st_dir.shift(1)
        bars_since = 999
        for j in range(1, FRESH_BARS + 2):
            if len(changed) > j and changed.iloc[-j]:
                bars_since = j - 1
                break
        fresh = bars_since <= FRESH_BARS

        long_sig  = close_now > dema_now and dir_now ==  1 and vol_ok and rsi_long  and mom_long  and dema_ok and fresh
        short_sig = close_now < dema_now and dir_now == -1 and vol_ok and rsi_short and mom_short and dema_ok and fresh

        if not long_sig and not short_sig:
            return None

        direction = "LONG" if long_sig else "SHORT"
        sl  = close_now - SL_ATR * atr_now if long_sig else close_now + SL_ATR * atr_now
        tp  = close_now + TP_ATR * atr_now if long_sig else close_now - TP_ATR * atr_now
        rr  = round(abs(tp - close_now) / max(abs(sl - close_now), 0.000001), 2)
        tp1 = close_now + (tp - close_now) * 0.4 if long_sig else close_now - (close_now - tp) * 0.4
        tp3 = close_now + (tp - close_now) * 1.5 if long_sig else close_now - (close_now - tp) * 1.5

        return {
            "strategy":   "DST",
            "symbol":     symbol,
            "direction":  direction,
            "close":      close_now,
            "sl":         round(sl,  8),
            "tp":         round(tp,  8),
            "tp1":        round(tp1, 8),
            "tp2":        round(tp,  8),
            "tp3":        round(tp3, 8),
            "rr":         rr,
            "rsi":        round(rsi_now,   1),
            "vol_ratio":  round(vol_now / volma_now, 2),
            "dema":       round(dema_now,  8),
            "atr":        round(atr_now,   8),
            "fresh_bars": bars_since,
        }
    except:
        return None
        # ═══════════════════════════════════════════════════════════════════════════
# DST ISOLATED BACKGROUND SCANNER
# ═══════════════════════════════════════════════════════════════════════════

def run_dst_scan(coin_list, s, exchange_clients):
    """
    Fully isolated DEMA+ST scanner. Runs on 5m (or configured TF).
    If watchlist is enabled — builds its own coin list from watchlist directly.
    If watchlist is off — uses APEX5 coin list.
    """
    results = []
    tf = s.get('dst_timeframe', '5m')
    min_rr = float(s.get('dst_min_rr', 1.5))

    watchlist = s.get('dst_watchlist', [])
    use_watchlist = s.get('dst_use_watchlist', False)

    if use_watchlist and watchlist:
        # Build coin list directly from watchlist — independent of APEX5
        coin_list = []
        for base in watchlist:
            # Try USDT perp format on gate first, then mexc
            for ex_id, suffix in [('gate', '/USDT:USDT'), ('mexc', '/USDT:USDT'), ('gate', '/USDT')]:
                symbol = base + suffix
                coin_list.append({'symbol': symbol, 'exchange': ex_id})

    for item in coin_list:
        try:
            symbol = item if isinstance(item, str) else item.get('symbol', '')
            exchange_id = item.get('exchange', 'gate') if isinstance(item, dict) else 'gate'
            if not symbol:
                continue

            # Get exchange client
            ex = exchange_clients.get(exchange_id.lower())
            if ex is None:
                ex = exchange_clients.get('gate')
            if ex is None:
                continue

            # Fetch 5m OHLCV independently
            raw = asyncio.get_event_loop().run_until_complete(
                ex.fetch_ohlcv(symbol, tf, limit=250)
            ) if hasattr(ex, 'fetch_ohlcv') else []

            if not raw or len(raw) < 100:
                continue

            df = pd.DataFrame(raw, columns=['ts','open','high','low','close','volume'])
            df = df.astype(float)

            sig = check_dst_signal(df, symbol, s)
            if sig is None:
                continue
            if sig['rr'] < min_rr:
                continue

            sig['exchange'] = exchange_id.upper()
            sig['tf'] = tf
            utc_s, pkt_s = _dual_time()
            sig['scan_time'] = utc_s
            sig['scan_time_pkt'] = pkt_s
            results.append(sig)

        except:
            continue

    # Sort by R:R descending
    results.sort(key=lambda x: x.get('rr', 0), reverse=True)
    return results
# ═══════════════════════════════════════════════════════════════════════════
# GAINERS & LOSERS SCANNER — 4H catalyst + 5m entry
# ═══════════════════════════════════════════════════════════════════════════

def _gl_pct_change(df_4h):
    """Calculate 4H % change from open of first candle to current close."""
    try:
        if len(df_4h) < 2: return 0.0
        open_4h = float(df_4h['close'].iloc[-4]) if len(df_4h) >= 4 else float(df_4h['close'].iloc[0])
        close_now = float(df_4h['close'].iloc[-1])
        return (close_now - open_4h) / open_4h * 100
    except: return 0.0

def _gl_pct_change_1h(df_5m):
    """Calculate 1H % change using 5m candles (last 12 candles = 1H)."""
    try:
        if len(df_5m) < 12: return 0.0
        open_1h = float(df_5m['close'].iloc[-12])
        close_now = float(df_5m['close'].iloc[-1])
        return (close_now - open_1h) / open_1h * 100
    except: return 0.0

def _gl_dynamic_tpsl(df_5m, df_4h, close_now, direction='LONG'):
    """Calculate dynamic TP/SL from market structure."""
    try:
        h5 = df_5m['high']; l5 = df_5m['low']
        h4 = df_4h['high']; l4 = df_4h['low']

        # Swing lows and highs on last 30 5m bars
        swing_lows  = [float(l5.iloc[i]) for i in range(-30, -1)
                       if l5.iloc[i] < l5.iloc[i-1] and l5.iloc[i] < l5.iloc[i+1]]
        swing_highs = [float(h5.iloc[i]) for i in range(-30, -1)
                       if h5.iloc[i] > h5.iloc[i-1] and h5.iloc[i] > h5.iloc[i+1]]

        if direction == 'LONG':
            # SL: highest swing low below current price
            lows_below = [x for x in swing_lows if x < close_now * 0.999]
            sl = max(lows_below) if lows_below else close_now * 0.98

            # TP1: nearest swing high above current price
            highs_above = [x for x in swing_highs if x > close_now * 1.001]
            tp1 = min(highs_above) if highs_above else close_now * 1.02

            # TP2: 2x risk
            risk = close_now - sl
            tp2 = close_now + 2 * risk

            # TP3: 4H recent high
            tp3 = float(h4.iloc[-8:].max())
            if tp3 <= close_now: tp3 = close_now + 3 * risk

        else:  # SHORT
            # SL: lowest swing high above current price
            highs_above = [x for x in swing_highs if x > close_now * 1.001]
            sl = min(highs_above) if highs_above else close_now * 1.02

            # TP1: nearest swing low below current price
            lows_below = [x for x in swing_lows if x < close_now * 0.999]
            tp1 = max(lows_below) if lows_below else close_now * 0.98

            # TP2: 2x risk
            risk = sl - close_now
            tp2 = close_now - 2 * risk

            # TP3: 4H recent low
            tp3 = float(l4.iloc[-8:].min())
            if tp3 >= close_now: tp3 = close_now - 3 * risk

        risk = abs(close_now - sl)
        rr = round(abs(tp2 - close_now) / max(risk, 0.000001), 2)

        return {
            'sl': round(sl, 8),
            'tp': round(tp2, 8),
            'tp1': round(tp1, 8),
            'tp2': round(tp2, 8),
            'tp3': round(tp3, 8),
            'rr': rr
        }
    except:
        return None

def _gl_check_gainer_pullback(df_5m, df_4h, s, symbol):
    """Gainer Pullback — 4H up 3%+, 5m pulled back 1.5-8%, ready for second leg."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        chg_1h = _gl_pct_change_1h(df_5m)
        min_gain = float(s.get('gl_min_gain_pct', 3.0))
        if chg_4h < min_gain: return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])
        recent_high = float(h.iloc[-24:].max())
        pullback_pct = (recent_high - close_now) / recent_high * 100
        pb_min = float(s.get('gl_pullback_min', 1.5))
        pb_max = float(s.get('gl_pullback_max', 8.0))
        if not (pb_min <= pullback_pct <= pb_max): return None

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        if not (45 < rsi_now < float(s.get('gl_rsi_ob', 75))): return None

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_ratio = vol_now / volma
        if vol_ratio < float(s.get('gl_vol_expansion', 1.3)): return None
        vol_declining = float(v.iloc[-3:].mean()) < float(v.iloc[-8:-3].mean())
        if not vol_declining: return None

        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != 1: return None

        tpsl = _gl_dynamic_tpsl(df_5m, df_4h, close_now, 'LONG')
        if not tpsl: return None
        if tpsl['rr'] < float(s.get('gl_min_rr', 1.5)): return None

        return {
            'type': 'GAINER_PULLBACK', 'direction': 'LONG', 'symbol': symbol,
            'close': close_now, 'sl': tpsl['sl'], 'tp': tpsl['tp'],
            'tp1': tpsl['tp1'], 'tp2': tpsl['tp2'], 'tp3': tpsl['tp3'],
            'rr': tpsl['rr'], 'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(chg_1h, 2),
            'pullback_pct': round(pullback_pct, 2),
            'recent_high': round(recent_high, 8),
            'vol_ratio': round(vol_now / volma, 2) if volma > 0 else 0,
            'emoji': '🟢', 'label': 'GAINER PULLBACK'
        }
    except: return None

def _gl_check_gainer_breakout(df_5m, df_4h, s, symbol):
    """Gainer Breakout — 4H up 3%+, 5m breaking to new high with volume."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        if chg_4h < float(s.get('gl_min_gain_pct', 3.0)): return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])
        recent_high = float(h.iloc[-24:].max())
        breaking_out = close_now >= recent_high * 0.998

        if not breaking_out: return None

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        if not (55 < rsi_now < 80): return None

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_expanding = vol_now > volma * float(s.get('gl_vol_expansion', 1.5))
        if not vol_expanding: return None

        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != 1: return None

        tpsl = _gl_dynamic_tpsl(df_5m, df_4h, close_now, 'LONG')
        if not tpsl: return None
        if tpsl['rr'] < float(s.get('gl_min_rr', 1.5)): return None

        return {
            'type': 'GAINER_BREAKOUT', 'direction': 'LONG', 'symbol': symbol,
            'close': close_now, 'sl': tpsl['sl'], 'tp': tpsl['tp'],
            'tp1': tpsl['tp1'], 'tp2': tpsl['tp2'], 'tp3': tpsl['tp3'],
            'rr': rr, 'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(_gl_pct_change_1h(df_5m), 2),
            'vol_ratio': round(vol_now / volma, 2) if volma > 0 else 0,
            'emoji': '🚀', 'label': 'GAINER BREAKOUT'
        }
    except: return None

def _gl_check_loser_bounce(df_5m, df_4h, s, symbol):
    """Loser Bounce — 4H down 3%+, 5m showing capitulation + reversal signs."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        if chg_4h > -float(s.get('gl_min_loss_pct', 3.0)): return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        if not (15 < rsi_now < float(s.get('gl_rsi_os', 40)) + 10): return None

        # ST must be bearish (confirming downtrend — we are bouncing FROM a downtrend)
        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != -1: return None

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_spike = vol_now > volma * 1.5
        if not vol_spike: return None

        candle_shrinking = abs(float(c.iloc[-1]) - float(c.iloc[-2])) < abs(float(c.iloc[-2]) - float(c.iloc[-3]))
        if not candle_shrinking: return None

        tpsl = _gl_dynamic_tpsl(df_5m, df_4h, close_now, 'LONG')
        if not tpsl: return None
        if tpsl['rr'] < float(s.get('gl_min_rr', 1.5)): return None

        return {
            'type': 'LOSER_BOUNCE', 'direction': 'LONG', 'symbol': symbol,
            'close': close_now, 'sl': tpsl['sl'], 'tp': tpsl['tp'],
            'tp1': tpsl['tp1'], 'tp2': tpsl['tp2'], 'tp3': tpsl['tp3'],
            'rr': tpsl['rr'], 'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(_gl_pct_change_1h(df_5m), 2),
            'vol_ratio': round(vol_now / volma, 2) if volma > 0 else 0,
            'emoji': '🔄', 'label': 'LOSER BOUNCE'
        }
    except: return None

def _gl_check_loser_breakdown(df_5m, df_4h, s, symbol):
    """Loser Breakdown — 4H down 3%+, 5m breaking to new low with volume."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        if chg_4h > -float(s.get('gl_min_loss_pct', 3.0)): return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])
        recent_low = float(l.iloc[-24:].min())
        breaking_down = close_now <= recent_low * 1.002
        if not breaking_down: return None

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        if not (20 < rsi_now < 55): return None

        # ST must be bearish — confirming breakdown direction
        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != -1: return None

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_expanding = vol_now > volma * float(s.get('gl_vol_expansion', 1.5))
        if not vol_expanding: return None

        tpsl = _gl_dynamic_tpsl(df_5m, df_4h, close_now, 'SHORT')
        if not tpsl: return None
        if tpsl['rr'] < float(s.get('gl_min_rr', 1.5)): return None

        return {
            'type': 'LOSER_BREAKDOWN', 'direction': 'SHORT', 'symbol': symbol,
            'close': close_now, 'sl': tpsl['sl'], 'tp': tpsl['tp'],
            'tp1': tpsl['tp1'], 'tp2': tpsl['tp2'], 'tp3': tpsl['tp3'],
            'rr': tpsl['rr'], 'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(_gl_pct_change_1h(df_5m), 2),
            'vol_ratio': round(vol_now / volma, 2) if volma > 0 else 0,
            'emoji': '📉', 'label': 'LOSER BREAKDOWN'
        }
    except: return None

def _gl_check_pregainer(df_5m, df_4h, s, symbol):
    """Pre-Gainer — accumulation signs before the move starts."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        if chg_4h > 2.0 or chg_4h < -1.0: return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_increasing = float(v.iloc[-3:].mean()) > float(v.iloc[-8:-3].mean()) * 1.3
        if not vol_increasing: return None

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        rsi_crossing_50 = float(rsi.iloc[-2]) < 50 < rsi_now
        if not rsi_crossing_50: return None

        dema = _dst_dema(c, 200)
        dema_now = float(dema.iloc[-1])
        approaching_from_below = close_now < dema_now and abs(close_now - dema_now) / dema_now * 100 < 1.5
        above_dema = close_now > dema_now
        if not (approaching_from_below or above_dema): return None

        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != 1: return None

        return {
            'type': 'PRE_GAINER', 'direction': 'WATCH', 'symbol': symbol,
            'close': close_now, 'sl': None, 'tp': None, 'rr': None,
            'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(_gl_pct_change_1h(df_5m), 2),
            'vol_ratio': round(float(v.iloc[-1]) / float(vol_ma.iloc[-1]), 2),
            'emoji': '👀', 'label': 'PRE-GAINER WATCH'
        }
    except: return None

def _gl_check_preloser(df_5m, df_4h, s, symbol):
    """Pre-Loser — distribution signs before the drop starts."""
    try:
        chg_4h = _gl_pct_change(df_4h)
        if chg_4h < -2.0 or chg_4h > 1.0: return None

        c = df_5m['close']; h = df_5m['high']; l = df_5m['low']
        v = df_5m['volume']
        close_now = float(c.iloc[-1])

        vol_ma = v.rolling(20).mean()
        vol_now = float(v.iloc[-1])
        volma = float(vol_ma.iloc[-1])
        if volma <= 0: return None
        vol_declining_on_bounces = float(v.iloc[-3:].mean()) < float(v.iloc[-8:-3].mean()) * 0.7
        if not vol_declining_on_bounces: return None

        rsi = _dst_rsi(c)
        rsi_now = float(rsi.iloc[-1])
        rsi_crossing_50 = float(rsi.iloc[-2]) > 50 > rsi_now
        if not rsi_crossing_50: return None

        dema = _dst_dema(c, 200)
        dema_now = float(dema.iloc[-1])
        rejecting_dema = close_now < dema_now * 1.01 and close_now > dema_now * 0.99

        st_line, st_dir = _dst_supertrend(h, l, c, 3.0, 10)
        if float(st_dir.iloc[-1]) != -1: return None

        return {
            'type': 'PRE_LOSER', 'direction': 'WATCH', 'symbol': symbol,
            'close': close_now, 'sl': None, 'tp': None, 'rr': None,
            'rsi': round(rsi_now, 1),
            'chg_4h': round(chg_4h, 2), 'chg_1h': round(_gl_pct_change_1h(df_5m), 2),
            'vol_ratio': round(float(v.iloc[-1]) / float(vol_ma.iloc[-1]), 2),
            'emoji': '⚠️', 'label': 'PRE-LOSER WATCH'
        }
    except: return None

def run_gl_scan(coin_list, s, exchange_clients, status_placeholder=None):
    """
    Gainers & Losers scanner — fully independent.
    Fetches live tickers from exchange, ranks by 24H % change,
    picks top N gainers + top N losers, then runs 6 signal checks.
    """
    if not s.get('gl_enabled', True): return []
    results = []
    top_n = int(s.get('gl_top_n', 20))

    # ── Step 1: fetch live tickers to find real gainers/losers ──────────
    ex = exchange_clients.get('gate') or exchange_clients.get('mexc') or exchange_clients.get('bybit')
    if ex is None: return []

    try:
        import ccxt as ccxt_mod
        ex = ccxt_mod.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        tickers = ex.fetch_tickers()
    except Exception as e:
        print(f"[GL] Ticker fetch failed: {e}")
        return []

    # Filter to USDT perpetual futures only
    ranked = []
    for sym, t in tickers.items():
        if not sym.endswith('/USDT:USDT'): continue
        pct = t.get('percentage') or t.get('change') or 0
        vol = t.get('quoteVolume') or 0
        if vol < 500000: continue  # skip low volume coins
        try:
            ranked.append({'symbol': sym, 'pct': float(pct), 'vol': float(vol)})
        except: continue

    if not ranked:
        print("[GL] No tickers returned — check exchange connection")
        return []

    # Sort and pick top N gainers + top N losers (for active signals)
    gainers = sorted([r for r in ranked if r['pct'] > 0], key=lambda x: x['pct'], reverse=True)[:top_n]
    losers  = sorted([r for r in ranked if r['pct'] < 0], key=lambda x: x['pct'])[:top_n]
    active_list = gainers + losers

    # Full market scan for pre-gainer/pre-loser — flat coins between -2% and +2%
    flat_coins = sorted([r for r in ranked if -2.0 <= r['pct'] <= 2.0],
                        key=lambda x: x['vol'], reverse=True)[:100]

    total_coins = len(active_list) + len(flat_coins)
    print(f"[GL] Found {len(gainers)} gainers, {len(losers)} losers, {len(flat_coins)} flat coins for pre-signals")
    if status_placeholder:
        msg = f'🔍 Found {len(gainers)}g {len(losers)}l {len(flat_coins)}flat — {total_coins} total'
        status_placeholder.markdown(
            f'<div style="font-family:monospace;font-size:.65rem;color:#0284c7;">{msg}</div>',
            unsafe_allow_html=True)

    # Build separate coin lists
    # active_list — for pullback/breakout/bounce/breakdown signals
    # flat_coins — for pre-gainer/pre-loser signals only
    coin_list = active_list  # default for active signals

    checked = 0

    # ── Pass 1: Active signals on gainers/losers ──────────────────────
    active_checks = []
    if s.get('gl_alert_pullback', True):  active_checks.append(_gl_check_gainer_pullback)
    if s.get('gl_alert_breakout', True):  active_checks.append(_gl_check_gainer_breakout)
    if s.get('gl_alert_bounce', True):    active_checks.append(_gl_check_loser_bounce)
    if s.get('gl_alert_breakdown', True): active_checks.append(_gl_check_loser_breakdown)

    # ── Pass 2: Pre-signals on full flat coin universe ─────────────────
    pre_checks = []
    if s.get('gl_alert_pregainer', True): pre_checks.append(_gl_check_pregainer)
    if s.get('gl_alert_preloser', True):  pre_checks.append(_gl_check_preloser)

    # ── Pass 1: Active signals FIRST — top 20 gainers/losers only ─────
    for pass_coins, pass_checks in [(active_list, active_checks)]:
        if not pass_checks: continue
        for item in pass_coins:
            try:
                symbol = item['symbol']
                exchange_id = 'gate'
                if not symbol: continue

                import ccxt as _ccxt
                ex = _ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})

                checked += 1
                if status_placeholder and checked % 3 == 0:
                    msg2 = f'🔍 {checked}/{total_coins} — {symbol.replace("/USDT:USDT","")} | signals: {len(results)}'
                    status_placeholder.markdown(f'<div style="font-family:monospace;font-size:.65rem;color:#0284c7;">{msg2}</div>', unsafe_allow_html=True)
                raw_4h = ex.fetch_ohlcv(symbol, '4h', limit=50)
                raw_5m = ex.fetch_ohlcv(symbol, '5m', limit=250)

                if not raw_4h or len(raw_4h) < 10: continue
                if not raw_5m or len(raw_5m) < 100: continue

                df_4h = pd.DataFrame(raw_4h, columns=['ts','open','high','low','close','volume']).astype(float)
                df_5m = pd.DataFrame(raw_5m, columns=['ts','open','high','low','close','volume']).astype(float)

                chg_4h = _gl_pct_change(df_4h)
                chg_1h = _gl_pct_change_1h(df_5m)
                utc_str, pkt_str = _dual_time()
                scan_time = utc_str


                for check_fn in pass_checks:
                    sig = check_fn(df_5m, df_4h, s, symbol)
                    if sig:
                        sig['exchange'] = exchange_id.upper()
                        sig['scan_time'] = scan_time
                        sig['scan_time_pkt'] = pkt_str
                        sig['chg_4h'] = round(chg_4h, 2)
                        sig['chg_1h'] = round(chg_1h, 2)
                        results.append(sig)

            except: continue

    results.sort(key=lambda x: abs(x.get('chg_4h', 0)), reverse=True)

    # ── Pass 2: Pre-signals on flat coins — runs after active results ready ─
    if pre_checks:
        if status_placeholder:
            status_placeholder.markdown('<div style="font-family:monospace;font-size:.65rem;color:#7c3aed;">👀 Scanning flat coins for Pre-Gainer/Pre-Loser signals...</div>', unsafe_allow_html=True)
        for item in flat_coins:
            try:
                symbol = item['symbol']
                if not symbol: continue
                import ccxt as _ccxt
                ex = _ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
                raw_4h = ex.fetch_ohlcv(symbol, '4h', limit=50)
                raw_5m = ex.fetch_ohlcv(symbol, '5m', limit=250)
                if not raw_4h or len(raw_4h) < 10: continue
                if not raw_5m or len(raw_5m) < 100: continue
                df_4h = pd.DataFrame(raw_4h, columns=['ts','open','high','low','close','volume']).astype(float)
                df_5m = pd.DataFrame(raw_5m, columns=['ts','open','high','low','close','volume']).astype(float)
                chg_4h = _gl_pct_change(df_4h)
                chg_1h = _gl_pct_change_1h(df_5m)
                utc_str, pkt_str = _dual_time()
                for check_fn in pre_checks:
                    sig = check_fn(df_5m, df_4h, s, symbol)
                    if sig:
                        sig['exchange'] = 'GATE'
                        sig['scan_time'] = utc_str
                        sig['scan_time_pkt'] = pkt_str
                        sig['chg_4h'] = round(chg_4h, 2)
                        sig['chg_1h'] = round(chg_1h, 2)
                        results.append(sig)
            except: continue

    results.sort(key=lambda x: abs(x.get('chg_4h', 0)), reverse=True)
    return results

def send_gl_discord_alert(webhook_url, sig):
    """Send Gainers/Losers signal to Discord."""
    try:
        type_colors = {
            'GAINER_PULLBACK': 0x059669, 'GAINER_BREAKOUT': 0x10b981,
            'LOSER_BOUNCE': 0x0284c7, 'LOSER_BREAKDOWN': 0xdc2626,
            'PRE_GAINER': 0xf59e0b, 'PRE_LOSER': 0xf97316
        }
        color = type_colors.get(sig['type'], 0x6366f1)
        dir_emoji = "📗" if sig['direction'] == 'LONG' else "📕" if sig['direction'] == 'SHORT' else "👁"

        tp_sl_line = ""
        if sig.get('tp') and sig.get('sl'):
            tp_sl_line = f"\n🎯 **TP:** `${sig['tp']:.6f}` | 🛑 **SL:** `${sig['sl']:.6f}` | **R:R:** {sig['rr']}"

        embed = {
            'title': f"{sig['emoji']} {sig['label']} | {sig['symbol']} ({sig['exchange']})",
            'color': color,
            'description': (
                f"{dir_emoji} **{sig['direction']}** | 4H: **{sig['chg_4h']:+.2f}%** | 1H: **{sig['chg_1h']:+.2f}%**\n"
                f"**Entry:** `${sig['close']:.6f}`"
                f"{tp_sl_line}\n"
                f"📊 **RSI:** {sig['rsi']} | **VOL:** {sig['vol_ratio']}× avg"
            ),
            'footer': {'text': f'APEX5 — Gainers/Losers Scanner • {sig.get("scan_time","–")} | {sig.get("scan_time_pkt","–")}'}
        }
        requests.post(webhook_url, json={'embeds': [embed]}, timeout=5)
    except: pass

def send_dst_discord_alert(webhook_url, sig):
    """Send isolated DST signal to Discord."""
    try:
        color = 0x0284c7  # blue for DST signals
        direction_emoji = "📗" if sig['direction'] == 'LONG' else "📕"
        embed = {
            'title': f'🔵 DEMA+ST | {sig["symbol"]} ({sig["exchange"]}) — {sig["tf"]}',
            'color': color,
            'description': (
                f"{direction_emoji} **{sig['direction']}** | R:R **{sig['rr']}**\n"
                f"**Entry:** `${sig['close']:.6f}`\n"
                f"🎯 **TP1:** `${sig['tp1']:.6f}` | **TP2:** `${sig['tp2']:.6f}` | **TP3:** `${sig['tp3']:.6f}`\n"
                f"🛑 **SL:** `${sig['sl']:.6f}`\n"
                f"📊 **RSI:** {sig['rsi']} | **VOL:** {sig['vol_ratio']}× avg | **ST Flip:** {sig['fresh_bars']} bars ago\n"
                f"📐 **DEMA:** `${sig['dema']:.6f}` | **ATR:** `${sig['atr']:.6f}`"
            ),
            'footer': {'text': f'APEX5 — DEMA+ST Scanner • {sig.get("scan_time","–")} | {sig.get("scan_time_pkt","–")}'}
        }
        requests.post(webhook_url, json={'embeds': [embed]}, timeout=5)
    except:
        pass
# ═══════════════════════════════════════════════════════════════════════════
# ENGINE
# ═══════════════════════════════════════════════════════════════════════════
class PrePumpScreener:
    def __init__(self, cmc_key="", okx_key="", okx_secret="", okx_passphrase="", gate_key="", gate_secret=""):
        self.cmc_key=cmc_key
        okx_p={'enableRateLimit':True,'rateLimit':50,'timeout':8000,'options':{'defaultType':'swap'}}
        if okx_key and okx_secret and okx_passphrase:
            okx_p.update({'apiKey':okx_key,'secret':okx_secret,'password':okx_passphrase})
        self.okx=ccxt.okx(okx_p)
        self.mexc=ccxt.mexc({'enableRateLimit':True,'rateLimit':60,'timeout':8000,'options':{'defaultType':'swap'}})
        gate_p={'enableRateLimit':True,'rateLimit':50,'timeout':8000,'options':{'defaultType':'swap'}}
        if gate_key and gate_secret: gate_p.update({'apiKey':gate_key,'secret':gate_secret})
        self.gate=ccxt.gateio(gate_p)

    async def fetch_ohlcv(self,exch,sym,tf,n=200):
        try:
            raw=await exch.fetch_ohlcv(sym,tf,limit=n)
            if not raw: return pd.DataFrame()
            df=pd.DataFrame(raw,columns=['ts','open','high','low','close','volume'])
            df['ts']=pd.to_datetime(df['ts'],unit='ms')
            return df
        except: return pd.DataFrame()

    async def fetch_btc(self):
        try:
            df=await self.fetch_ohlcv(self.okx,"BTC/USDT:USDT","1h",80)
            if df.empty: df=await self.fetch_ohlcv(self.gate,"BTC/USDT:USDT","1h",80)
            if df.empty: df=await self.fetch_ohlcv(self.mexc,"BTC/USDT:USDT","1h",80)
            if df.empty: return "NEUTRAL",0,50
            df.ta.ema(length=20,append=True); df.ta.ema(length=50,append=True); df.ta.rsi(length=14,append=True)
            e20=[c for c in df.columns if 'EMA_20' in c]; e50=[c for c in df.columns if 'EMA_50' in c]
            rc=[c for c in df.columns if 'RSI' in c]
            if not e20 or not e50 or not rc: return "NEUTRAL",df['close'].iloc[-1],50
            p=df['close'].iloc[-1]; r=df[rc[0]].iloc[-1]
            if p<df[e20[0]].iloc[-1] and p<df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]<df[e50[0]].iloc[-1] and r<45:
                return "BEARISH",p,r
            elif p>df[e20[0]].iloc[-1] and p>df[e50[0]].iloc[-1] and df[e20[0]].iloc[-1]>df[e50[0]].iloc[-1] and r>55:
                return "BULLISH",p,r
            return "NEUTRAL",p,r
        except: return "NEUTRAL",0,50

    async def safe_fetch(self,exch,sym):
        fi={}; tick={}; ob={'bids':[],'asks':[]}
        try: fi=await exch.fetch_funding_rate(sym)
        except: fi={'fundingRate':0}
        try: tick=await exch.fetch_ticker(sym)
        except: pass
        try: ob=await exch.fetch_order_book(sym,limit=100)
        except: pass
        return fi,tick,ob

    async def fetch_oi(self,exch,sym):
        try:
            oi=await exch.fetch_open_interest(sym)
            return float(oi.get('openInterestValue') or oi.get('openInterest') or 0)
        except: return 0.0

    async def fetch_funding_history(self,exch,sym):
        try:
            h=await exch.fetch_funding_rate_history(sym,limit=24)
            return [float(x.get('fundingRate',0)) for x in h if x.get('fundingRate') is not None]
        except: return []

    async def fetch_oi_history(self,exch,sym):
        try:
            h=await exch.fetch_open_interest_history(sym,"1h",limit=24)
            return [float(x.get('openInterestValue',0) or x.get('openInterest',0)) for x in h]
        except: return []

    async def fetch_recent_trades(self,exch,sym,min_usdt=50000):
        try:
            trades=await exch.fetch_trades(sym,limit=100)
            wb=[]; ws=[]
            for t in trades:
                cost=float(t.get('cost',0) or 0); side=t.get('side',''); price=float(t.get('price',0) or 0)
                if cost>=min_usdt:
                    (wb if side=='buy' else ws).append({'cost':cost,'price':price})
            return wb,ws
        except: return [],[]

    async def fetch_orderflow_imbalance(self,exch,sym,lookback=10):
        try:
            df=await self.fetch_ohlcv(exch,sym,"5m",lookback+5)
            if df.empty or len(df)<lookback: return 0.0,'NEUTRAL'
            r=df.tail(lookback)
            bv=float(r[r['close']>r['open']]['volume'].sum())
            sv=float(r[r['close']<=r['open']]['volume'].sum())
            total=bv+sv
            if total==0: return 0.0,'NEUTRAL'
            bp=bv/total*100
            return bp,('BUY' if bp>55 else ('SELL' if bp<45 else 'NEUTRAL'))
        except: return 0.0,'NEUTRAL'

    async def fetch_4h_ob_and_fvg(self, exch, sym, price):
        """
        Fetch 4H Order Blocks AND Fair Value Gaps for HTF SL placement.
        OB: strong momentum candle (body>55% range) with move-away confirmation.
        FVG: gap between candle[i-1].high and candle[i+1].low (bullish) or vice versa (bearish).
        """
        bull_obs=[]; bear_obs=[]; bull_fvgs=[]; bear_fvgs=[]
        try:
            df=await self.fetch_ohlcv(exch,sym,"4h",60)
            if df.empty or len(df)<10: return bull_obs,bear_obs,bull_fvgs,bear_fvgs
            closes=df['close'].values; opens=df['open'].values
            highs=df['high'].values; lows=df['low'].values

            # ── Order Blocks ───────────────────────────────────────────────
            for i in range(len(df)-5,5,-1):
                c_open=float(opens[i]); c_close=float(closes[i])
                c_high=float(highs[i]); c_low=float(lows[i])
                c_range=c_high-c_low
                if c_range<=0: continue
                body=abs(c_close-c_open); body_pct=body/c_range
                if body_pct<0.55: continue
                if i+2>=len(df): continue
                if c_close>c_open:  # Bullish OB
                    move=(float(highs[i+2])-c_close)/c_close*100
                    if move>=0.8:
                        oh=c_high; ol=max(c_open,c_low)
                        if price>ol and price>oh*0.98:
                            dist=(price-oh)/price*100
                            if dist<=15: bull_obs.append({'zone_hi':oh,'zone_lo':ol,'dist_pct':dist,'body_pct':round(body_pct,2),'tf':'4H','type':'OB'})
                else:  # Bearish OB
                    move=(c_close-float(lows[i+2]))/c_close*100
                    if move>=0.8:
                        ol=c_low; oh=min(c_open,c_high)
                        if price<oh and price<ol*1.02:
                            dist=(ol-price)/price*100
                            if dist<=15: bear_obs.append({'zone_hi':oh,'zone_lo':ol,'dist_pct':dist,'body_pct':round(body_pct,2),'tf':'4H','type':'OB'})

            # ── Fair Value Gaps ────────────────────────────────────────────
            # Bullish FVG: candle[i-1].high < candle[i+1].low (gap up — support)
            # Bearish FVG: candle[i-1].low > candle[i+1].high (gap down — resistance)
            for i in range(2, len(df)-1):
                prev_h=float(highs[i-1]); prev_l=float(lows[i-1])
                next_h=float(highs[i+1]); next_l=float(lows[i+1])
                # Bullish FVG: gap between prev candle high and next candle low
                if prev_h < next_l:
                    fvg_lo=prev_h; fvg_hi=next_l; mid=(fvg_lo+fvg_hi)/2
                    if price>fvg_lo:
                        dist=(price-fvg_hi)/price*100 if price>fvg_hi else 0
                        if dist<=12:
                            bull_fvgs.append({'zone_hi':fvg_hi,'zone_lo':fvg_lo,'mid':mid,'dist_pct':dist,'tf':'4H','type':'FVG'})
                # Bearish FVG: gap between next candle high and prev candle low
                if prev_l > next_h:
                    fvg_hi=prev_l; fvg_lo=next_h; mid=(fvg_lo+fvg_hi)/2
                    if price<fvg_hi:
                        dist=(fvg_lo-price)/price*100 if price<fvg_lo else 0
                        if dist<=12:
                            bear_fvgs.append({'zone_hi':fvg_hi,'zone_lo':fvg_lo,'mid':mid,'dist_pct':dist,'tf':'4H','type':'FVG'})

            bull_obs.sort(key=lambda x:x['dist_pct']); bear_obs.sort(key=lambda x:x['dist_pct'])
            bull_fvgs.sort(key=lambda x:x['dist_pct']); bear_fvgs.sort(key=lambda x:x['dist_pct'])
        except: pass
        return bull_obs,bear_obs,bull_fvgs,bear_fvgs

    async def fetch_liquidation_map(self,exch,sym,price):
        clusters=[]
        try:
            dsym_c=sym.split(':')[0].replace('/USDT','')
            r=requests.get("https://www.okx.com/api/v5/public/liquidation-orders",
                params={"instType":"SWAP","instId":f"{dsym_c}-USDT-SWAP","state":"unfilled","limit":"100"},timeout=5)
            if r.status_code==200:
                raw=r.json().get('data',[]); items=raw[0] if raw else []; buckets={}
                for item in items:
                    try:
                        liq_px=float(item.get('bkPx',0)); liq_sz=float(item.get('sz',0))*liq_px
                        side='SHORT_LIQ' if item.get('side')=='sell' else 'LONG_LIQ'
                        if liq_px<=0 or liq_sz<10000: continue
                        bucket=round(liq_px/price,3); key=(bucket,side)
                        if key not in buckets: buckets[key]={'price':liq_px,'side':side,'size_usd':0,'count':0}
                        buckets[key]['size_usd']+=liq_sz; buckets[key]['count']+=1
                    except: pass
                for (bkt,side),data in buckets.items():
                    if data['size_usd']<50000: continue
                    dist=abs(data['price']-price)/price*100
                    clusters.append({**data,'dist_pct':dist})
                clusters.sort(key=lambda x:x['dist_pct'])
        except: pass
        return clusters

    def fetch_new_listings(self,s):
        cache=st.session_state.get('listing_cache',{})
        if cache.get('ts') and time.time()-cache['ts']<1800: return cache.get('data',[])
        listings=[]; now=time.time()
        try:
            r=requests.get("https://www.okx.com/api/v5/public/instruments",params={"instType":"SWAP"},timeout=7)
            if r.status_code==200:
                cutoff_ms=(now-7*86400)*1000
                for inst in r.json().get('data',[]):
                    try:
                        lt=float(inst.get('listTime',0))
                        if lt>=cutoff_ms:
                            sym_c=inst['instId'].replace('-USDT-SWAP','')
                            listings.append({'symbol':sym_c,'exchange':'OKX','listed_ts':lt,'listed_ago_h':(now*1000-lt)/3600000})
                    except: pass
        except: pass
        try:
            r2=requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/contracts",timeout=7)
            if r2.status_code==200:
                cutoff_s=now-7*86400; existing={l['symbol'] for l in listings}
                for c in r2.json():
                    try:
                        ct=float(c.get('create_time',0))
                        if ct>=cutoff_s:
                            sym_c=c['name'].replace('_USDT','')
                            if sym_c not in existing:
                                listings.append({'symbol':sym_c,'exchange':'GATE','listed_ts':ct*1000,'listed_ago_h':(now-ct)/3600})
                    except: pass
        except: pass
        st.session_state.listing_cache={'ts':now,'data':listings}
        return listings

    def fetch_onchain_whale(self,sym,s):
        min_usd=s.get('onchain_whale_min',500000); cache=st.session_state.get('onchain_cache',{}); now=time.time()
        if sym in cache and now-cache[sym].get('ts',0)<600: return cache[sym]
        result={'available':False,'signal':'NEUTRAL','detail':'','inflow':0,'outflow':0,'ts':now}
        try:
            r=requests.get("https://api.whale-alert.io/v1/transactions",
                params={"api_key":"free","min_value":str(int(min_usd)),"currency":sym.lower().replace('1000',''),
                        "limit":"20","start":str(int(now-3600))},timeout=5)
            if r.status_code==200:
                txns=r.json().get('transactions',[])
                inflow=sum(t.get('amount_usd',0) for t in txns if t.get('to',{}).get('owner_type')=='exchange')
                outflow=sum(t.get('amount_usd',0) for t in txns if t.get('from',{}).get('owner_type')=='exchange')
                if inflow+outflow>=min_usd:
                    net=outflow-inflow
                    result={'available':True,'ts':now,'signal':'BULLISH' if net>0 else 'BEARISH',
                            'detail':f"ExchIn ${inflow/1e6:.1f}M | ExchOut ${outflow/1e6:.1f}M",'inflow':inflow,'outflow':outflow}
        except: pass
        if not result['available']:
            try:
                CG={'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin','XRP':'ripple',
                    'ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2','LINK':'chainlink',
                    'DOT':'polkadot','MATIC':'matic-network','OP':'optimism','ARB':'arbitrum',
                    'SUI':'sui','APT':'aptos','PEPE':'pepe','WIF':'dogwifcoin','TON':'the-open-network'}
                cg_id=CG.get(sym.upper(),sym.lower())
                r2=requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization":"false","tickers":"false","market_data":"true","developer_data":"false"},timeout=6)
                if r2.status_code==200:
                    md=r2.json().get('market_data',{})
                    vol=float(md.get('total_volume',{}).get('usd',0) or 0)
                    mcp=float(md.get('market_cap',{}).get('usd',0) or 0)
                    c1h=float(md.get('price_change_percentage_1h_in_currency',{}).get('usd',0) or 0)
                    vr=vol/mcp if mcp>0 else 0
                    if vr>0.08 and abs(c1h)>0.8:
                        result={'available':True,'ts':now,'signal':'BULLISH' if c1h>0 else 'BEARISH',
                                'detail':f"Vol/MCap {vr:.2f}x | 1h {c1h:+.1f}% (CG proxy)",'inflow':0,'outflow':0}
            except: pass
        if 'onchain_cache' not in st.session_state: st.session_state.onchain_cache={}
        st.session_state.onchain_cache[sym]=result
        return result

    def fetch_sentiment_data(self,sym_base,exch_name):
        result={'top_long_pct':50.0,'top_short_pct':50.0,'retail_long_pct':50.0,'taker_buy_pct':50.0,'available':False,'source':''}
        try:
            if exch_name=="GATE":
                ccy=sym_base.replace("-USDT-SWAP","").replace("USDT","").replace("_USDT","")
                r1=requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/contract_stats",
                    params={"contract":f"{ccy}_USDT","interval":"1h","limit":8},timeout=4)
                if r1.status_code==200:
                    rows=r1.json()
                    if rows:
                        lsr=float(rows[-1].get('lsr_account',1.0) or 1.0)
                        lp=(lsr/(1+lsr))*100
                        result.update({'top_long_pct':lp,'top_short_pct':100-lp,'retail_long_pct':lp,'available':True,'source':'Gate'})
                r2=requests.get("https://fx-api.gateio.ws/api/v4/futures/usdt/trades",
                    params={"contract":f"{ccy}_USDT","limit":100},timeout=4)
                if r2.status_code==200:
                    trades=r2.json()
                    bv=sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0))>0)
                    sv=sum(abs(float(t.get('size',0))) for t in trades if float(t.get('size',0))<0)
                    total=bv+sv
                    if total>0: result['taker_buy_pct']=(bv/total)*100
            elif exch_name=="OKX":
                r1=requests.get("https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio",
                    params={"ccy":sym_base.replace("-USDT-SWAP","").replace("USDT",""),"period":"1H"},timeout=4)
                if r1.status_code==200:
                    rows=r1.json().get('data',[])
                    if rows:
                        ls_ratio=float(rows[0][1]); lp=(ls_ratio/(ls_ratio+1))*100
                        result.update({'top_long_pct':lp,'top_short_pct':100-lp,'retail_long_pct':lp,'available':True,'source':'OKX'})
                r2=requests.get("https://www.okx.com/api/v5/rubik/stat/taker-volume",
                    params={"ccy":sym_base.replace("-USDT-SWAP","").replace("USDT",""),"instType":"CONTRACTS","period":"5m"},timeout=4)
                if r2.status_code==200:
                    rows=r2.json().get('data',[])[:12]
                    if rows:
                        bv=sum(float(r[1]) for r in rows); sv=sum(float(r[2]) for r in rows)
                        total=bv+sv
                        if total>0: result['taker_buy_pct']=(bv/total)*100
        except: pass
        return result

    def fetch_reddit_buzz(self,coin_sym,s):
        if not s.get('social_enabled',True): return {'mentions':0,'score':0,'available':False,'source':''}
        sym=coin_sym.upper().replace('1000','').replace('10000','')
        cache=st.session_state.get('social_cache',{}); now=time.time()
        if sym in cache and (now-cache[sym].get('ts',0))<300: return cache[sym]
        result={'mentions':0,'score':0,'upvote_avg':0,'available':False,'source':'','sentiment':'NEUTRAL','top_post':'','ts':now}
        max_pts=s.get('social_reddit_weight',8); min_ment=s.get('social_min_mentions',3); buzz_thr=s.get('social_buzz_threshold',10)
        try:
            subs="CryptoCurrency+CryptoMoonShots+SatoshiStreetBets+altcoin+CryptoMarkets"
            rr=requests.get(f"https://www.reddit.com/r/{subs}/search.json",
                params={"q":sym,"sort":"new","t":"hour","limit":25,"restrict_sr":"1"},
                headers={"User-Agent":"Mozilla/5.0 APEX5/3.0"},timeout=6)
            if rr.status_code==200:
                posts=rr.json().get('data',{}).get('children',[])
                if posts:
                    mentions=len(posts); upvotes=[p['data'].get('score',0) for p in posts]
                    avg_up=sum(upvotes)/max(1,len(upvotes))
                    bull_kw=['moon','pump','buy','bullish','launch','listing','breakout','gem','surge','ath']
                    bear_kw=['dump','crash','sell','bearish','scam','rug','dead','rekt','fraud','fail']
                    bh=0; brh=0
                    for p in posts:
                        txt=(p['data'].get('title','')+' '+p['data'].get('selftext','')).lower()
                        bh+=sum(1 for w in bull_kw if w in txt); brh+=sum(1 for w in bear_kw if w in txt)
                    sent=('BULLISH' if bh>brh+1 else 'BEARISH' if brh>bh+1 else 'NEUTRAL')
                    sc=min(max_pts,int((mentions/buzz_thr)*max_pts)) if mentions>=min_ment else 0
                    result={'mentions':mentions,'score':sc,'upvote_avg':avg_up,'available':True,'source':'Reddit',
                            'sentiment':sent,'top_post':posts[0]['data'].get('title','')[:60],'ts':now}
        except: pass
        if not result['available']:
            try:
                cg_map={'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','BNB':'binancecoin','XRP':'ripple',
                        'ADA':'cardano','DOGE':'dogecoin','AVAX':'avalanche-2','LINK':'chainlink',
                        'DOT':'polkadot','MATIC':'matic-network','LTC':'litecoin','UNI':'uniswap',
                        'ATOM':'cosmos','XLM':'stellar','NEAR':'near','APT':'aptos','ARB':'arbitrum',
                        'OP':'optimism','INJ':'injective-protocol','SUI':'sui','TIA':'celestia',
                        'PEPE':'pepe','SHIB':'shiba-inu','WIF':'dogwifhat','BONK':'bonk'}
                cg_id=cg_map.get(sym,sym.lower())
                cg=requests.get(f"https://api.coingecko.com/api/v3/coins/{cg_id}",
                    params={"localization":"false","tickers":"false","market_data":"true","community_data":"true","developer_data":"false"},timeout=6)
                if cg.status_code==200:
                    data=cg.json(); cd=data.get('community_data',{}); md=data.get('market_data',{})
                    subs_count=cd.get('reddit_subscribers',0) or 0; active=cd.get('reddit_accounts_active_48h',0) or 0
                    tw_foll=cd.get('twitter_followers',0) or 0
                    c1h=md.get('price_change_percentage_1h_in_currency',{}).get('usd',0) or 0
                    sent=('BULLISH' if c1h>1.5 else 'BEARISH' if c1h<-1.5 else 'NEUTRAL')
                    mp=min(50,int(active/50)) if active else (2 if subs_count>50000 else 0)
                    sc=min(max_pts,int((mp/buzz_thr)*max_pts)) if mp>=min_ment else 0
                    top=(f"r/ {subs_count:,} subs | {active:,} active 48h"+(f" | Twitter {tw_foll:,}" if tw_foll else "")+(f" | 1h {c1h:+.2f}%" if c1h else ""))
                    result={'mentions':mp,'score':sc,'upvote_avg':0,'available':True,'source':'CoinGecko',
                            'sentiment':sent,'top_post':top[:80],'ts':now}
            except: pass
        if 'social_cache' not in st.session_state: st.session_state.social_cache={}
        st.session_state.social_cache[sym]=result
        return result

    def cmc_data(self,sym):
        if not self.cmc_key: return None
        try:
            r=requests.get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                headers={"X-CMC_PRO_API_KEY":self.cmc_key},params={"symbol":sym.replace("1000","")},timeout=3)
            if r.status_code==200:
                d=r.json()['data']; coin=d[list(d.keys())[0]]; q=coin['quote']['USD']
                return {'rank':coin.get('cmc_rank',9999),'mcap':q.get('market_cap') or 0,
                        'vol24':q.get('volume_24h') or 0,'change24':q.get('percent_change_24h') or 0}
        except: pass
        return None

    def ob_score_calc(self,bids,asks,sig,s):
        try:
            def valid(levels):
                out=[]
                for lv in levels:
                    try:
                        p,q=float(lv[0]),float(lv[1])
                        if p>0 and q>0: out.append((p,q))
                    except: pass
                return out
            b=valid(bids); a=valid(asks)
            if not b or not a: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}
            bv=sum(p*q for p,q in b); av=sum(p*q for p,q in a)
            tot=bv+av; bid_pct=(bv/tot*100) if tot>0 else 50; ratio=bv/av if av>0 else 1.0
            wb=max(b,key=lambda x:x[0]*x[1]); wa=max(a,key=lambda x:x[0]*x[1])
            wbv=wb[0]*wb[1]; wav=wa[0]*wa[1]; score=0; msg=""
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
            return score,msg,{'bid_pct':bid_pct,'ratio':ratio,'whale_bid_val':wbv,'whale_ask_val':wav,'whale_bid_px':wb[0],'whale_ask_px':wa[0]}
        except: return 0,"",{'bid_pct':50,'ratio':1.0,'whale_bid_val':0,'whale_ask_val':0,'whale_bid_px':0,'whale_ask_px':0}

    def find_whale_walls(self,bids,asks,price,sig,s):
        wmin=s['whale_min_usdt']; whale_sc=0; whale_details=[]; reasons_out=[]
        def parse_walls(levels,side):
            walls=[]
            for lv in levels:
                try:
                    p,q=float(lv[0]),float(lv[1]); val=p*q
                    if val>=wmin: walls.append({'price':p,'value':val,'dist_pct':abs(p-price)/price*100,'side':side})
                except: pass
            return sorted(walls,key=lambda x:x['value'],reverse=True)
        bid_walls=parse_walls(bids,'BID'); ask_walls=parse_walls(asks,'ASK')
        near=s.get('pts_whale_near',25); mid=s.get('pts_whale_mid',15); far=s.get('pts_whale_far',8)
        if sig=="LONG" and bid_walls:
            w=bid_walls[0]
            if w['dist_pct']<=0.5: whale_sc=near
            elif w['dist_pct']<=1.5: whale_sc=mid
            elif w['dist_pct']<=3.0: whale_sc=far
            if whale_sc>0:
                whale_details.append({'side':'BUY','value':w['value'],'price':w['price'],'dist_pct':w['dist_pct']})
                reasons_out.append(f"🐋 BUY WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% below)")
            if len(bid_walls)>=3: whale_sc+=5; reasons_out.append(f"🐋 {len(bid_walls)} stacked bid walls — layered buy support")
        elif sig=="SHORT" and ask_walls:
            w=ask_walls[0]
            if w['dist_pct']<=0.5: whale_sc=near
            elif w['dist_pct']<=1.5: whale_sc=mid
            elif w['dist_pct']<=3.0: whale_sc=far
            if whale_sc>0:
                whale_details.append({'side':'SELL','value':w['value'],'price':w['price'],'dist_pct':w['dist_pct']})
                reasons_out.append(f"🐋 SELL WALL {fmt(w['value'])} @ ${w['price']:.6f} ({w['dist_pct']:.2f}% above)")
            if len(ask_walls)>=3: whale_sc+=5; reasons_out.append(f"🐋 {len(ask_walls)} stacked ask walls — layered sell resistance")
        whale_str=""
        if whale_details:
            w=whale_details[0]
            whale_str=f"{'BUY' if w['side']=='BUY' else 'SELL'} {fmt(w['value'])} @ ${w['price']:.6f}"
        return whale_sc,whale_str,whale_details,reasons_out

    async def analyze(self,exch_name,exch_obj,sym,s,btc_trend):
        dsym=sym.split(':')[0].replace('/USDT','')
        if s['cooldown_on'] and is_on_cooldown(dsym,s['cooldown_hrs']): return None
        df_f=await self.fetch_ohlcv(exch_obj,sym,s['fast_tf'],200)
        df_slow=await self.fetch_ohlcv(exch_obj,sym,s['slow_tf'],100)
        fi,tick,ob=await self.safe_fetch(exch_obj,sym)
        oi=await self.fetch_oi(exch_obj,sym)
        if df_f.empty or df_slow.empty: return None
        price=float(tick.get('last',0) or 0)
        if price<=0: return None
        try:
            df_slow.ta.ema(length=50,append=True)
            df_f.ta.rsi(length=14,append=True); df_f.ta.atr(length=14,append=True)
            df_f.ta.macd(fast=12,slow=26,signal=9,append=True); df_f.ta.bbands(length=20,std=2,append=True)
            e50_cols=[c for c in df_slow.columns if 'EMA_50' in c or ('EMA' in c and '50' in c)]
            rsi_cols=[c for c in df_f.columns if 'RSI_14' in c or 'RSI' in c]
            atr_cols=[c for c in df_f.columns if 'ATRr_14' in c or 'ATRr' in c]
            macd_cols=[c for c in df_f.columns if c.startswith('MACD_') and not c.startswith('MACDh') and not c.startswith('MACDs')]
            macds_cols=[c for c in df_f.columns if c.startswith('MACDs_')]
            bbl_cols=[c for c in df_f.columns if c.startswith('BBL_')]
            bbu_cols=[c for c in df_f.columns if c.startswith('BBU_')]
            if not e50_cols or not rsi_cols or not atr_cols: return None
            e50=float(df_slow[e50_cols[0]].iloc[-1]); rsi=float(df_f[rsi_cols[0]].iloc[-1])
            atr=float(df_f[atr_cols[0]].iloc[-1]); macd=float(df_f[macd_cols[0]].iloc[-1]) if macd_cols else 0
            macds=float(df_f[macds_cols[0]].iloc[-1]) if macds_cols else 0
            bbl=float(df_f[bbl_cols[0]].iloc[-1]) if bbl_cols else price*0.97
            bbu=float(df_f[bbu_cols[0]].iloc[-1]) if bbu_cols else price*1.03
            vma=float(df_f['volume'].rolling(20).mean().iloc[-1]); lvol=float(df_f['volume'].iloc[-1])
            if vma==0: vma=1
            rsi_series_raw=df_f[rsi_cols[0]].dropna().values
        except: return None

        sig="LONG" if float(df_slow['close'].iloc[-1])>e50 else "SHORT"
        if s['btc_filter'] and btc_trend=="BEARISH" and sig=="LONG": return None
        qv_now=float(tick.get('quoteVolume',0) or 0)
        if s.get('min_vol_filter',300000)>0 and qv_now<s['min_vol_filter']: return None
        atr_pct=(atr/price*100) if price>0 else 0
        if atr_pct<s.get('atr_min_pct',0.2): return None
        if atr_pct>s.get('atr_max_pct',10.0): return None
        spread_max=s.get('spread_max_pct',0.5)
        if spread_max>0:
            bids_raw=ob.get('bids',[]); asks_raw=ob.get('asks',[])
            if bids_raw and asks_raw:
                best_bid=float(bids_raw[0][0]); best_ask=float(asks_raw[0][0])
                if best_bid>0 and (best_ask-best_bid)/best_bid*100>spread_max: return None

        pump_score=0; reasons=[]; bd={}; warnings=[]
        try: pc20=((price-float(df_f['close'].iloc[-20]))/float(df_f['close'].iloc[-20]))*100
        except: pc20=0

        # ── Late Entry Detector ───────────────────────────────────────────
        late_entry_penalty=0; late_entry_flag=False
        late_chg_thresh=s.get('late_entry_chg_thresh',8.0)
        if abs(pc20)>late_chg_thresh:
            try:
                closes=df_f['close'].values
                base_price=float(df_f['close'].iloc[-21]) if len(df_f)>=21 else float(df_f['close'].iloc[0])
                crossed_at=None
                for ci in range(len(closes)-1,max(len(closes)-25,0),-1):
                    if abs((float(closes[ci])-base_price)/base_price*100)<late_chg_thresh*0.5: crossed_at=ci; break
                candles_since=len(closes)-1-(crossed_at or (len(closes)-15))
                if candles_since>10:
                    late_entry_penalty=s.get('late_entry_penalty',20); late_entry_flag=True
                    d="LATE ENTRY" if sig=="LONG" else "LATE SHORT"
                    warnings.append(f"⚠️ {d}: {abs(pc20):.1f}% move was {candles_since} candles ago — entry window likely closed")
            except: pass
        pump_score-=late_entry_penalty; bd['late_entry_penalty']=-late_entry_penalty

        # ── Volume Exhaustion ─────────────────────────────────────────────
        vol_exhaust_penalty=0; vol_exhaust_flag=False
        try:
            vol5_avg=float(df_f['volume'].rolling(5).mean().iloc[-1])
            high20=float(df_f['high'].rolling(20).max().iloc[-1])
            if high20>0 and abs(price-high20)/high20*100<3.0 and vol5_avg>0 and lvol<vol5_avg*0.60 and sig=="LONG":
                vol_exhaust_penalty=s.get('vol_exhaust_penalty',15); vol_exhaust_flag=True
                warnings.append(f"⚠️ DISTRIBUTION PHASE: Volume {lvol/vol5_avg:.1f}× avg near 20c high — smart money selling")
        except: pass
        pump_score-=vol_exhaust_penalty; bd['vol_exhaust_penalty']=-vol_exhaust_penalty

        # ── RSI Direction ─────────────────────────────────────────────────
        rsi_direction='FLAT'; rsi_dir_pts=0; rsi_dir_reason=""
        try:
            if len(rsi_series_raw)>=4:
                r3=rsi_series_raw[-3:]; p3=rsi_series_raw[-6:-3] if len(rsi_series_raw)>=6 else rsi_series_raw[:3]
                rna=float(r3.mean()); rpa=float(p3.mean()); slope=rna-rpa
                if sig=="LONG":
                    if slope>3: rsi_direction='RISING'; rsi_dir_pts=8; rsi_dir_reason=f"📈 RSI rising ({rpa:.1f}→{rna:.1f}) — momentum building UP"
                    elif slope<-3: rsi_direction='FALLING'; rsi_dir_pts=-8; rsi_dir_reason=f"⚠️ RSI declining ({rpa:.1f}→{rna:.1f}) — LONG momentum fading"
                else:
                    if slope<-3: rsi_direction='FALLING'; rsi_dir_pts=8; rsi_dir_reason=f"📉 RSI falling ({rpa:.1f}→{rna:.1f}) — momentum building DOWN"
                    elif slope>3: rsi_direction='RISING'; rsi_dir_pts=-8; rsi_dir_reason=f"⚠️ RSI rising ({rpa:.1f}→{rna:.1f}) — SHORT momentum fading"
        except: pass
        if rsi_dir_pts!=0:
            pump_score+=rsi_dir_pts
            if rsi_dir_reason and rsi_dir_pts>0: reasons.append(rsi_dir_reason)
            elif rsi_dir_pts<0 and abs(rsi_dir_pts)>=8: warnings.append(rsi_dir_reason)
        bd['rsi_direction']=rsi_dir_pts

        # ── Near Local Top Penalty ────────────────────────────────────────
        near_top_penalty=0; near_top_flag=False
        try:
            high20_val=float(df_f['high'].rolling(20).max().iloc[-1])
            if high20_val>0:
                dist_from_top=abs(price-high20_val)/high20_val*100
                md_declining=False; rsi_declining=False
                if macd_cols and len(df_f)>=3:
                    mv=df_f[macd_cols[0]].values
                    md_declining=float(mv[-1])<float(mv[-2])<float(mv[-3])
                if len(rsi_series_raw)>=3:
                    rsi_declining=float(rsi_series_raw[-1])<float(rsi_series_raw[-2])<float(rsi_series_raw[-3])
                if dist_from_top<3.0 and md_declining and rsi_declining and sig=="LONG":
                    near_top_penalty=s.get('near_top_penalty',15); near_top_flag=True
                    warnings.append(f"⚠️ NEAR LOCAL TOP: Price {dist_from_top:.1f}% from 20c high, MACD+RSI declining")
        except: pass
        pump_score-=near_top_penalty; bd['near_top_penalty']=-near_top_penalty

        # 1 ── ORDER BOOK ──────────────────────────────────────────────────
        ob_sc,ob_msg,ob_data=self.ob_score_calc(ob.get('bids',[]),ob.get('asks',[]),sig,s)
        pump_score+=ob_sc; bd['ob_imbalance']=ob_sc
        if ob_msg: reasons.append(ob_msg)

        # 2 ── WHALE WALLS ─────────────────────────────────────────────────
        whale_sc,whale_str,whale_details,whale_reasons=self.find_whale_walls(ob.get('bids',[]),ob.get('asks',[]),price,sig,s)
        pump_score+=whale_sc; bd['whale_wall']=whale_sc; reasons.extend(whale_reasons)

        # 3 ── FUNDING RATE ────────────────────────────────────────────────
        fr=float(fi.get('fundingRate',0) or 0); fr_sc=0
        if sig=="LONG":
            if fr<=-s['funding_high']:   fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme neg funding {fr*100:.4f}% — shorts will be squeezed")
            elif fr<=-s['funding_mid']:  fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong neg funding {fr*100:.4f}% — squeeze building")
            elif fr<=-s['funding_low']:  fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Neg funding {fr*100:.4f}%")
            elif fr<0:                   fr_sc=3;                     reasons.append(f"⚡ Slightly neg funding {fr*100:.4f}%")
        else:
            if fr>=s['funding_high']:    fr_sc=s['pts_funding_high']; reasons.append(f"⚡ Extreme pos funding {fr*100:.4f}% — longs squeezed")
            elif fr>=s['funding_mid']:   fr_sc=s['pts_funding_mid'];  reasons.append(f"⚡ Strong pos funding {fr*100:.4f}%")
            elif fr>=s['funding_low']:   fr_sc=s['pts_funding_low'];  reasons.append(f"⚡ Pos funding {fr*100:.4f}%")
            elif fr>0:                   fr_sc=3;                     reasons.append(f"⚡ Slightly pos funding {fr*100:.4f}%")
        pump_score+=fr_sc; bd['funding']=fr_sc

        # 4 ── FUNDING HISTORY ─────────────────────────────────────────────
        funding_history=await self.fetch_funding_history(exch_obj,sym); funding_hist_sc=0; funding_age_penalty=0
        if len(funding_history)>=6 and abs(pc20)>5.0:
            try:
                early=funding_history[:6]; late=funding_history[-6:]
                avg_e=sum(early)/len(early); avg_l=sum(late)/len(late)
                if sig=="LONG" and pc20>5.0:
                    if avg_e>-0.0001 and avg_l<-0.0003:
                        funding_age_penalty=10; warnings.append(f"⚠️ FUNDING TRAP: Negative funding appeared AFTER pump — shorts may be right")
                    elif avg_e<-0.0003 and avg_l<-0.0001:
                        funding_hist_sc+=8; reasons.append(f"⚡ Funding negative BEFORE price surge — classic true squeeze setup")
            except: pass
        if len(funding_history)>=6:
            last_6=funding_history[-6:]
            if sig=="LONG" and all(r<0 for r in last_6) and funding_age_penalty==0:
                funding_hist_sc=12; reasons.append(f"⚡ Funding negative 6+ consecutive periods — deep squeeze setup")
            elif sig=="SHORT" and all(r>0 for r in last_6):
                funding_hist_sc=12; reasons.append(f"⚡ Funding positive 6+ consecutive periods — prolonged long squeeze")
            elif sig=="LONG" and sum(1 for r in last_6 if r<0)>=4 and funding_age_penalty==0:
                funding_hist_sc=6; reasons.append(f"⚡ Funding mostly negative last 6 periods")
            elif sig=="SHORT" and sum(1 for r in last_6 if r>0)>=4:
                funding_hist_sc=6; reasons.append(f"⚡ Funding mostly positive last 6 periods")
        pump_score+=funding_hist_sc-funding_age_penalty; bd['funding_hist']=funding_hist_sc; bd['funding_age_penalty']=-funding_age_penalty

        # 5 ── OPEN INTEREST ───────────────────────────────────────────────
        oi_sc=0; oi_history=await self.fetch_oi_history(exch_obj,sym); oi_change_6h=0.0
        if len(oi_history)>=6 and oi_history[-6]>0:
            oi_change_6h=(oi_history[-1]-oi_history[-6])/oi_history[-6]*100
        if oi>0:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:   oi_sc=s['pts_oi_high']; reasons.append(f"📈 OI + price up {pc20:.1f}% — confirmed accumulation")
            elif sig=="LONG" and pc20>=s['oi_price_chg_low']:  oi_sc=s['pts_oi_low'];  reasons.append(f"📈 OI growing, price +{pc20:.1f}%")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']: oi_sc=s['pts_oi_high']; reasons.append(f"📉 OI + price down {pc20:.1f}%")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_low']:  oi_sc=s['pts_oi_low'];  reasons.append(f"📉 OI building on drop {pc20:.1f}%")
            if abs(oi_change_6h)>=20:   oi_sc+=10; reasons.append(f"📈 OI spiked {oi_change_6h:+.1f}% in 6h — new money entering NOW")
            elif abs(oi_change_6h)>=10: oi_sc+=5;  reasons.append(f"📈 OI up {oi_change_6h:+.1f}% in 6h")
        else:
            if sig=="LONG" and pc20>=s['oi_price_chg_high']:   oi_sc=8; reasons.append(f"📈 Price up {pc20:.1f}% (OI unavailable)")
            elif sig=="SHORT" and pc20<=-s['oi_price_chg_high']: oi_sc=8; reasons.append(f"📉 Price down {pc20:.1f}% (OI unavailable)")
        pump_score+=oi_sc; bd['oi_spike']=oi_sc

        # 6 ── VOLUME SURGE ────────────────────────────────────────────────
        vsurge=lvol/vma; v_sc=0; explosive_thresh=s.get('vol_surge_explosive',5.0)
        if vsurge>=explosive_thresh:         v_sc=s.get('pts_vol_explosive',20); reasons.append(f"🚀 EXPLOSIVE volume {vsurge:.1f}×avg — institutional move NOW")
        elif vsurge>=s['vol_surge_high']:    v_sc=s['pts_vol_high']; reasons.append(f"🔥 Volume {vsurge:.1f}×avg — major activity NOW")
        elif vsurge>=s['vol_surge_mid']:     v_sc=s['pts_vol_mid'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — elevated")
        elif vsurge>=s['vol_surge_low']:     v_sc=s['pts_vol_low'];  reasons.append(f"📊 Volume {vsurge:.1f}×avg — above normal")
        pump_score+=v_sc; bd['vol_surge']=v_sc

        # 7 ── LIQUIDATION CLUSTERS ────────────────────────────────────────
        liq_sc=0; liq_target=0; liq_detail=""
        try:
            top3=df_f.nlargest(3,'volume')
            for _,row in top3.iterrows():
                mid=(float(row['high'])+float(row['low']))/2; dist=abs(price-mid)/price*100
                target=mid*1.02 if sig=="LONG" else mid*0.98
                if dist<=s['liq_cluster_near']:   liq_sc=s['pts_liq_near']; liq_target=target; liq_detail=f"${target:.6f}"; reasons.append(f"🧲 Liq cluster {dist:.1f}% away @ ${mid:.5f}"); break
                elif dist<=s['liq_cluster_mid'] and liq_sc<s['pts_liq_mid']:  liq_sc=s['pts_liq_mid']
                elif dist<=s['liq_cluster_far'] and liq_sc<s['pts_liq_far']:  liq_sc=s['pts_liq_far']
        except: pass
        pump_score+=liq_sc; bd['liq_cluster']=liq_sc

        # 8 ── 24H VOLUME ──────────────────────────────────────────────────
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

        # 9 ── TECHNICALS ──────────────────────────────────────────────────
        tech_sc=0
        if sig=="LONG" and macd>macds:     tech_sc+=s['pts_macd']; reasons.append("📊 MACD bullish cross")
        elif sig=="SHORT" and macd<macds:  tech_sc+=s['pts_macd']; reasons.append("📊 MACD bearish cross")
        if sig=="LONG" and rsi<s['rsi_oversold']:      tech_sc+=s['pts_rsi']; reasons.append(f"📉 RSI oversold {rsi:.1f}")
        elif sig=="SHORT" and rsi>s['rsi_overbought']: tech_sc+=s['pts_rsi']; reasons.append(f"📈 RSI overbought {rsi:.1f}")
        if sig=="LONG" and float(df_f['close'].iloc[-1])<=bbl:    tech_sc+=s['pts_bb']; reasons.append("🎯 Price at lower BB")
        elif sig=="SHORT" and float(df_f['close'].iloc[-1])>=bbu: tech_sc+=s['pts_bb']; reasons.append("🎯 Price at upper BB")
        tech_sc+=s['pts_ema']; reasons.append(f"✅ EMA50 trend: {sig}")
        # RSI Divergence
        try:
            cl=df_f['close']; ra=rsi_series_raw
            if len(ra)>=10 and len(cl)>=10:
                r_arr=ra[-10:]; c_arr=cl.values[-10:]
                plnow=min(c_arr[-3:]); plprev=min(c_arr[:5]); rlnow=min(r_arr[-3:]); rlprev=min(r_arr[:5])
                phnow=max(c_arr[-3:]); phprev=max(c_arr[:5]); rhnow=max(r_arr[-3:]); rhprev=max(r_arr[:5])
                dp=s.get('pts_divergence',10)
                if sig=="LONG":
                    if plnow<plprev and rlnow>rlprev: tech_sc+=dp; reasons.append(f"📐 Bullish RSI divergence — price lower low, RSI higher low")
                    elif plnow>plprev and rlnow<rlprev: tech_sc+=dp//2; reasons.append(f"📐 Hidden bullish divergence")
                elif sig=="SHORT":
                    if phnow>phprev and rhnow<rhprev: tech_sc+=dp; reasons.append(f"📐 Bearish RSI divergence — price higher high, RSI lower high")
                    elif phnow<phprev and rhnow>rhprev: tech_sc+=dp//2; reasons.append(f"📐 Hidden bearish divergence")
        except: pass
        # Candle Patterns
        try:
            pp=s.get('pts_candle_pattern',8)
            c0=df_f.iloc[-1]; c1=df_f.iloc[-2]; c2=df_f.iloc[-3]
            b0=float(c0['close'])-float(c0['open']); b1=float(c1['close'])-float(c1['open'])
            r0=float(c0['high'])-float(c0['low']); r1=float(c1['high'])-float(c1['low'])
            if r0>0 and r1>0:
                if sig=="LONG":
                    lw=min(float(c0['open']),float(c0['close']))-float(c0['low'])
                    if lw/r0>0.6 and abs(b0)/r0<0.3: tech_sc+=pp; reasons.append("🔨 Hammer candle — buyers defended strongly")
                    elif b0>0 and b1<0 and abs(b0)>abs(b1)*1.1: tech_sc+=pp; reasons.append("🕯️ Bullish engulfing")
                    elif all(float(df_f.iloc[-k]['close'])>float(df_f.iloc[-k]['open']) for k in [1,2,3]) and float(df_f['volume'].iloc[-1])>float(df_f['volume'].iloc[-2])>float(df_f['volume'].iloc[-3]):
                        tech_sc+=pp; reasons.append("📈 3 consecutive green candles with rising volume")
                else:
                    uw=float(c0['high'])-max(float(c0['open']),float(c0['close']))
                    if uw/r0>0.6 and abs(b0)/r0<0.3: tech_sc+=pp; reasons.append("🌠 Shooting star — sellers rejected rally")
                    elif b0<0 and b1>0 and abs(b0)>abs(b1)*1.1: tech_sc+=pp; reasons.append("🕯️ Bearish engulfing")
                    elif all(float(df_f.iloc[-k]['close'])<float(df_f.iloc[-k]['open']) for k in [1,2,3]) and float(df_f['volume'].iloc[-1])>float(df_f['volume'].iloc[-2])>float(df_f['volume'].iloc[-3]):
                        tech_sc+=pp; reasons.append("📉 3 consecutive red candles with rising volume")
        except: pass
        pump_score+=tech_sc; bd['technicals']=tech_sc

        # 10 ── SESSION ────────────────────────────────────────────────────
        sname,smult,_=get_session(); ses_sc=0
        if smult>=1.4: ses_sc=s['pts_session']; reasons.append(f"⏰ {sname} — peak session")
        elif smult>=1.3: ses_sc=max(1,s['pts_session']//2); reasons.append(f"⏰ {sname}")
        pump_score+=ses_sc; bd['session']=ses_sc

        # 11 ── MOMENTUM ───────────────────────────────────────────────────
        candle_body=float(df_f['close'].iloc[-1])-float(df_f['open'].iloc[-1])
        recent_momentum=float(df_f['close'].iloc[-1])-float(df_f['close'].iloc[-3])
        momentum_confirmed=False; mom_sc=0
        if sig=="LONG" and candle_body>0 and recent_momentum>0:
            momentum_confirmed=True; mom_sc=8; reasons.append(f"✅ Momentum confirmed: price UP last 3 candles ({recent_momentum/price*100:+.2f}%)")
        elif sig=="SHORT" and candle_body<0 and recent_momentum<0:
            momentum_confirmed=True; mom_sc=8; reasons.append(f"✅ Momentum confirmed: price DOWN last 3 candles ({recent_momentum/price*100:+.2f}%)")
        pump_score+=mom_sc; bd['momentum']=mom_sc
        if s.get('require_momentum',False) and not momentum_confirmed: return None

        # 11b ── MTF CONFIRMATION ──────────────────────────────────────────
        mtf_sc=0
        if s.get('mtf_confirm',True):
            try:
                df_med=await self.fetch_ohlcv(exch_obj,sym,"1h",60)
                if not df_med.empty:
                    df_med.ta.ema(length=50,append=True)
                    ec=[c for c in df_med.columns if 'EMA_50' in c]
                    if ec:
                        e50m=float(df_med[ec[0]].iloc[-1]); cm=float(df_med['close'].iloc[-1])
                        mt="LONG" if cm>e50m else "SHORT"; ft="LONG" if candle_body>0 else "SHORT"
                        aligned=sum(1 for t in [sig,mt,ft] if t==sig)
                        if aligned==3:   mtf_sc=s.get('pts_mtf',12);      reasons.append(f"🎯 All 3 TFs aligned {sig} — {s['fast_tf']}+1h+{s['slow_tf']}")
                        elif aligned==2: mtf_sc=s.get('pts_mtf',12)//2;  reasons.append(f"🎯 2/3 TFs aligned {sig}")
                        else:            mtf_sc=-5;                        reasons.append(f"⚠️ TF conflict — {s['fast_tf']} vs 1h vs {s['slow_tf']}")
            except: pass
        pump_score+=mtf_sc; bd['mtf']=mtf_sc

        # 12 ── SENTIMENT ──────────────────────────────────────────────────
        sentiment={'top_long_pct':50,'top_short_pct':50,'retail_long_pct':50,'taker_buy_pct':50,'available':False,'source':''}
        sent_sc=0
        if exch_name in ("GATE","OKX"):
            sb=dsym+("-USDT-SWAP" if exch_name=="OKX" else "_USDT")
            sentiment=self.fetch_sentiment_data(sb,exch_name)
            if sentiment['available']:
                pts=s.get('pts_sentiment',20); pt=s.get('pts_taker',10)
                if sig=="LONG":
                    if sentiment['top_short_pct']>65 and sentiment['retail_long_pct']>60: sent_sc=pts; reasons.append(f"🧠 Smart money {sentiment['top_short_pct']:.0f}% SHORT vs retail {sentiment['retail_long_pct']:.0f}% LONG — squeeze fuel")
                    elif sentiment['top_short_pct']>55: sent_sc=pts//2; reasons.append(f"🧠 Top traders {sentiment['top_short_pct']:.0f}% short")
                    if sentiment['taker_buy_pct']>62:   sent_sc+=pt;    reasons.append(f"💚 Taker buy {sentiment['taker_buy_pct']:.0f}%")
                    elif sentiment['taker_buy_pct']>55: sent_sc+=pt//2; reasons.append(f"💚 Taker buy slightly dominant {sentiment['taker_buy_pct']:.0f}%")
                else:
                    if sentiment['top_long_pct']>65 and sentiment['retail_long_pct']>65: sent_sc=pts; reasons.append(f"🧠 Smart money {sentiment['top_long_pct']:.0f}% LONG + retail crowded — distribution")
                    elif sentiment['top_long_pct']>55: sent_sc=pts//2; reasons.append(f"🧠 Top traders {sentiment['top_long_pct']:.0f}% long")
                    if sentiment['taker_buy_pct']<38:   sent_sc+=pt;    reasons.append(f"🔴 Taker sell {100-sentiment['taker_buy_pct']:.0f}%")
                    elif sentiment['taker_buy_pct']<45: sent_sc+=pt//2; reasons.append(f"🔴 Taker sell slightly dominant")
        pump_score+=sent_sc; bd['sentiment']=sent_sc

        # 12b ── OI+FUNDING COMBO ──────────────────────────────────────────
        combo_sc=0
        if bd.get('oi_spike',0)>=10 and (bd.get('funding',0)>=s.get('pts_funding_high',25) or bd.get('funding_hist',0)>=12):
            combo_sc=s.get('pts_oi_funding_combo',10); reasons.append("💥 OI surge + extreme funding combo — maximum squeeze pressure")
        pump_score+=combo_sc; bd['oi_funding_combo']=combo_sc

        # 13 ── SOCIAL BUZZ ────────────────────────────────────────────────
        social_data=self.fetch_reddit_buzz(dsym,s); social_sc=0
        if social_data.get('available') and social_data['mentions']>=s.get('social_min_mentions',3):
            social_sc=social_data['score']
            em="🚀" if social_data.get('sentiment')=='BULLISH' else ("🩸" if social_data.get('sentiment')=='BEARISH' else "💬")
            reasons.append(f"{em} {social_data.get('source','Reddit')}: {social_data['mentions']} mentions/hr | {social_data.get('sentiment','?')}")
            if social_data.get('top_post'): reasons.append(f"📢 Top post: \"{social_data['top_post'][:55]}...\"")
        pump_score+=social_sc; bd['social_buzz']=social_sc

        # 14 ── ORDER FLOW ─────────────────────────────────────────────────
        of_pct,of_dir=await self.fetch_orderflow_imbalance(exch_obj,sym,s.get('orderflow_lookback',10))
        of_sc=0; pts_of=s.get('pts_orderflow',12); sell_pct=100-of_pct
        if sig=='LONG':
            if of_dir=='BUY' and of_pct>=65:   of_sc=pts_of;   reasons.append(f"📊 Order flow {of_pct:.0f}% BUY — sustained accumulation")
            elif of_dir=='BUY' and of_pct>=58: of_sc=pts_of//2; reasons.append(f"📊 Order flow mildly bullish ({of_pct:.0f}% buy)")
        elif sig=='SHORT':
            if of_dir=='SELL' and sell_pct>=65:   of_sc=pts_of;   reasons.append(f"📊 Order flow {sell_pct:.0f}% SELL — sustained distribution")
            elif of_dir=='SELL' and sell_pct>=58: of_sc=pts_of//2; reasons.append(f"📊 Order flow mildly bearish ({sell_pct:.0f}% sell)")
        pump_score+=of_sc; bd['orderflow']=of_sc

        # 15 ── LIQUIDATION MAP ────────────────────────────────────────────
        liq_map=await self.fetch_liquidation_map(exch_obj,sym,price); liq_map_sc=0; pts_lm=s.get('pts_liq_map',15)
        if liq_map:
            nearest=liq_map[0]; d=nearest['dist_pct']; sz_m=nearest['size_usd']/1e6; slbl=nearest['side']
            em_lm="💥" if ((slbl=='SHORT_LIQ' and sig=='LONG') or (slbl=='LONG_LIQ' and sig=='SHORT')) else "🧲"
            if d<=1.5:   liq_map_sc=pts_lm;    reasons.append(f"{em_lm} Liq cluster {slbl} ${sz_m:.1f}M @ ${nearest['price']:.4f} ({d:.2f}% away)")
            elif d<=3.0: liq_map_sc=pts_lm//2; reasons.append(f"🧲 Liq cluster {slbl} ${sz_m:.1f}M at {d:.1f}%")
        pump_score+=liq_map_sc; bd['liq_map']=liq_map_sc

        # 16 ── LISTING DETECTOR ───────────────────────────────────────────
        new_listings=self.fetch_new_listings(s); listing_sc=0; listing_info={}; pts_lst=s.get('listing_alert_pts',25)
        for lst in new_listings:
            if lst['symbol'].upper()==dsym.upper():
                h=lst.get('listed_ago_h',999); listing_info=lst
                if h<=24:   listing_sc=pts_lst;     reasons.append(f"🆕 BRAND NEW LISTING on {lst['exchange']} {h:.0f}h ago")
                elif h<=72: listing_sc=pts_lst//2;  reasons.append(f"🆕 Recent listing {h:.0f}h ago")
                elif h<=168:listing_sc=pts_lst//4;  reasons.append(f"🆕 Listed this week ({h:.0f}h ago)")
                break
        pump_score+=listing_sc; bd['listing']=listing_sc

        # 17 ── ON-CHAIN WHALE ─────────────────────────────────────────────
        onchain=self.fetch_onchain_whale(dsym,s); onchain_sc=0; pts_oc=s.get('pts_onchain_whale',15)
        if onchain.get('available'):
            if onchain['signal']=='BULLISH' and sig=='LONG':   onchain_sc=pts_oc;       reasons.append(f"🐋 On-chain: {onchain['detail']} — leaving exchanges")
            elif onchain['signal']=='BEARISH' and sig=='SHORT': onchain_sc=pts_oc;       reasons.append(f"🐋 On-chain: {onchain['detail']} — entering exchanges")
            elif onchain['signal']=='BULLISH' and sig=='SHORT': onchain_sc=-(pts_oc//2); reasons.append(f"⚠️ On-chain bullish conflicts with SHORT")
            elif onchain['signal']=='BEARISH' and sig=='LONG':  onchain_sc=-(pts_oc//2); reasons.append(f"⚠️ On-chain bearish conflicts with LONG")
        pump_score+=onchain_sc; bd['onchain']=onchain_sc
        # 18 ── WYCKOFF SPRING DETECTOR ──────────────────────────────────────
        wyckoff_sc = 0
        try:
            closes = df_f['close'].values
            highs = df_f['high'].values
            lows = df_f['low'].values
            volumes = df_f['volume'].values
            if len(closes) >= 20:
                # Find range over last 20 candles
                range_hi = float(max(highs[-20:-2]))
                range_lo = float(min(lows[-20:-2]))
                range_size = range_hi - range_lo
                last_low = float(lows[-1])
                last_close = float(closes[-1])
                last_vol = float(volumes[-1])
                avg_vol = float(df_f['volume'].rolling(20).mean().iloc[-1])
                # Spring conditions:
                # 1. Price was ranging (range exists)
                # 2. Last candle wicked BELOW range low (stop hunt)
                # 3. Last candle CLOSED BACK ABOVE range low (immediate recovery)
                # 4. Volume spike on the wick candle
                if range_size > 0:
                    wick_below = last_low < range_lo
                    recovered = last_close > range_lo
                    vol_spike = last_vol > avg_vol * 1.5
                    range_contract = range_size / price < 0.08  # price was in tight range
                    if wick_below and recovered and vol_spike and sig == 'LONG':
                        wyckoff_sc = 30
                        reasons.append(f"🎯 WYCKOFF SPRING: Wick swept ${range_lo:.6f} support then recovered — stop hunt complete, reversal imminent")
                    elif wick_below and recovered and sig == 'LONG':
                        wyckoff_sc = 15
                        reasons.append(f"🎯 Wyckoff spring pattern — wick below support recovered")
        except: pass
        pump_score += wyckoff_sc; bd['wyckoff_spring'] = wyckoff_sc

        # 19 ── CVD DIVERGENCE ────────────────────────────────────────────────
        cvd_sc = 0
        try:
            if len(df_f) >= 15:
                # CVD = cumulative sum of (close > open ? volume : -volume)
                df_cvd = df_f.copy()
                df_cvd['delta'] = df_cvd.apply(
                    lambda r: float(r['volume']) if float(r['close']) > float(r['open'])
                    else -float(r['volume']), axis=1)
                df_cvd['cvd'] = df_cvd['delta'].cumsum()
                cvd_vals = df_cvd['cvd'].values
                price_vals = df_cvd['close'].values
                # Last 10 candles
                cvd_now = float(cvd_vals[-3:].mean())
                cvd_prev = float(cvd_vals[-13:-10].mean())
                price_now = float(price_vals[-3:].mean())
                price_prev = float(price_vals[-13:-10].mean())
                price_up = price_now > price_prev
                price_down = price_now < price_prev
                cvd_up = cvd_now > cvd_prev
                cvd_down = cvd_now < cvd_prev
                if sig == 'LONG' and price_down and cvd_up:
                    # Price falling but buying pressure increasing = accumulation
                    cvd_sc = 20
                    reasons.append(f"📊 CVD DIVERGENCE: Price dropping but buy volume accumulating — market maker absorbing sells")
                elif sig == 'SHORT' and price_up and cvd_down:
                    # Price rising but selling pressure increasing = distribution
                    cvd_sc = 20
                    reasons.append(f"📊 CVD DIVERGENCE: Price rising but sell volume dominating — market maker distributing into pumps")
                elif sig == 'LONG' and price_down and cvd_up == False and cvd_down == False:
                    cvd_sc = 8
                    reasons.append(f"📊 CVD neutral on price drop — no panic selling")
        except: pass
        pump_score += cvd_sc; bd['cvd_divergence'] = cvd_sc

        # 20 ── STOP HUNT ZONE DETECTOR ──────────────────────────────────────
        stophunt_sc = 0
        try:
            if len(df_f) >= 10:
                closes = df_f['close'].values
                lows = df_f['low'].values
                highs = df_f['high'].values
                current_price = price
                # Round number proximity (stop hunts cluster at round numbers)
                magnitude = 10 ** (len(str(int(price))) - 1)
                nearest_round = round(price / magnitude) * magnitude
                dist_round = abs(price - nearest_round) / price * 100
                # Swing low/high from last 10 candles
                swing_lo_10 = float(min(lows[-10:-1]))
                swing_hi_10 = float(max(highs[-10:-1]))
                dist_swing_lo = abs(price - swing_lo_10) / price * 100
                dist_swing_hi = abs(price - swing_hi_10) / price * 100
                if sig == 'LONG':
                    # Price near swing low = stop hunt zone for longs
                    near_swing_lo = dist_swing_lo <= 1.5
                    near_round_dn = dist_round <= 1.0 and price < nearest_round
                    # Funding negative here = extra confirmation
                    funding_neg = fr < -0.0001
                    if near_swing_lo and funding_neg:
                        stophunt_sc = 25
                        reasons.append(f"🎯 STOP HUNT ZONE: Price at swing low ${swing_lo_10:.6f} + negative funding — market maker trap, reversal likely")
                    elif near_swing_lo:
                        stophunt_sc = 12
                        reasons.append(f"🎯 Stop hunt zone: Price at key swing low ${swing_lo_10:.6f} — stops likely below here")
                    elif near_round_dn and funding_neg:
                        stophunt_sc = 15
                        reasons.append(f"🎯 Stop hunt at round number ${nearest_round:.4f} + negative funding — classic MM trap")
                elif sig == 'SHORT':
                    near_swing_hi = dist_swing_hi <= 1.5
                    near_round_up = dist_round <= 1.0 and price > nearest_round
                    funding_pos = fr > 0.0001
                    if near_swing_hi and funding_pos:
                        stophunt_sc = 25
                        reasons.append(f"🎯 STOP HUNT ZONE: Price at swing high ${swing_hi_10:.6f} + positive funding — distribution trap")
                    elif near_swing_hi:
                        stophunt_sc = 12
                        reasons.append(f"🎯 Stop hunt zone: Price at swing high ${swing_hi_10:.6f}")
                    elif near_round_up and funding_pos:
                        stophunt_sc = 15
                        reasons.append(f"🎯 Stop hunt at round number ${nearest_round:.4f} + positive funding")
        except: pass
        pump_score += stophunt_sc; bd['stop_hunt'] = stophunt_sc
        pump_score=max(0,min(pump_score,100))

        # ── ACCURACY GATES ────────────────────────────────────────────────
        active_cats=sum(1 for k,v in bd.items() if v>0 and k not in ('session','mtf'))
        if active_cats<s.get('min_active_signals',3): return None
        if len(reasons)<s.get('min_reasons',1): return None
        fng=st.session_state.get('fng_val',50)
        if sig=="LONG" and fng<s.get('fng_long_threshold',30) and pump_score<60: return None
        if sig=="SHORT" and fng>s.get('fng_short_threshold',70) and pump_score<60: return None

        # ── SL: 4H OB + FVG ──────────────────────────────────────────────
        bull_obs_4h,bear_obs_4h,bull_fvgs,bear_fvgs=[],[],[],[]
        if s.get('use_4h_ob_for_sl',True):
            bull_obs_4h,bear_obs_4h,bull_fvgs,bear_fvgs=await self.fetch_4h_ob_and_fvg(exch_obj,sym,price)

        try:
            rh=df_f['high'].rolling(5).max().dropna(); rl=df_f['low'].rolling(5).min().dropna()
            swing_high=float(rh.iloc[-2]) if len(rh)>=2 else price*1.03
            swing_low=float(rl.iloc[-2]) if len(rl)>=2 else price*0.97
            if sig=="LONG":
                structure_sl=swing_low*0.995
                whale_bid_px=whale_details[0]['price'] if whale_details and whale_details[0]['side']=='BUY' else 0
                whale_sl=whale_bid_px*0.995 if 0<whale_bid_px<price else structure_sl
                # Best SL from 4H OB or FVG
                ob_4h_sl=structure_sl; ob_4h_used=False; fvg_sl=structure_sl; fvg_used=False
                if bull_obs_4h:
                    c=bull_obs_4h[0]; cand=c['zone_lo']*0.995
                    if cand<price and cand>structure_sl*0.98:
                        ob_4h_sl=cand; ob_4h_used=True
                        reasons.append(f"🧱 4H OB support ${c['zone_lo']:.5f}–${c['zone_hi']:.5f} — SL anchored below")
                if bull_fvgs:
                    f=bull_fvgs[0]; cand=f['zone_lo']*0.995
                    if cand<price and cand>structure_sl*0.97:
                        fvg_sl=cand; fvg_used=True
                        reasons.append(f"🔷 4H FVG support ${f['zone_lo']:.5f}–${f['zone_hi']:.5f} — SL anchored in gap")
                sl_cands=[x for x in [structure_sl,whale_sl,ob_4h_sl,fvg_sl] if x<price]
                sl=max(sl_cands) if sl_cands else price-atr*1.5
                if sl>=price: sl=price-atr*1.5
                sl_dist=price-sl; min_rr_tp=price+sl_dist*max(s.get('min_rr',1.5),1.5)
                tp=max(swing_high,min_rr_tp)
                if liq_target>price: tp=min(tp,liq_target) if liq_target<tp else tp
            else:
                structure_sl=swing_high*1.005
                whale_ask_px=whale_details[0]['price'] if whale_details and whale_details[0]['side']=='SELL' else 0
                whale_sl=whale_ask_px*1.005 if whale_ask_px>price else structure_sl
                ob_4h_sl=structure_sl
                if bear_obs_4h:
                    c=bear_obs_4h[0]; cand=c['zone_hi']*1.005
                    if cand>price and cand<structure_sl*1.02:
                        ob_4h_sl=cand
                        reasons.append(f"🧱 4H OB resistance ${c['zone_lo']:.5f}–${c['zone_hi']:.5f} — SL above it")
                fvg_sl=structure_sl
                if bear_fvgs:
                    f=bear_fvgs[0]; cand=f['zone_hi']*1.005
                    if cand>price and cand<structure_sl*1.02:
                        fvg_sl=cand
                        reasons.append(f"🔷 4H FVG resistance ${f['zone_lo']:.5f}–${f['zone_hi']:.5f}")
                sl_cands=[x for x in [structure_sl,whale_sl,ob_4h_sl,fvg_sl] if x>price]
                sl=min(sl_cands) if sl_cands else price+atr*1.5
                if sl<=price: sl=price+atr*1.5
                sl_dist=sl-price; min_rr_tp=price-sl_dist*max(s.get('min_rr',1.5),1.5)
                tp=min(swing_low,min_rr_tp)
                if liq_target>0 and liq_target<price: tp=max(tp,liq_target)
        except:
            tp=price+atr*3 if sig=="LONG" else price-atr*3
            sl=price-atr*1.5 if sig=="LONG" else price+atr*1.5

        try:
            sl_dist=abs(price-sl) if abs(price-sl)>0 else atr
            if sig=="LONG":
                tp1=price+sl_dist*1.5; tp2=tp; tp3=max(price+sl_dist*4.0,liq_target) if liq_target>swing_high else price+sl_dist*4.0
                tp3=max(tp3,price+sl_dist*3.0)
            else:
                tp1=price-sl_dist*1.5; tp2=tp; tp3=min(price-sl_dist*4.0,liq_target) if liq_target>0 and liq_target<swing_low else price-sl_dist*4.0
                tp3=min(tp3,price-sl_dist*3.0)
        except:
            tp1=price+atr*1.5 if sig=="LONG" else price-atr*1.5
            tp2=price+atr*3   if sig=="LONG" else price-atr*3
            tp3=price+atr*5   if sig=="LONG" else price-atr*5

        try:
            rr=abs(tp-price)/abs(price-sl)
            if rr<s.get('min_rr',1.5): return None
        except: pass

        result={
            'symbol':dsym,'exchange':exch_name,'price':price,'pump_score':pump_score,
            'tp1':tp1,'tp2':tp2,'tp3':tp3,'type':sig,'reasons':reasons,'tp':tp,'sl':sl,
            'rsi':rsi,'funding':fr,'atr':atr,'ob':ob_data,'cmc':cmc,'oi':oi,
            'oi_change_6h':oi_change_6h,'liq_target':liq_target,'liq_detail':liq_detail,
            'whale_str':whale_str,'whale_details':whale_details,'vol_mcap':vol_mcap_ratio,
            'signal_breakdown':bd,'session':sname,'price_chg_20':pc20,
            'timestamp':datetime.now().strftime('%H:%M:%S'),
            'quote_vol':float(tick.get('quoteVolume',0) or 0),'sentiment':sentiment,
            'momentum_confirmed':momentum_confirmed,
            'funding_history':funding_history[-8:] if funding_history else [],
            'social_data':social_data,'atr_pct':round(atr_pct,2),
            'pct_24h':float(tick.get('percentage') or tick.get('change') or 0),
            'vol_surge_ratio':round(vsurge,2),'liq_map_data':liq_map[:3] if liq_map else [],
            'listing_data':listing_info,'onchain_data':onchain,
            'orderflow_data':{'pct':round(of_pct,1),'dir':of_dir},
            'entry_lo':round(price-atr*0.35,8) if sig=='LONG' else round(price+atr*0.1,8),
            'entry_hi':round(price+atr*0.1,8)  if sig=='LONG' else round(price+atr*0.35,8),
            'warnings':warnings,'late_entry_flag':late_entry_flag,
            'vol_exhaust_flag':vol_exhaust_flag,'near_top_flag':near_top_flag,
            'rsi_direction':rsi_direction,
            'bull_obs_4h':bull_obs_4h[:2] if bull_obs_4h else [],
            'bear_obs_4h':bear_obs_4h[:2] if bear_obs_4h else [],
            'bull_fvgs':bull_fvgs[:2] if bull_fvgs else [],
            'bear_fvgs':bear_fvgs[:2] if bear_fvgs else [],
        }
        result['_cls_cfg']={
            'breakout_oi_min':s.get('cls_breakout_oi_min',7),'breakout_vol_min':s.get('cls_breakout_vol_min',4),
            'breakout_sc_min':s.get('cls_breakout_score_min',25),'squeeze_fund_min':s.get('cls_squeeze_fund_min',8),
            'squeeze_ob_min':s.get('cls_squeeze_ob_min',6),
        }
        # ── ATR Expansion Filter ──────────────────────────────────────────
        if s.get('atr_expansion_filter', False):
            try:
                atr_mult = float(s.get('atr_expansion_mult', 1.5))
                atr_series = _dst_atr(df_f['high'], df_f['low'], df_f['close'], 14)
                atr_now = float(atr_series.iloc[-1])
                atr_24h_avg = float(atr_series.iloc[-288:].mean()) if len(atr_series) >= 288 else float(atr_series.mean())
                if atr_24h_avg > 0 and atr_now < atr_mult * atr_24h_avg:
                    result['pump_score'] = 0
                    result['warnings'].append(f"⚡ ATR expansion filter: current ATR {atr_now:.6f} < {atr_mult}× 24h avg {atr_24h_avg:.6f} — sideways market")
            except:
                pass

        # ── Relative Strength vs BTC Filter ──────────────────────────────
        if s.get('rs_btc_filter', False) and result.get('type') == 'LONG':
            try:
                rs_min = float(s.get('rs_btc_min', 0.0))
                btc_price = st.session_state.get('btc_price', 0)
                coin_close = float(df_f['close'].iloc[-1])
                coin_open_1h = float(df_f['close'].iloc[-12]) if len(df_f) >= 12 else coin_close
                coin_chg_1h = (coin_close - coin_open_1h) / coin_open_1h * 100 if coin_open_1h > 0 else 0
                btc_chg_1h = st.session_state.get('btc_1h_chg', 0)
                rs_vs_btc = coin_chg_1h - btc_chg_1h
                result['signal_breakdown']['rs_btc'] = round(rs_vs_btc, 2)
                if rs_vs_btc < rs_min:
                    result['pump_score'] = max(0, result['pump_score'] - 15)
                    result['warnings'].append(f"📉 RS vs BTC: coin {coin_chg_1h:+.1f}% vs BTC {btc_chg_1h:+.1f}% = RS {rs_vs_btc:+.1f}% (min {rs_min:+.1f}%)")
            except:
                pass
        result['cls'] = classify(result)

        # ── DEMA + SuperTrend cross-check ─────────────────────────────────
        dst = check_dst_signal(df_f, dsym, s)
        result['dst_signal'] = dst

        if dst and dst['direction'] == sig:
            boost = int(s.get('dst_confirm_boost', 8))
            result['dst_confirmed'] = True
            result['pump_score'] = min(100, result['pump_score'] + boost)
            result['reasons'].insert(0,
                f"🔵 DEMA+ST CONFIRMED: Both APEX5 + DEMA/SuperTrend agree {sig} "
                f"| DEMA dist {abs(dst['close']-dst['dema'])/dst['close']*100:.2f}% "
                f"| ST flipped {dst['fresh_bars']} bars ago | R:R {dst['rr']}")
            result['signal_breakdown']['dst_boost'] = boost
        elif dst and dst['direction'] != sig:
            penalty = int(s.get('dst_conflict_penalty', 5))
            result['dst_confirmed'] = False
            result['pump_score'] = max(0, result['pump_score'] - penalty)
            result['warnings'].append(
                f"⚠️ DEMA+ST conflict: APEX5 says {sig} but DEMA/SuperTrend says {dst['direction']}")
            result['signal_breakdown']['dst_boost'] = -penalty
        else:
            result['dst_confirmed'] = False
            result['signal_breakdown']['dst_boost'] = 0

        if s['cooldown_on']: set_cooldown(dsym)
        return result

    async def run(self, s):
        btc_trend,btc_px,btc_rsi=await self.fetch_btc()
        await asyncio.gather(self.okx.load_markets(),self.mexc.load_markets(),self.gate.load_markets())
        tk_okx,tk_mexc,tk_gate={},{},{}
        try: tk_okx=await self.okx.fetch_tickers()
        except: pass
        try: tk_mexc=await self.mexc.fetch_tickers()
        except: pass
        try: tk_gate=await self.gate.fetch_tickers()
        except: pass
        process_journal_tracking({**tk_okx,**tk_mexc,**tk_gate},s)
        swaps_dict={}
        for sym,t in tk_mexc.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                swaps_dict[sym]={'vol':float(t['quoteVolume'] or 0),'pct':float(t.get('percentage') or t.get('change') or 0),'exch_name':'MEXC','exch_obj':self.mexc}
        for sym,t in tk_gate.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                swaps_dict[sym]={'vol':float(t['quoteVolume'] or 0),'pct':float(t.get('percentage') or t.get('change') or 0),'exch_name':'GATE','exch_obj':self.gate}
        for sym,t in tk_okx.items():
            if sym.endswith(':USDT') and t.get('quoteVolume'):
                swaps_dict[sym]={'vol':float(t['quoteVolume'] or 0),'pct':float(t.get('percentage') or t.get('change') or 0),'exch_name':'OKX','exch_obj':self.okx}
        # ── Crypto-only filter — remove commodities and non-crypto ────
        _COMMODITY_PATTERNS = [
            'XAU','XAG','GOLD','SILVER','UKOIL','USOIL','BRENT','WTI',
            'NATGAS','COPPER','PLATINUM','PALLADIUM','WHEAT','CORN','SOYBEAN'
        ]
        _blacklist = [x.strip().upper() for x in s.get('symbol_blacklist','').split(',') if x.strip()]
        _all_excluded = _COMMODITY_PATTERNS + _blacklist
        def _is_crypto(sym):
            base = sym.split('/')[0].upper()
            return not any(base == ex or base.startswith(ex) for ex in _all_excluded)
        swaps_dict = {sym: v for sym, v in swaps_dict.items() if _is_crypto(sym)}

        scan_modes=s.get('scan_modes',['mixed'])
        if isinstance(scan_modes,str): scan_modes=[scan_modes]
        depth=s['scan_depth']
        def get_mode_symbols(mode,d,n):
            if mode=='gainers': return sorted(d.items(),key=lambda x:x[1]['pct'],reverse=True)[:n]
            elif mode=='losers': return sorted(d.items(),key=lambda x:x[1]['pct'])[:n]
            elif mode=='mixed':
                bg=sorted(d.items(),key=lambda x:x[1]['pct'],reverse=True)
                bl=sorted(d.items(),key=lambda x:x[1]['pct'])
                half=n//2; seen=set(); mixed=[]
                for item in bg[:half]+bl[:half]:
                    if item[0] not in seen: mixed.append(item); seen.add(item[0])
                return mixed
            else: return sorted(d.items(),key=lambda x:x[1]['vol'],reverse=True)[:n]
        combined={}; per_mode=max(depth//max(1,len(scan_modes)),10)
        for mode in scan_modes:
            for sym,data in get_mode_symbols(mode,swaps_dict,per_mode):
                if sym not in combined: combined[sym]=data
        symbols=list(combined.items())[:depth]
        results=[]; errors=[]; pb=st.progress(0); st_=st.empty()
        for i,(sym,data) in enumerate(symbols):
            exch_name=data['exch_name']; exch_obj=data['exch_obj']
            st_.markdown(f"<span style='font-family:monospace;font-size:.68rem;color:#7a82a0;'>Scanning {i+1}/{len(symbols)} — <b>{sym.split(':')[0]}</b> ({exch_name}) — found: <b style='color:#0f1117'>{len(results)}</b></span>",unsafe_allow_html=True)
            try:
                r=await self.analyze(exch_name,exch_obj,sym,s,btc_trend)
                if r: results.append(r)
            except Exception as e: errors.append(f"{sym} ({exch_name}): {str(e)[:80]}")
            pb.progress((i+1)/len(symbols)); await asyncio.sleep(0.5)
        st_.empty(); pb.empty()
        try:
            await self.okx.close(); await self.mexc.close(); await self.gate.close()
        except: pass
        results.sort(key=lambda x:x['pump_score'],reverse=True)
        if s.get('dedup_symbols',True):
            seen={}
            for r in results:
                sb=r['symbol']
                if sb not in seen or r['pump_score']>seen[sb]['pump_score']: seen[sb]=r
            results=sorted(seen.values(),key=lambda x:x['pump_score'],reverse=True)
        return results,btc_trend,btc_px,btc_rsi,errors
# ─── CARD RENDERER ────────────────────────────────────────────────────────────
def render_card(res, is_sniper=False, dual_confirmed=False):
    sc=res['pump_score']; col=pump_color(sc,is_sniper); lbl=pump_label(sc,res['type'],is_sniper)
    sig=res['type']; bd=res.get('signal_breakdown',{}); ob_data=res.get('ob',{})
    cmc=res.get('cmc') or {}; card_cls="pc-long" if sig=="LONG" else "pc-short"
    sig_col="var(--green)" if sig=="LONG" else "var(--red)"
    sentiment=res.get('sentiment',{})
    if not isinstance(sentiment,dict): sentiment={}
    momentum_confirmed=res.get('momentum_confirmed',False)
    warnings_list=res.get('warnings',[])
    exch=res.get('exchange','MEXC')
    exch_colors={"OKX":"#00bcd4","GATE":"#e040fb","MEXC":"#2563eb"}
    exch_col=exch_colors.get(exch,"#2563eb")
    if exch=="OKX": trade_link=f"https://www.okx.com/trade-swap/{res['symbol'].lower()}-usdt-swap"
    elif exch=="GATE": trade_link=f"https://www.gate.io/futures_trade/USDT/{res['symbol']}_USDT"
    else: trade_link=f"https://www.mexc.com/exchange/{res['symbol']}_USDT"

    def pip(v,lo=5,hi=12):
        if v>=hi: return "<span class='pip pip-on'></span>"
        if v>=lo: return "<span class='pip pip-half'></span>"
        return "<span class='pip pip-off'></span>"

    bid_pct=ob_data.get('bid_pct',50)
    chg=cmc.get('change24',0)
    cmc_html=""
    if cmc:
        chg_c="var(--green)" if chg>=0 else "var(--red)"
        cmc_html=(f'<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;">'
                  f'<span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:var(--muted);">Rank #{cmc.get("rank","?")}</span>'
                  f'<span style="background:var(--panel);border:1px solid var(--border);border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:var(--muted);">MCap {fmt(cmc.get("mcap",0))}</span>'
                  f'<span style="background:{"var(--green-bg)" if chg>=0 else "var(--red-bg)"};border:1px solid {"var(--green-bd)" if chg>=0 else "var(--red-bd)"};border-radius:4px;padding:2px 7px;font-family:monospace;font-size:.6rem;color:{chg_c};">{"+".join(["" if chg<0 else "+"])}{chg:.2f}%</span></div>')

    dual_html=""
    if dual_confirmed:
        dual_html='<div class="dual-confirm">🔥 DUAL CONFIRMED — Scanner + Sentinel both flagged this coin — HIGH CONVICTION SIGNAL</div>'

    warnings_html=""
    if warnings_list:
        wi="".join([f'<div style="padding:3px 0;font-size:.68rem;">{w}</div>' for w in warnings_list])
        warnings_html=(f'<div style="background:#fef3c7;border:1px solid #f59e0b;border-left:4px solid #f59e0b;border-radius:6px;padding:8px 12px;margin:6px 0;">'
                       f'<div style="font-family:monospace;font-size:.6rem;font-weight:700;color:#92400e;margin-bottom:4px;">⚠️ RISK WARNINGS</div>'
                       f'<div style="font-family:monospace;color:#92400e;">{wi}</div></div>')

    # 4H OB + FVG display
    ob4h_html=""
    bull_obs=res.get('bull_obs_4h',[]); bear_obs=res.get('bear_obs_4h',[])
    bull_fvgs=res.get('bull_fvgs',[]); bear_fvgs=res.get('bear_fvgs',[])
    if sig=="LONG":
        if bull_obs:
            o=bull_obs[0]; ob4h_html+=f'<div style="background:#ecfdf5;border-left:3px solid #059669;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#065f46;margin:4px 0;">🧱 4H OB Support: ${o["zone_lo"]:.5f}–${o["zone_hi"]:.5f} | body {o["body_pct"]*100:.0f}% | {o["dist_pct"]:.1f}% below</div>'
        if bull_fvgs:
            f=bull_fvgs[0]; ob4h_html+=f'<div style="background:#eff6ff;border-left:3px solid #2563eb;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#1e40af;margin:4px 0;">🔷 4H FVG Gap: ${f["zone_lo"]:.5f}–${f["zone_hi"]:.5f} | {f["dist_pct"]:.1f}% away — SL in gap</div>'
    elif sig=="SHORT":
        if bear_obs:
            o=bear_obs[0]; ob4h_html+=f'<div style="background:#fef2f2;border-left:3px solid #dc2626;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#7f1d1d;margin:4px 0;">🧱 4H OB Resistance: ${o["zone_lo"]:.5f}–${o["zone_hi"]:.5f} | {o["dist_pct"]:.1f}% above</div>'
        if bear_fvgs:
            f=bear_fvgs[0]; ob4h_html+=f'<div style="background:#fff7ed;border-left:3px solid #ea580c;border-radius:6px;padding:6px 12px;font-family:monospace;font-size:.66rem;color:#7c2d12;margin:4px 0;">🔷 4H FVG Gap: ${f["zone_lo"]:.5f}–${f["zone_hi"]:.5f} | {f["dist_pct"]:.1f}% away</div>'

    social_html=""
    sd=res.get('social_data',{})
    if not isinstance(sd,dict): sd={}
    if sd.get('available'):
        scfg={'BULLISH':('#059669','#ecfdf5','#a7f3d0','📈'),'BEARISH':('#dc2626','#fef2f2','#fecaca','📉'),'NEUTRAL':('#d97706','#fffbeb','#fde68a','💬')}
        sc_color,sc_bg,sc_bd,sc_emoji=scfg.get(sd.get('sentiment','NEUTRAL'),scfg['NEUTRAL'])
        bp=min(100,int((sd.get('mentions',0)/max(1,S.get('social_buzz_threshold',10)))*100))
        avg_line=f' · avg {sd.get("upvote_avg",0):.0f} upvotes' if sd.get('upvote_avg',0)>0 else ""
        tp60=sd.get('top_post','')[:60]
        tl=f'<div style="font-size:.6rem;color:{sc_color};margin-top:4px;opacity:.85;">"{tp60}..."</div>' if tp60 else ""
        social_html=(f'<div style="background:{sc_bg};border:1px solid {sc_bd};border-left:4px solid {sc_color};border-radius:6px;padding:8px 12px;margin:6px 0;">'
                     f'<div style="display:flex;justify-content:space-between;"><span style="font-family:monospace;font-size:.62rem;font-weight:700;color:{sc_color};">{sc_emoji} {sd.get("source","Reddit")} Buzz</span>'
                     f'<span style="font-family:monospace;font-size:.7rem;font-weight:700;color:{sc_color};">{sd.get("sentiment","NEUTRAL")}</span></div>'
                     f'<div style="display:flex;align-items:center;gap:8px;margin-top:5px;"><div style="flex:1;height:4px;background:rgba(0,0,0,.08);border-radius:2px;"><div style="width:{bp}%;height:100%;background:{sc_color};border-radius:2px;"></div></div>'
                     f'<span style="font-family:monospace;font-size:.6rem;color:{sc_color};">{sd.get("mentions",0)} mentions/hr{avg_line}</span></div>{tl}</div>')

    whale_html=""
    wd=res.get('whale_details',[])
    if wd:
        w=wd[0]; wc="var(--green)" if w['side']=='BUY' else "var(--red)"; wbg="var(--green-bg)" if w['side']=='BUY' else "var(--red-bg)"
        whale_html=(f'<div style="background:{wbg};border-left:3px solid {wc};border-radius:6px;padding:7px 12px;font-family:monospace;font-size:.72rem;color:{wc};margin:6px 0;font-weight:600;display:flex;justify-content:space-between;">'
                    f'<span>🐋 {w["side"]} WALL {fmt(w["value"])} @ ${w["price"]:.6f}</span>'
                    f'<span style="color:var(--muted);font-size:.58rem;">({w["dist_pct"]:.2f}% {"below" if w["side"]=="BUY" else "above"})</span></div>')

    liq_html=""
    if res.get('liq_target',0):
        liq_html=f'<div style="background:var(--purple-bg);border-left:3px solid var(--purple);border-radius:6px;padding:7px 12px;font-size:.72rem;color:var(--purple);margin:6px 0;">🧲 Liq magnet @ {res["liq_detail"]}</div>'

    mom_html=""
    if momentum_confirmed:
        mc="var(--green)" if sig=="LONG" else "var(--red)"; mb="var(--green-bg)" if sig=="LONG" else "var(--red-bg)"
        mom_html=f'<span class="momentum-badge" style="background:{mb};color:{mc};border:1px solid {mc};">{"▲" if sig=="LONG" else "▼"} MOMENTUM LIVE</span>'

    rsi_dir=res.get('rsi_direction','FLAT'); rsi_dir_html=""
    if rsi_dir=='RISING' and sig=="LONG":   rsi_dir_html='<span style="background:#ecfdf5;color:#059669;border:1px solid #a7f3d0;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ↑</span>'
    elif rsi_dir=='FALLING' and sig=="SHORT": rsi_dir_html='<span style="background:#fef2f2;color:#dc2626;border:1px solid #fecaca;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ↓</span>'
    elif (rsi_dir=='FALLING' and sig=="LONG") or (rsi_dir=='RISING' and sig=="SHORT"): rsi_dir_html='<span style="background:#fef3c7;color:#92400e;border:1px solid #fde68a;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.56rem;font-weight:700;">RSI ⚠</span>'

    sentiment_html=""
    if sentiment.get('available'):
        def sbar(label,pct,ch,cl):
            c=ch if pct>50 else cl
            return (f'<div class="sentiment-bar"><div class="sbar-label">{label}</div>'
                    f'<div class="sbar-track"><div class="sbar-fill" style="width:{pct:.0f}%;background:{c};"></div></div>'
                    f'<div class="sbar-val" style="color:{c};">{pct:.0f}%</div></div>')
        sentiment_html=(f'<div style="margin:8px 0;padding:10px;background:var(--panel);border-radius:8px;border:1px solid var(--border);">'
                        f'<div style="font-family:monospace;font-size:.55rem;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:6px;">{sentiment.get("source","Exchange")} Sentiment</div>'
                        f'{sbar("Longs",sentiment["top_long_pct"],"var(--green)","var(--red)")}'
                        f'{sbar("Retail Long",sentiment["retail_long_pct"],"var(--green)","var(--red)")}'
                        f'{sbar("Taker Buy Vol",sentiment["taker_buy_pct"],"var(--green)","var(--red)")}</div>')

    oi_change=res.get('oi_change_6h',0); oi_change_html=""
    if abs(oi_change)>=5:
        oc="var(--green)" if oi_change>0 else "var(--red)"; ob_bg="var(--green-bg)" if oi_change>0 else "var(--red-bg)"
        oi_change_html=f'<span style="background:{ob_bg};color:{oc};border:1px solid;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.6rem;font-weight:600;">OI {oi_change:+.1f}% 6h</span>'

    # ALL reasons (no truncation on card)
    def _esc(s): return str(s).replace("{","{{").replace("}","}}")
    reasons_html="".join([f"<div class='r'><span style='color:var(--muted);margin-right:6px;'>▸</span>{_esc(r)}</div>" for r in res['reasons']])

    _price=float(res.get('price') or 0); _tp=float(res.get('tp') or 0); _sl=float(res.get('sl') or 0)
    _rsi=float(res.get('rsi') or 0)
    _tp1=float(res.get('tp1') or _tp or 0); _tp2=float(res.get('tp2') or _tp or 0); _tp3=float(res.get('tp3') or _tp or 0)
    tp1_pct=f"+{abs(_tp1-_price)/_price*100:.2f}%" if _price>0 and _tp1>0 else ""
    tp2_pct=f"+{abs(_tp2-_price)/_price*100:.2f}%" if _price>0 and _tp2>0 else ""
    tp3_pct=f"+{abs(_tp3-_price)/_price*100:.2f}%" if _price>0 and _tp3>0 else ""
    sl_pct=f"-{abs(_price-_sl)/_price*100:.2f}%" if _price>0 and _sl>0 else ""
    pct_24h=float(res.get('pct_24h',0) or 0); pct_col="var(--green)" if pct_24h>=0 else "var(--red)"
    pct_display=f"{'+'if pct_24h>=0 else ''}{pct_24h:.2f}%"
    _elo_d=f"${float(res.get('entry_lo') or 0):.6f}"; _ehi_d=f"${float(res.get('entry_hi') or 0):.6f}"
    sym_cls=res.get('cls','—').upper(); ts=res.get('timestamp',''); session=res.get('session','')
    new_badge='<span style="background:#0ea5e9;color:#fff;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">🆕 NEW</span>' if res.get('is_new') else ''
    jump_sc=res.get('score_jump',0)
    jump_badge=f'<span style="background:#f59e0b22;color:#f59e0b;font-size:.52rem;font-weight:700;padding:1px 6px;border-radius:3px;margin-right:4px;">⬆️ +{jump_sc}pt</span>' if jump_sc>=15 else ''
    warn_count_html=f'<span style="background:#fef3c7;color:#92400e;border:1px solid #f59e0b;padding:2px 7px;border-radius:4px;font-family:monospace;font-size:.58rem;font-weight:700;">⚠️ {len(warnings_list)} RISK</span>' if warnings_list else ''
    try:
        rr=abs(_tp-_price)/abs(_price-_sl) if abs(_price-_sl)>0 else 0
        rr_str=f"{rr:.1f}:1"; rr_col="var(--green)" if rr>=2 else ("var(--amber)" if rr>=1.5 else "var(--red)")
    except: rr_str="N/A"; rr_col="var(--muted)"
    exch_bg=exch_col+"22"; exch_bd=exch_col+"44"; sig_bg=sig_col+"18"; sig_bd=sig_col+"44"
    score_bg=col+"14"; sniper_cls=" sniper" if is_sniper else ""
    try:
        card_html=f"""<div class="pump-card {card_cls}">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;">
    <div style="display:flex;align-items:center;gap:12px;">
      <div class="score-ring{sniper_cls}" style="color:{col};border-color:{col};background:{score_bg};">{sc}</div>
      <div>
        <div style="font-family:monospace;font-size:1.2rem;font-weight:700;">{res['symbol']}</div>
        <div style="display:flex;align-items:center;gap:6px;margin-top:3px;flex-wrap:wrap;">
          <span style="background:{exch_bg};border:1px solid {exch_bd};color:{exch_col};padding:1px 7px;border-radius:3px;font-family:monospace;font-size:.58rem;font-weight:700;">{exch}</span>
          <span style="background:{sig_bg};border:1px solid {sig_bd};color:{sig_col};padding:2px 10px;border-radius:3px;font-family:monospace;font-size:.72rem;font-weight:800;">{'🟢 LONG' if sig=='LONG' else '🔴 SHORT'}</span>
          <span style="font-family:monospace;font-size:.6rem;color:{col};font-weight:600;">{lbl}</span>
          {mom_html}{rsi_dir_html}{oi_change_html}{warn_count_html}{new_badge}{jump_badge}
          <span style="font-family:monospace;font-size:.58rem;color:var(--muted);">{ts}</span>
        </div>
      </div>
    </div>
    <div style="text-align:right;font-family:monospace;font-size:.58rem;color:var(--muted);">
      R:R <span style="color:{rr_col};font-weight:600;">{rr_str}</span><br>RSI <span style="color:var(--text);">{_rsi:.1f}</span><br>{session}
    </div>
  </div>
  {dual_html}{warnings_html}
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
    <div class="pip-item">{pip(max(0,bd.get('dst_boost',0)),5,8)} DEMA+ST</div>
  </div>
  {f'''<div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:6px;padding:6px 10px;margin:4px 0;display:flex;gap:6px;flex-wrap:wrap;align-items:center;">
    <span style="font-family:monospace;font-size:.55rem;font-weight:700;color:#0284c7;">🔵 DEMA+ST</span>
    <span style="font-family:monospace;font-size:.55rem;background:{"#dcfce7" if r.get("dst_confirmed") else "#fef2f2"};color:{"#16a34a" if r.get("dst_confirmed") else "#dc2626"};padding:1px 6px;border-radius:3px;">{"✅ CONFIRMED" if r.get("dst_confirmed") else "❌ NO SIGNAL"}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#64748b;">DIR: {"🟢 " + r["dst_signal"]["direction"] if r.get("dst_signal") else "–"}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#64748b;">RSI: {str(round(r["dst_signal"]["rsi"],1)) if r.get("dst_signal") else "–"}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#64748b;">VOL: {str(r["dst_signal"]["vol_ratio"])+"×" if r.get("dst_signal") else "–"}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#64748b;">ST FLIP: {str(r["dst_signal"]["fresh_bars"])+" bars ago" if r.get("dst_signal") else "–"}</span>
    <span style="font-family:monospace;font-size:.55rem;color:#64748b;">R:R {str(r["dst_signal"]["rr"]) if r.get("dst_signal") else "–"}</span>
  </div>''' if r.get("dst_signal") or r.get("dst_confirmed") is not None else
  '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:5px 10px;margin:4px 0;"><span style="font-family:monospace;font-size:.55rem;color:#94a3b8;">🔵 DEMA+ST — no signal this candle</span></div>'}
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
    <span style="font-family:monospace;font-size:.5rem;color:#60a5fa;margin-left:8px;">wait for pullback into zone</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin:8px 0;">
    <div class="px-cell"><div class="px-lbl">Entry</div><div class="px-val" style="color:var(--blue);font-size:.72rem;">${_price:.6f}</div><div style="font-family:monospace;font-size:.5rem;color:{pct_col};">{pct_display} 24h</div></div>
    <div class="px-cell" style="border-top:2px solid #86efac44;"><div class="px-lbl">TP1 <span style="color:#86efac;font-size:.5rem;">scalp</span></div><div class="px-val" style="color:#86efac;font-size:.68rem;">${_tp1:.6f}</div><div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp1_pct}</div></div>
    <div class="px-cell" style="border-top:2px solid var(--green);"><div class="px-lbl">TP2 <span style="color:var(--green);font-size:.5rem;">target</span></div><div class="px-val" style="color:var(--green);font-size:.72rem;">${_tp2:.6f}</div><div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp2_pct}</div></div>
    <div class="px-cell" style="border-top:2px solid #f59e0b;"><div class="px-lbl">TP3 <span style="color:#f59e0b;font-size:.5rem;">max run</span></div><div class="px-val" style="color:#f59e0b;font-size:.68rem;">${_tp3:.6f}</div><div style="font-family:monospace;font-size:.5rem;color:var(--muted);">{tp3_pct}</div></div>
    <div class="px-cell" style="border-top:2px solid var(--red);"><div class="px-lbl">Stop Loss</div><div class="px-val" style="color:var(--red);font-size:.72rem;">${_sl:.6f}</div><div style="font-family:monospace;font-size:.52rem;color:var(--muted);">{sl_pct}</div></div>
  </div>
  {whale_html}{liq_html}{social_html}{cmc_html}{sentiment_html}
  <div class="reasons-list" style="margin-top:8px;">{reasons_html}</div>
  <div style="margin-top:10px;display:flex;gap:12px;align-items:center;">
    <a href="{trade_link}" target="_blank" style="font-family:monospace;font-size:.62rem;color:var(--blue);text-decoration:none;font-weight:600;">Trade {res['symbol']} on {exch} →</a>
    <span style="font-family:monospace;font-size:.58rem;color:var(--muted);">Class: <b style="color:var(--text);">{sym_cls}</b></span>
  </div>
</div>"""
        card_html="\n".join(line.lstrip() for line in card_html.splitlines())
        st.markdown(card_html,unsafe_allow_html=True)
    except Exception as ce:
        st.error(f"Card render error [{res.get('symbol','?')}]: {ce}")
        st.code(f"{res.get('symbol','?')} | Score:{res.get('pump_score','?')} | {res.get('type','?')} | ${res.get('price',0):.6f}")


# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚡ APEX5")
    st.caption("Pump & Dump Intelligence")
    # FIX: Use session state for nav to prevent journal→scanner glitch
    nav_options=["🔥 Scanner","⚙️ Settings","📒 Journal","📊 Backtest","🧠 Catalyst"]
    nav=st.radio("Navigation",nav_options,label_visibility="collapsed",
                 index=nav_options.index(st.session_state.get('nav_state','🔥 Scanner')))
    if nav!=st.session_state.nav_state:
        st.session_state.nav_state=nav
        st.rerun()
    st.divider()
    st.subheader("Quick Controls")
    q_depth=st.slider("Coins to Scan",10,100,S['scan_depth'],step=10,key="q_depth")
    q_min=st.slider("Min Score",1,100,S['min_score'],key="q_min")
    q_btc=st.toggle("BTC Bear blocks LONGs",S['btc_filter'],key="q_btc")
    q_mom=st.toggle("Require Momentum",S.get('require_momentum',False),key="q_mom")
    st.caption("Scan Focus (select multiple):")
    _saved_modes=S.get('scan_modes',['mixed'])
    if isinstance(_saved_modes,str): _saved_modes=[_saved_modes]
    q_mode_vol=st.checkbox("Volume",value='volume' in _saved_modes,key="q_vol")
    q_mode_gain=st.checkbox("Gainers",value='gainers' in _saved_modes,key="q_gain")
    q_mode_loss=st.checkbox("Losers",value='losers' in _saved_modes,key="q_loss")
    q_mode_mixed=st.checkbox("Mixed",value='mixed' in _saved_modes,key="q_mixed")
    selected_modes=([m for m,v in [('volume',q_mode_vol),('gainers',q_mode_gain),('losers',q_mode_loss),('mixed',q_mode_mixed)] if v]) or ['mixed']
    st.divider()
    st.subheader("Auto-Pilot")
    q_auto=st.toggle("Continuous Scan",S.get('auto_scan',False))
    q_auto_int=st.number_input("Interval (mins)",1,60,S.get('auto_interval',5))
    st.divider()
    if st.button("Clear Cooldowns"):
        if os.path.exists(COOLDOWN_FILE): os.remove(COOLDOWN_FILE)
        st.success("Done")
    st.subheader("Sentinel")
    st.caption("Scans TOP 100 coins continuously.")
    q_sentinel=st.toggle("Enable Sentinel",st.session_state.get("sentinel_active",False))
    if q_sentinel!=st.session_state.get("sentinel_active",False):
        st.session_state.sentinel_active=q_sentinel
        st.session_state.sentinel_total_checked=0; st.session_state.sentinel_signals_found=0
    if st.session_state.get("sentinel_active"):
        st.write(f"Checked: {st.session_state.get('sentinel_total_checked',0)} | Signals: {st.session_state.get('sentinel_signals_found',0)}")
    sn,sm,sc_=get_session()
    st.divider(); st.write(f"Session: **{sn}** ({sm}x)")


# ─── HEADER / TICKER ─────────────────────────────────────────────────────────
st.markdown('<div style="padding:18px 0 14px;"><div style="font-family:monospace;font-size:1.5rem;font-weight:700;color:#0f1117;">APEX5</div><div style="font-family:monospace;font-size:.56rem;font-weight:400;letter-spacing:.16em;color:#7a82a0;text-transform:uppercase;margin-top:2px;">Pump & Dump Intelligence Terminal v3.0 — Dual Confirm + FVG + Backtest</div></div>',unsafe_allow_html=True)

if time.time()-st.session_state.get('fng_last_fetch',0)>300:
    try:
        fg=requests.get("https://api.alternative.me/fng/?limit=1",timeout=2).json()
        st.session_state.fng_val=int(fg['data'][0]['value']); st.session_state.fng_txt=fg['data'][0]['value_classification']
        st.session_state.fng_last_fetch=time.time()
    except: pass

fng_v=st.session_state.fng_val; fng_t=st.session_state.fng_txt
fng_c="#059669" if fng_v>=60 else ("#dc2626" if fng_v<=40 else "#d97706")
btc_c="#059669" if st.session_state.btc_trend=="BULLISH" else ("#dc2626" if st.session_state.btc_trend=="BEARISH" else "#7a82a0")
sn_,_,sc_now=get_session()
st.markdown(f"""<div class="ticker-bar">
  <div><div class="t-lbl">BTC</div><div class="t-val">${st.session_state.btc_price:,.0f} <span style="color:{btc_c};">{st.session_state.btc_trend}</span></div></div>
  <div><div class="t-lbl">Fear &amp; Greed</div><div class="t-val" style="color:{fng_c};">{fng_v} — {fng_t.upper()}</div></div>
  <div><div class="t-lbl">Session</div><div class="t-val" style="color:{sc_now};">{sn_}</div></div>
  <div><div class="t-lbl">Last Scan</div><div class="t-val">{st.session_state.last_scan}</div></div>
  <div><div class="t-lbl">Raw / Filtered</div><div class="t-val">{st.session_state.last_raw_count} / {len(st.session_state.results)}</div></div>
  <div><div class="t-lbl">Scans</div><div class="t-val">#{st.session_state.scan_count}</div></div>
  <div><div class="t-lbl">UTC</div><div class="t-val">{datetime.now(timezone.utc).strftime('%H:%M:%S')}</div></div>
  <div><div class="t-lbl">PKT</div><div class="t-val">{(datetime.now(timezone.utc) + __import__('datetime').timedelta(hours=5)).strftime('%H:%M:%S')}</div></div>
</div>""",unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════
if nav=="⚙️ Settings":
    st.markdown('<div class="section-h">Settings — all thresholds and scoring weights</div>',unsafe_allow_html=True)
    with st.form("settings_form"):
        # ── ALERTS ────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">1. 🔔 Notification Controls</div>',unsafe_allow_html=True)
        ac1,ac2,ac3=st.columns(3)
        with ac1: ns_alert_score=st.slider("Min Score for Alerts",10,100,S.get('alert_min_score',60))
        with ac2:
            st.markdown("**Types:**")
            ns_al_long=st.checkbox("Alert LONGs",S.get('alert_longs',True))
            ns_al_short=st.checkbox("Alert SHORTs",S.get('alert_shorts',True))
        with ac3:
            st.markdown("**Classes:**")
            ns_al_sq=st.checkbox("Squeeze",S.get('alert_squeeze',True))
            ns_al_br=st.checkbox("Breakout",S.get('alert_breakout',True))
            ns_al_wh=st.checkbox("Whale",S.get('alert_whale',True))
            ns_al_ea=st.checkbox("Early",S.get('alert_early',False))
        st.markdown("**Daily Summary Alert:**")
        ds1,ds2=st.columns(2)
        with ds1: ns_ds_on=st.toggle("Daily Journal Summary to Discord/Telegram",S.get('daily_summary_on',True))
        with ds2: ns_ds_hr=st.slider("Summary hour (UTC)",0,23,int(S.get('daily_summary_hour',8)))
        st.markdown('<div class="setting-help">Min Score: only send Discord/Telegram alerts for signals above this score. Types: filter LONG/SHORT alerts. Classes: filter by signal category. Daily Summary: sends a journal recap at the chosen UTC hour.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── JOURNAL FILTERS (NEW) ─────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#2563eb;"><div class="stg-title" style="color:#2563eb;">2. 📒 Journal Logging Filters — What Gets Saved</div>',unsafe_allow_html=True)
        jf1,jf2,jf3=st.columns(3)
        with jf1:
            st.markdown("**Signal Classes to Log:**")
            j_cls_opts=["god_tier","squeeze","breakout","whale_driven","early"]
            saved_j_cls=S.get('j_filter_classes',j_cls_opts)
            ns_j_cls=st.multiselect("Classes",j_cls_opts,default=saved_j_cls,key="j_cls_ms")
            st.markdown('<div class="setting-help">Only signals matching these classes will be saved to journal</div>',unsafe_allow_html=True)
        with jf2:
            ns_j_min_sc=st.slider("Min score to journal",1,100,int(S.get('j_min_score',25)))
            st.markdown('<div class="setting-help">Signals below this score won\'t be journaled regardless of class</div>',unsafe_allow_html=True)
        with jf3:
            st.markdown("**Required Technicals (ALL must be present):**")
            tech_opts=["ob","funding","oi","volume","whale","sentiment","mtf","orderflow"]
            saved_j_tech=S.get('j_require_technicals',[])
            ns_j_tech=st.multiselect("Must have signals",tech_opts,default=saved_j_tech,key="j_tech_ms")
            st.markdown('<div class="setting-help">Leave empty = no checklist. Add items = ALL must be active for signal to be journaled</div>',unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── RISK FILTERS ──────────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#f59e0b;"><div class="stg-title" style="color:#92400e;">3. ⚠️ Risk & Anti-False-Positive Filters</div>',unsafe_allow_html=True)
        rf1,rf2,rf3=st.columns(3)
        with rf1:
            ns_late_thresh=st.number_input("Late Entry threshold %",3.0,20.0,float(S.get('late_entry_chg_thresh',8.0)),0.5,format="%.1f")
            ns_late_pen=st.slider("Late Entry penalty pts",5,30,int(S.get('late_entry_penalty',20)))
        with rf2:
            ns_exhaust_pen=st.slider("Volume Exhaustion penalty pts",5,25,int(S.get('vol_exhaust_penalty',15)))
            ns_near_top_pen=st.slider("Near Local Top penalty pts",5,25,int(S.get('near_top_penalty',15)))
        with rf3:
            ns_use_4h_ob=st.toggle("Use 4H OB+FVG for SL",S.get('use_4h_ob_for_sl',True))
            st.info("FVG (Fair Value Gaps) on 4H are now also used as SL zones alongside Order Blocks.")
        st.markdown('<div class="setting-help">Late Entry: penalizes signals where price already moved too far. Higher threshold = more lenient. Exhaustion/Near Top penalties reduce score when volume fades or price is near a local high. 4H OB+FVG: uses order blocks as smarter SL placement.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── SCAN ──────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">4. 🔍 Scan Configuration</div>',unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1: ns_depth=st.slider("Coins to scan",10,200,S['scan_depth'],step=10)
        with c2: ns_fast=st.selectbox("Signal TF",["1m","5m","15m","1h"],index=["1m","5m","15m","1h"].index(S['fast_tf']))
        with c3: ns_slow=st.selectbox("Trend TF",["1h","4h","1d"],index=["1h","4h","1d"].index(S['slow_tf']))
        st.markdown('<div class="setting-help">Coins to scan: higher = more signals but slower scan. Signal TF: the fast timeframe used for entry triggers (5m recommended). Trend TF: the slow timeframe used to confirm direction (4H recommended).</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── FILTERS ───────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">5. 🎯 Core Filters</div>',unsafe_allow_html=True)
        c1,c2,c3,c4,c5,c6=st.columns(6)
        with c1: ns_min=st.slider("Min score to show",1,100,S['min_score'])
        with c2:
            ns_ji=st.checkbox("Log IMMINENT (70+)",S.get('j_imminent',True))
            ns_jb=st.checkbox("Log BUILDING (45+)",S.get('j_building',True))
            ns_je=st.checkbox("Log EARLY (25+)",S.get('j_early',False))
        with c3:
            ns_minr=st.slider("Min signals",1,10,S.get('min_reasons',1))
        with c4:
            ns_btc=st.toggle("BTC Bear block",S['btc_filter'])
            ns_mom=st.toggle("Require momentum",S.get('require_momentum',False))
        with c5:
            ns_cd=False
            ns_cdh=0
        with c6:
            ns_min_rr=st.slider("Min R:R",1.0,4.0,float(S.get('min_rr',1.5)),step=0.1)
        st.markdown('<div class="setting-help">Min score: hide signals below this score. Min signals: require at least N confirming indicators. BTC Bear block: suppress LONG signals when BTC is bearish. Require momentum: only show signals with active momentum. Min R:R: minimum reward-to-risk ratio — 2.0 means TP is 2× further than SL.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── WHALE WALLS ───────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">6. 🐋 Whale Wall Detection</div>',unsafe_allow_html=True)
        c1,c2,c3,c4=st.columns(4)
        with c1: ns_whale=st.selectbox("Min wall size",[50000,100000,250000,500000,1000000,5000000],index=[50000,100000,250000,500000,1000000,5000000].index(S['whale_min_usdt']),format_func=lambda x:f"${x:,.0f}")
        with c2: ns_pts_wh_near=st.slider("Points ≤0.5%",10,35,S.get('pts_whale_near',25))
        with c3: ns_pts_wh_mid=st.slider("Points 0.5–1.5%",5,25,S.get('pts_whale_mid',15))
        with c4: ns_pts_wh_far=st.slider("Points 1.5–3%",3,15,S.get('pts_whale_far',8))
        st.markdown('<div class="setting-help">Min wall size: minimum USDT order size to count as a whale wall. Larger = fewer but more significant walls. Points: how many score points a wall adds based on distance from price — closer walls are worth more points.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── ORDER BOOK ────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">7. ⚖️ Order Book / Funding / OI / Volume</div>',unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_ob_l=st.number_input("OB low ratio",0.5,3.0,S['ob_ratio_low'],0.05,format="%.2f"); ns_pts_ob_l=st.slider("OB pts low",1,15,S['pts_ob_low'])
            ns_ob_m=st.number_input("OB mid ratio",0.5,5.0,S['ob_ratio_mid'],0.1,format="%.2f"); ns_pts_ob_m=st.slider("OB pts mid",5,20,S['pts_ob_mid'])
            ns_ob_h=st.number_input("OB high ratio",1.0,10.0,S['ob_ratio_high'],0.25,format="%.2f"); ns_pts_ob_h=st.slider("OB pts high",10,30,S['pts_ob_high'])
        with c2:
            ns_fr_l=st.number_input("Funding low",0.00001,0.005,S['funding_low'],0.00005,format="%.5f"); ns_pts_fr_l=st.slider("Funding pts low",1,15,S['pts_funding_low'],key="pfrl")
            ns_fr_m=st.number_input("Funding mid",0.0001,0.01,S['funding_mid'],0.0001,format="%.5f"); ns_pts_fr_m=st.slider("Funding pts mid",5,20,S['pts_funding_mid'],key="pfrm")
            ns_fr_h=st.number_input("Funding high",0.0005,0.05,S['funding_high'],0.0005,format="%.5f"); ns_pts_fr_h=st.slider("Funding pts high",10,30,S['pts_funding_high'],key="pfrh")
        with c3:
            ns_oi_l=st.number_input("OI price chg low%",0.1,5.0,S['oi_price_chg_low'],0.1,format="%.1f"); ns_pts_oi_l=st.slider("OI pts low",1,15,S['pts_oi_low'],key="poil")
            ns_oi_h=st.number_input("OI price chg high%",0.5,10.0,S['oi_price_chg_high'],0.5,format="%.1f"); ns_pts_oi_h=st.slider("OI pts high",5,25,S['pts_oi_high'],key="poih")
            ns_vs_l=st.number_input("Vol surge low×",1.0,3.0,S['vol_surge_low'],0.1,format="%.1f"); ns_pts_vs_l=st.slider("Vol pts low",1,10,S['pts_vol_low'],key="pvl")
            ns_vs_m=st.number_input("Vol surge mid×",1.5,5.0,S['vol_surge_mid'],0.25,format="%.1f"); ns_pts_vs_m=st.slider("Vol pts mid",3,15,S['pts_vol_mid'],key="pvm")
            ns_vs_h=st.number_input("Vol surge high×",2.0,10.0,S['vol_surge_high'],0.5,format="%.1f"); ns_pts_vs_h=st.slider("Vol pts high",5,20,S['pts_vol_high'],key="pvh")
        st.markdown('<div class="setting-help">OB ratio: bid/ask imbalance threshold — higher ratio = stronger buying pressure. Funding: positive = longs paying shorts (bullish bias). OI: open interest change confirming new money entering. Vol surge: volume spike multiplier vs 20-period average. Each tier adds more score points as the signal strengthens.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── LIQ CLUSTERS / TECHNICALS / SENTIMENT ─────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">8. 🧲 Liq / Tech / Sentiment</div>',unsafe_allow_html=True)
        c1,c2,c3=st.columns(3)
        with c1:
            ns_lq_n=st.number_input("Liq near %",0.5,5.0,S['liq_cluster_near'],0.25,format="%.2f"); ns_pts_lq_n=st.slider("Pts near",5,20,S['pts_liq_near'],key="pln")
            ns_lq_m=st.number_input("Liq mid %",1.0,10.0,S['liq_cluster_mid'],0.5,format="%.1f"); ns_pts_lq_m=st.slider("Pts mid",3,15,S['pts_liq_mid'],key="plm")
            ns_lq_f=st.number_input("Liq far %",2.0,20.0,S['liq_cluster_far'],1.0,format="%.1f"); ns_pts_lq_f=st.slider("Pts far",1,10,S['pts_liq_far'],key="plf")
        with c2:
            ns_rsi_os=st.slider("RSI oversold (LONG)",20,55,S['rsi_oversold'])
            ns_rsi_ob=st.slider("RSI overbought (SHORT)",45,80,S['rsi_overbought'])
            ns_pts_macd=st.slider("MACD pts",1,10,S['pts_macd']); ns_pts_rsi=st.slider("RSI pts",1,10,S['pts_rsi'])
            ns_pts_bb=st.slider("BB pts",1,10,S['pts_bb']); ns_pts_ema=st.slider("EMA pts",1,8,S['pts_ema'])
        with c3:
            ns_pts_sent=st.slider("Sentiment pts",5,30,S.get('pts_sentiment',20))
            ns_pts_taker=st.slider("Taker pts",3,20,S.get('pts_taker',10))
            ns_v24_l=st.number_input("24h vol low $",100000,10000000,S['vol24h_low'],100000,format="%d"); ns_pts_v24_l=st.slider("Pts 24h low",1,8,S['pts_vol24_low'],key="pv24l")
            ns_v24_m=st.number_input("24h vol mid $",1000000,50000000,S['vol24h_mid'],1000000,format="%d"); ns_pts_v24_m=st.slider("Pts 24h mid",2,12,S['pts_vol24_mid'],key="pv24m")
            ns_v24_h=st.number_input("24h vol high $",5000000,200000000,S['vol24h_high'],5000000,format="%d"); ns_pts_v24_h=st.slider("Pts 24h high",4,15,S['pts_vol24_high'],key="pv24h")
        st.markdown('<div class="setting-help">Liq clusters: liquidation zones near price add score — closer = more points. RSI oversold/overbought: thresholds for LONG/SHORT entries. MACD/BB/EMA pts: score added when each indicator confirms. Sentiment/Taker: score for positive market sentiment and aggressive buying. 24h vol: minimum daily volume tiers for liquidity filtering.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── ACCURACY & INTELLIGENCE ────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">9. 🎯 Accuracy + Intelligence Signals</div>',unsafe_allow_html=True)
        af1,af2,af3=st.columns(3)
        with af1:
            ns_min_vol=st.number_input("Min 24h vol $",0,100000000,int(S.get('min_vol_filter',300000)),100000,format="%d")
            ns_min_sigs=st.slider("Min signal cats",1,6,int(S.get('min_active_signals',3)))
            ns_dedup=st.toggle("Dedup coins",S.get('dedup_symbols',True))
            ns_atr_min=st.number_input("ATR min %",0.0,2.0,float(S.get('atr_min_pct',0.2)),0.05,format="%.2f")
            ns_atr_max=st.number_input("ATR max %",2.0,20.0,float(S.get('atr_max_pct',10.0)),0.5,format="%.1f")
            ns_spread_max=st.number_input("Max spread %",0.0,2.0,float(S.get('spread_max_pct',0.5)),0.05,format="%.2f")
        with af2:
            ns_fng_lt=st.slider("F&G LONG min",10,45,int(S.get('fng_long_threshold',30)))
            ns_fng_st=st.slider("F&G SHORT min",55,90,int(S.get('fng_short_threshold',70)))
            ns_mtf=st.toggle("MTF confirmation",S.get('mtf_confirm',True))
            ns_pts_mtf=st.slider("MTF pts",0,20,int(S.get('pts_mtf',12)))
            ns_pts_div=st.slider("RSI div pts",0,20,int(S.get('pts_divergence',10)))
            ns_pts_cpat=st.slider("Candle pattern pts",0,15,int(S.get('pts_candle_pattern',8)))
        with af3:
            ns_pts_combo=st.slider("OI+Funding combo pts",0,20,int(S.get('pts_oi_funding_combo',10)))
            ns_vol_exp_t=st.number_input("Explosive vol ×avg",3.0,20.0,float(S.get('vol_surge_explosive',5.0)),0.5,format="%.1f")
            ns_pts_vol_e=st.slider("Explosive vol pts",10,30,int(S.get('pts_vol_explosive',20)))
            ns_of_lb=st.slider("Order flow lookback",5,30,int(S.get('orderflow_lookback',10)))
            ns_pts_of=st.slider("Order flow pts",3,20,int(S.get('pts_orderflow',12)))
            ns_pts_lm=st.slider("Liq map pts",3,25,int(S.get('pts_liq_map',15)))
            ns_pts_lst=st.slider("New listing pts",5,35,int(S.get('listing_alert_pts',25)))
            ns_oc_min=st.number_input("On-chain whale min $",100000,100000000,int(S.get('onchain_whale_min',500000)),100000,format="%d")
            ns_pts_oc=st.slider("On-chain pts",3,25,int(S.get('pts_onchain_whale',15)))
        st.markdown('<div class="setting-help">Liq clusters: liquidation zones near price add score — closer = more points. RSI oversold/overbought: thresholds for LONG/SHORT entries. MACD/BB/EMA pts: score added when each indicator confirms. Sentiment/Taker: score for positive market sentiment and aggressive buying. 24h vol: minimum daily volume tiers for liquidity filtering.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── SENTINEL ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#7c3aed;"><div class="stg-title" style="color:#7c3aed;">10. 🛰️ Sentinel Mode</div>',unsafe_allow_html=True)
        sc1,sc2,sc3=st.columns(3)
        with sc1: ns_sent_score=st.slider("Alert threshold",40,95,int(S.get('sentinel_score_threshold',70)))
        with sc2: ns_sent_batch=st.slider("Coins per batch",2,20,int(S.get('sentinel_batch_size',5)))
        with sc3: ns_sent_interval=st.slider("Secs between batches",5,120,int(S.get('sentinel_check_interval',30)))
        st.markdown('<div class="setting-help">Sentinel monitors coins continuously in the background. Alert threshold: only alert when score exceeds this. Coins per batch: how many coins to check per cycle — lower = gentler on API. Secs between batches: pause between each batch to avoid rate limits.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── CLASSIFIER ────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">11. 🏷️ Tab Classifier Thresholds</div>',unsafe_allow_html=True)
        _ca,_cb,_cc=st.columns(3)
        with _ca:
            st.caption('🟡 Breakout')
            ns_br_oi=st.slider('Min OI spike pts',1,20,int(S.get('cls_breakout_oi_min',7)),key='br_oi')
            ns_br_vol=st.slider('Min Vol surge pts',1,15,int(S.get('cls_breakout_vol_min',4)),key='br_vol')
            ns_br_sc=st.slider('Min score',10,60,int(S.get('cls_breakout_score_min',25)),key='br_sc')
        with _cb:
            st.caption('🔴 Squeeze')
            ns_sq_fd=st.slider('Min Funding pts',1,20,int(S.get('cls_squeeze_fund_min',8)),key='sq_fd')
            ns_sq_ob=st.slider('Min OB pts',1,15,int(S.get('cls_squeeze_ob_min',6)),key='sq_ob')
        with _cc:
            st.caption('ℹ️ Note'); st.info('Unmatched → Early tab.')
        st.markdown('<div class="setting-help">Controls which tab a signal is sorted into. Breakout: requires OI spike + vol surge. Squeeze: requires funding pressure + order book imbalance. Any signal not meeting Breakout or Squeeze criteria goes to the Early tab.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── AUTO-JOURNAL CHECK ────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#059669;"><div class="stg-title" style="color:#059669;">12. 📒 Auto-Journal Exit Tracking</div>',unsafe_allow_html=True)
        aj1,aj2=st.columns(2)
        with aj1:
            ns_aj_on=st.toggle("Enable Auto-Journal Tracking",S.get('journal_autocheck_on',True))
            ns_aj_mins=st.slider("Check interval (mins)",1,60,int(S.get('journal_autocheck_mins',15)))
        with aj2:
            ns_aj_force=st.checkbox("🔄 Force check on Save",value=False)
        st.markdown('<div class="setting-help">Controls which tab a signal is sorted into. Breakout: requires OI spike + vol surge. Squeeze: requires funding pressure + order book imbalance. Any signal not meeting Breakout or Squeeze criteria goes to the Early tab.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── SOCIAL ────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">13. 📡 Social Media</div>',unsafe_allow_html=True)
        so1,so2,so3,so4=st.columns(4)
        with so1: ns_social_en=st.toggle("Enable Reddit Buzz",S.get('social_enabled',True))
        with so2: ns_social_wt=st.slider("Max Reddit pts",3,20,int(S.get('social_reddit_weight',8)))
        with so3: ns_social_min=st.slider("Min mentions",1,10,int(S.get('social_min_mentions',3)))
        with so4: ns_social_buzz=st.slider("Mentions for max pts",5,50,int(S.get('social_buzz_threshold',10)))
        ns_apify=st.text_input("Apify Token (optional)",S.get('apify_token',''),type="password")
        st.markdown('<div class="setting-help">Reddit Buzz scans social media for coin mentions. Max Reddit pts: maximum score added from social signals. Min mentions: ignore coins mentioned fewer than this many times. Mentions for max pts: how many mentions needed to earn full points. Apify token: required for live Reddit scraping — leave empty to disable.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)
        # ── DEMA + SUPERTREND ─────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#0284c7;"><div class="stg-title" style="color:#0284c7;">14. 🔵 DEMA + SuperTrend Strategy</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint">Runs alongside APEX5 on every scan. When both strategies agree → score boost. When they conflict → small penalty. Disable to skip entirely.</div>', unsafe_allow_html=True)
        dst_c1, dst_c2, dst_c3, dst_c4 = st.columns(4)
        with dst_c1:
            st.markdown("**Core**")
            ns_dst_enabled   = st.toggle("Enable DEMA+ST", S.get('dst_enabled', True))
            ns_dst_dema_len  = st.slider("DEMA Length", 5, 500, int(S.get('dst_dema_len', 200)))
            st.markdown('<div class="setting-help">200 = slow trend filter (recommended). Lower = faster but more noise.</div>', unsafe_allow_html=True)
            ns_dst_st_period = st.slider("ST ATR Period", 5, 30, int(S.get('dst_st_period', 10)))
            ns_dst_st_mult   = st.number_input("ST Multiplier", 1.0, 6.0, float(S.get('dst_st_mult', 3.0)), 0.25, format="%.2f")
            st.markdown('<div class="setting-help">Higher multiplier = fewer but stronger ST flips</div>', unsafe_allow_html=True)
            st.markdown("**Risk Management**")
            ns_dst_use_trail = st.toggle("Use Trailing Stop", S.get('dst_use_trail', False))
            ns_dst_trail_atr = st.number_input("Trail Stop ATR ×", 0.5, 5.0, float(S.get('dst_trail_atr', 2.5)), 0.25, format="%.2f")
            st.markdown("**Profit Management**")
            ns_dst_partial   = st.toggle("Partial Profit Taking", S.get('dst_partial', True))
            ns_dst_partial_pct = st.number_input("Partial Exit %", 10, 90, int(S.get('dst_partial_pct', 50)))
            ns_dst_partial_rr  = st.number_input("Partial Exit at R:R", 0.5, 5.0, float(S.get('dst_partial_rr', 2.0)), 0.25, format="%.2f")
            ns_dst_breakeven   = st.toggle("Move Stop to Breakeven", S.get('dst_breakeven', True))
            ns_dst_be_rr       = st.number_input("Breakeven at R:R", 0.5, 3.0, float(S.get('dst_be_rr', 1.5)), 0.25, format="%.2f")
        with dst_c2:
            st.markdown("**Filters**")
            ns_dst_use_vol   = st.toggle("Use Volume Filter",   S.get('dst_use_vol',   True))
            ns_dst_vol_mult  = st.number_input("Min Volume ×avg", 1.0, 3.0, float(S.get('dst_vol_mult', 1.2)), 0.1, format="%.1f")
            ns_dst_use_rsi   = st.toggle("Use RSI Filter",      S.get('dst_use_rsi',   True))
            ns_dst_rsi_ob    = st.slider("RSI Overbought", 55, 80, int(S.get('dst_rsi_ob', 65)))
            ns_dst_rsi_os    = st.slider("RSI Oversold", 20, 45, int(S.get('dst_rsi_os', 35)))
            ns_dst_use_mom   = st.toggle("Use Momentum Filter", S.get('dst_use_mom',   True))
            ns_dst_mom_bars  = st.slider("Momentum Bars", 1, 10, int(S.get('dst_mom_bars', 3)))
            ns_dst_use_dema_dist = st.toggle("Use DEMA Distance Filter", S.get('dst_use_dema_dist', True))
            ns_dst_min_dema_dist = st.number_input("Min DEMA Dist %", 0.0, 2.0, float(S.get('dst_min_dema_dist', 0.1)), 0.05, format="%.2f")
            st.markdown('<div class="setting-help">Toggle each filter on/off independently</div>', unsafe_allow_html=True)
        with dst_c3:
            st.markdown("**Freshness & TP/SL**")
            ns_dst_fresh_bars = st.slider("Max bars since ST flip", 1, 20, int(S.get('dst_fresh_bars', 5)))
            st.markdown('<div class="setting-help">Only fires if ST flipped within this many candles</div>', unsafe_allow_html=True)
            ns_dst_sl_atr = st.number_input("SL ATR ×", 0.5, 4.0, float(S.get('dst_sl_atr', 1.5)), 0.25, format="%.2f")
            ns_dst_timeframe = st.selectbox("Timeframe", ["1m","3m","5m","15m","30m","1h","4h"], index=["1m","3m","5m","15m","30m","1h","4h"].index(S.get('dst_timeframe','5m')))
            ns_dst_tp_atr = st.number_input("TP ATR ×", 1.0, 8.0, float(S.get('dst_tp_atr', 3.0)), 0.25, format="%.2f")
            st.markdown('<div class="setting-help">DST TP/SL shown in confirm reason only. APEX5 TP/SL is unchanged.</div>', unsafe_allow_html=True)
        with dst_c4:
            st.markdown("**Scoring**")
            ns_dst_boost   = st.slider("Boost when confirmed", 0, 20, int(S.get('dst_confirm_boost', 8)))
            ns_dst_penalty = st.slider("Penalty when conflicting", 0, 15, int(S.get('dst_conflict_penalty', 5)))
            st.markdown('<div class="setting-help"><b>Confirmed:</b> both agree → +boost pts<br><b>Conflict:</b> disagree → −penalty pts<br>Set penalty to 0 to ignore conflicts</div>', unsafe_allow_html=True)
            dst_col = "#0284c7" if S.get('dst_enabled', True) else "#9ca3af"
            dst_txt = "🟢 DEMA+ST ACTIVE on every scan" if S.get('dst_enabled', True) else "⚫ DEMA+ST DISABLED"
            st.markdown(f'<div style="background:#eff6ff;border-left:4px solid {dst_col};border-radius:6px;padding:8px 12px;font-family:monospace;font-size:.65rem;color:{dst_col};margin-top:8px;">{dst_txt}</div>', unsafe_allow_html=True)
            st.markdown("---")
        st.markdown("**🎯 DST Watchlist**")
        ns_dst_use_watchlist = st.toggle("Scan watchlist only", S.get('dst_use_watchlist', False))
        st.markdown('<div class="setting-help">When ON — only scans coins below. When OFF — scans all APEX5 coins.</div>', unsafe_allow_html=True)

        TOP_100_FUTURES = [
            "BTC","ETH","SOL","BNB","XRP","DOGE","ADA","AVAX","LINK","DOT",
            "MATIC","UNI","ATOM","LTC","BCH","APT","OP","ARB","INJ","SUI",
            "TIA","SEI","PEPE","WIF","BONK","FLOKI","SHIB","FTM","NEAR","ALGO",
            "VET","FIL","SAND","MANA","AXS","GALA","ENJ","CHZ","FLOW","ICP",
            "EGLD","XLM","HBAR","EOS","TRX","XMR","ZEC","DASH","NEO","WAVES",
            "KAVA","BAND","CELO","ZIL","ONE","ANKR","CRV","SNX","COMP","AAVE",
            "MKR","YFI","SUSHI","1INCH","BAL","REN","UMA","ZRX","LRC","DYDX",
            "GMX","GNS","PERP","BLUR","APE","LOOKS","IMX","GODS","BEAM","MAGIC",
            "RUNE","LUNA","OSMO","JUNO","SCRT","ROWAN","AKT","STARS","EVMOS","CRO",
            "OKB","HT","KCS","GT","MX","WOO","DODO","RDNT","STG","PENDLE"
        ]
        TOP_100_FUTURES.sort()
        ns_dst_watchlist = st.multiselect(
            "Select coins to watch (saved on Submit)",
            options=TOP_100_FUTURES,
            default=[c for c in S.get('dst_watchlist', []) if c in TOP_100_FUTURES],
            key='dst_wl_multiselect'
        )
        if ns_dst_watchlist:
            st.markdown(f'<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;padding:6px 10px;font-family:monospace;font-size:.6rem;color:#0284c7;margin:4px 0;">👁 Watching {len(ns_dst_watchlist)} coins: {", ".join(ns_dst_watchlist)}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="font-family:monospace;font-size:.6rem;color:#94a3b8;">No coins selected — DST will scan all APEX5 coins</div>', unsafe_allow_html=True)


        st.markdown('</div>', unsafe_allow_html=True)
        # ── PROFESSIONAL FILTERS ──────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#7c3aed;"><div class="stg-title" style="color:#7c3aed;">15. ⚡ Professional Filters</div>', unsafe_allow_html=True)
        st.markdown('<div class="hint">Advanced filters used by professional traders to eliminate low-quality signals.</div>', unsafe_allow_html=True)

        pf_c1, pf_c2, pf_c3 = st.columns(3)

        with pf_c1:
            st.markdown("**🕐 Kill-Switch (Time Filter)**")
            ns_ks_on = st.toggle("Enable Kill-Switch", S.get('kill_switch_on', False))
            st.markdown('<div class="setting-help">No alerts during these UTC hours. Add up to 3 kill zones.</div>', unsafe_allow_html=True)
            current_h = datetime.now(timezone.utc).hour
            existing_zones = S.get('kill_zones', [{"start":16,"end":19,"label":"Late NY","active":True}])
            ns_kill_zones = []
            defaults = [{"start":16,"end":19,"label":"Late NY / Asia Transition"},
                        {"start":0,"end":4,"label":"Asia Dead Hours"},
                        {"start":12,"end":14,"label":"London Lunch"}]
            for i in range(3):
                zone = existing_zones[i] if i < len(existing_zones) else defaults[i]
                st.markdown(f"**Zone {i+1}**")
                zc1, zc2 = st.columns(2)
                with zc1:
                    zs = st.slider(f"From (UTC)", 0, 23, int(zone.get('start', defaults[i]['start'])), key=f'ks_zone_{i}_start')
                with zc2:
                    ze = st.slider(f"Until (UTC)", 0, 23, int(zone.get('end', defaults[i]['end'])), key=f'ks_zone_{i}_end')
                zc3, zc4 = st.columns([3,1])
                with zc3:
                    zl = st.text_input(f"Label", value=zone.get('label', defaults[i]['label']), key=f'ks_zone_{i}_label')
                with zc4:
                    st.markdown("<br>", unsafe_allow_html=True)
                    za = st.checkbox("On", value=zone.get('active', i==0), key=f'ks_zone_{i}_active')
                if za:
                    ns_kill_zones.append({"start":zs,"end":ze,"label":zl,"active":True})
                    if ns_ks_on:
                        in_kz = zs <= current_h < ze
                        kz_color = "#dc2626" if in_kz else "#059669"
                        kz_txt = f"🔴 ACTIVE" if in_kz else f"🟢 outside"
                        st.markdown(f'<div style="border-left:3px solid {kz_color};padding:3px 8px;font-family:monospace;font-size:.58rem;color:{kz_color};">{kz_txt}</div>', unsafe_allow_html=True)
                else:
                    ns_kill_zones.append({"start":zs,"end":ze,"label":zl,"active":False})
            ns_ks_start = ns_kill_zones[0]['start'] if ns_kill_zones else 16
            ns_ks_end = ns_kill_zones[0]['end'] if ns_kill_zones else 19

        with pf_c2:
            st.markdown("**📊 Required Technicals**")
            st.markdown('<div class="setting-help">When ON — coin MUST have this score > 0 to signal.</div>', unsafe_allow_html=True)
            rt_c1, rt_c2 = st.columns(2)
            with rt_c1:
                ns_req_4h_fvg   = st.toggle("4H FVG",        S.get('req_4h_fvg',      False))
                ns_req_4h_ob    = st.toggle("4H OB",         S.get('req_4h_ob',       False))
                ns_req_5m_align = st.toggle("5m Alignment",  S.get('req_5m_align',    False))
                ns_req_ob       = st.toggle("OB Imbalance",  S.get('req_ob',          False))
                ns_req_whale    = st.toggle("Whale Wall",    S.get('req_whale',       False))
                ns_req_funding  = st.toggle("Funding",       S.get('req_funding',     False))
                ns_req_oi       = st.toggle("OI Spike",      S.get('req_oi',          False))
                ns_req_volume   = st.toggle("Volume Surge",  S.get('req_volume',      False))
                ns_req_liq      = st.toggle("Liq Cluster",   S.get('req_liq',         False))
            with rt_c2:
                ns_req_mtf      = st.toggle("MTF Align",     S.get('req_mtf',         False))
                ns_req_momentum = st.toggle("Momentum",      S.get('req_momentum',    False))
                ns_req_sentiment= st.toggle("Sentiment",     S.get('req_sentiment',   False))
                ns_req_social   = st.toggle("Social Buzz",   S.get('req_social',      False))
                ns_req_orderflow= st.toggle("Order Flow",    S.get('req_orderflow',   False))
                ns_req_liq_map  = st.toggle("Liq Map",       S.get('req_liq_map',     False))
                ns_req_onchain  = st.toggle("On-Chain",      S.get('req_onchain',     False))
                ns_req_wyckoff  = st.toggle("Wyckoff Spring",S.get('req_wyckoff',     False))
                ns_req_cvd      = st.toggle("CVD Divergence",S.get('req_cvd',         False))
                ns_req_stophunt = st.toggle("Stop Hunt",     S.get('req_stophunt',    False))
            ns_req_technicals = st.toggle("Technicals Score",S.get('req_technicals',  False))
            ns_req_session    = st.toggle("Session Score",   S.get('req_session',     False))
            st.markdown('<div class="setting-help">SMC setup = enable 4H FVG + 4H OB + MTF. Reduces signals 70-80% but higher conviction.</div>', unsafe_allow_html=True)

        with pf_c3:
            st.markdown("**🔬 Market Condition Filters**")
            ns_rs_btc       = st.toggle("Relative Strength vs BTC", S.get('rs_btc_filter', False))
            ns_rs_btc_min   = st.number_input("Min RS vs BTC (1H)", -10.0, 10.0, float(S.get('rs_btc_min', 0.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">Only LONG if coin outperforming BTC by this % on 1H.</div>', unsafe_allow_html=True)
            ns_atr_exp      = st.toggle("ATR Expansion Filter",     S.get('atr_expansion_filter', False))
            ns_atr_exp_mult = st.number_input("ATR Expansion ×24h avg", 1.0, 5.0, float(S.get('atr_expansion_mult', 1.5)), 0.25, format="%.2f")
            st.markdown('<div class="setting-help">Only signal if current ATR is this × above 24h average. Avoids sideways grind markets.</div>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)
        # ── GAINERS & LOSERS ──────────────────────────────────────────────
        st.markdown('<div class="stg-card" style="border-color:#7c3aed;"><div class="stg-title" style="color:#7c3aed;">16. 📊 Gainers & Losers Scanner</div>', unsafe_allow_html=True)
        st.markdown('<div class="setting-help">Scans the real top gainers and losers from GATE futures exchange. Runs automatically on startup and every X minutes. Use the button to force a scan anytime.</div>', unsafe_allow_html=True)
        gl_s0a, gl_s0b = st.columns(2)
        with gl_s0a:
            ns_symbol_blacklist = st.text_input("Symbol blacklist (comma separated)", S.get('symbol_blacklist',''), placeholder="e.g. SILVER,XAG,UKOIL,BRENT")
            st.markdown('<div class="setting-help">Coins in this list will be excluded from ALL scans (APEX5 + G/L). Commodities like XAU, XAG, SILVER, UKOIL are auto-filtered — add any extras here.</div>', unsafe_allow_html=True)
        with gl_s0b:
            ns_ms_refresh = st.slider("Market Pulse refresh interval (min)", 1, 30, int(S.get('ms_refresh_interval', 5)))
            st.markdown('<div class="setting-help">How often Market Pulse auto-refreshes. ⬇️ Lower = more frequent but more API calls. Recommended: 5 min.</div>', unsafe_allow_html=True)
        gl_s1, gl_s2, gl_s3 = st.columns(3)
        with gl_s1:
            st.markdown("**⚙️ Core**")
            ns_gl_enabled  = st.toggle("Enable G/L Scanner", S.get('gl_enabled', True))
            st.markdown('<div class="setting-help">Master on/off switch for the entire G/L scanner.</div>', unsafe_allow_html=True)
            ns_gl_interval = st.number_input("Scan interval (min)", 1, 60, int(S.get('gl_interval', 5)), format="%d")
            st.markdown('<div class="setting-help">⬇️ Lower = more frequent scans, more API calls. ⬆️ Higher = less frequent, saves resources. Recommended: 5 min.</div>', unsafe_allow_html=True)
            ns_gl_top_n    = st.slider("Top N coins to scan", 5, 50, int(S.get('gl_top_n', 20)))
            st.markdown('<div class="setting-help">⬇️ Lower = faster scan, fewer coins checked. ⬆️ Higher = more coins checked but slower. Recommended: 20.</div>', unsafe_allow_html=True)
            ns_gl_min_rr   = st.number_input("Min R:R", 0.5, 5.0, float(S.get('gl_min_rr', 1.5)), 0.25, format="%.2f")
            st.markdown('<div class="setting-help">Minimum reward:risk ratio. ⬇️ Lower = more signals but lower quality. ⬆️ Higher = fewer but better setups. Recommended: 1.5–2.0.</div>', unsafe_allow_html=True)
        with gl_s2:
            st.markdown("**📏 Thresholds**")
            ns_gl_min_gain    = st.number_input("Min gain % (gainer)", 0.5, 20.0, float(S.get('gl_min_gain_pct', 3.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">⬇️ Lower = catches smaller moves, more signals. ⬆️ Higher = only strong momentum coins qualify. Recommended: 3%.</div>', unsafe_allow_html=True)
            ns_gl_min_loss    = st.number_input("Min loss % (loser)",  0.5, 20.0, float(S.get('gl_min_loss_pct', 3.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">⬇️ Lower = catches smaller drops for bounce trades. ⬆️ Higher = only deeply oversold coins qualify. Recommended: 3%.</div>', unsafe_allow_html=True)
            ns_gl_pb_min      = st.number_input("Min pullback %", 0.5, 5.0, float(S.get('gl_pullback_min', 1.5)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">Minimum retracement after initial pump. ⬇️ Lower = enter earlier in pullback (riskier). ⬆️ Higher = wait for deeper pullback (safer entry, fewer signals).</div>', unsafe_allow_html=True)
            ns_gl_pb_max      = st.number_input("Max pullback %", 3.0, 20.0, float(S.get('gl_pullback_max', 8.0)), 0.5, format="%.1f")
            st.markdown('<div class="setting-help">Maximum allowed pullback. ⬆️ Higher = allows deeper retracements. If pullback exceeds this the trend may be reversing — not a pullback anymore.</div>', unsafe_allow_html=True)
            ns_gl_vol_exp     = st.number_input("Vol expansion ×", 0.1, 5.0, float(S.get('gl_vol_expansion', 0.3)), 0.1, format="%.1f")
            st.markdown('<div class="setting-help">Minimum volume vs 20-bar average. ⬇️ Lower = more signals in quiet markets. ⬆️ Higher = only high conviction volume spikes. Recommended: 0.3–1.3.</div>', unsafe_allow_html=True)
            ns_gl_rsi_ob      = st.slider("RSI overbought", 60, 85, int(S.get('gl_rsi_ob', 75)))
            st.markdown('<div class="setting-help">RSI ceiling for gainer signals. ⬆️ Higher = allows more extended coins. ⬇️ Lower = stricter, avoids overbought entries.</div>', unsafe_allow_html=True)
            ns_gl_rsi_os      = st.slider("RSI oversold",   15, 40, int(S.get('gl_rsi_os', 30)))
            st.markdown('<div class="setting-help">RSI floor for loser bounce signals. ⬇️ Lower = only deeply oversold coins qualify. ⬆️ Higher = catches earlier bounces but riskier.</div>', unsafe_allow_html=True)
        with gl_s3:
            st.markdown("**🎯 Signal Types**")
            st.markdown('<div class="setting-help">Enable/disable each signal type individually.</div>', unsafe_allow_html=True)
            ns_gl_pullback  = st.toggle("Gainer Pullback 🟢",   S.get('gl_alert_pullback',  True))
            st.markdown('<div class="setting-help">Coin up 3%+ on 4H, pulled back 1.5-8%, volume declining — second leg entry.</div>', unsafe_allow_html=True)
            ns_gl_breakout  = st.toggle("Gainer Breakout 🚀",   S.get('gl_alert_breakout',  True))
            st.markdown('<div class="setting-help">Coin up 3%+ and breaking to new high with volume — momentum continuation.</div>', unsafe_allow_html=True)
            ns_gl_bounce    = st.toggle("Loser Bounce 🔄",      S.get('gl_alert_bounce',    True))
            st.markdown('<div class="setting-help">Coin down 3%+ with capitulation volume spike and RSI oversold — reversal entry.</div>', unsafe_allow_html=True)
            ns_gl_breakdown = st.toggle("Loser Breakdown 📉",   S.get('gl_alert_breakdown', True))
            st.markdown('<div class="setting-help">Coin down 3%+ breaking to new low with volume — short entry setup.</div>', unsafe_allow_html=True)
            ns_gl_pregainer = st.toggle("Pre-Gainer Watch 👀",  S.get('gl_alert_pregainer', True))
            st.markdown('<div class="setting-help">Flat coin with volume accumulating and RSI crossing 50 — watch for imminent pump.</div>', unsafe_allow_html=True)
            ns_gl_preloser  = st.toggle("Pre-Loser Watch ⚠️",   S.get('gl_alert_preloser',  True))
            st.markdown('<div class="setting-help">Flat coin with declining volume on bounces and RSI crossing below 50 — watch for imminent drop.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        # ── BACKTEST ──────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">17. 📊 Backtest Defaults</div>',unsafe_allow_html=True)
        bt1,bt2=st.columns(2)
        with bt1: ns_bt_min_sc=st.slider("Backtest min score",1,100,int(S.get('backtest_min_score',50)))
        with bt2: ns_bt_days=st.slider("Backtest days",7,90,int(S.get('backtest_days',30)))
        st.markdown('<div class="setting-help">Backtest min score: only replay journal signals above this score. ⬆️ Higher = fewer but higher quality signals tested. Backtest days: how far back to look for historical OHLCV data. ⬆️ Higher = longer test window but slower to load.</div>', unsafe_allow_html=True)
        st.markdown('</div>',unsafe_allow_html=True)

        # ── APIs ──────────────────────────────────────────────────────────
        st.markdown('<div class="stg-card"><div class="stg-title">18. 🔑 API Keys</div>',unsafe_allow_html=True)
        c1,c2=st.columns(2)
        with c1:
            ns_tg_tok=st.text_input("Telegram Bot Token",S.get('tg_token',''),type="password")
            ns_tg_cid=st.text_input("Telegram Chat ID",S.get('tg_chat_id',''))
            ns_okx_key=st.text_input("OKX API Key",S.get('okx_key',''),type="password")
            ns_okx_sec=st.text_input("OKX API Secret",S.get('okx_secret',''),type="password")
            ns_okx_pass=st.text_input("OKX Passphrase",S.get('okx_passphrase',''),type="password")
        with c2:
            ns_dc_web=st.text_input("Discord Webhook URL",S.get('discord_webhook',''),type="password")
            ns_groq_key=st.text_input("Groq API Key (Catalyst Intelligence)",S.get('groq_key',''),type="password")
            ns_cmc=st.text_input("CMC API Key",S.get('cmc_key',''),type="password")
            ns_gate_key=st.text_input("Gate.io API Key",S.get('gate_key',''),type="password")
            ns_gate_sec=st.text_input("Gate.io API Secret",S.get('gate_secret',''),type="password")
        st.markdown('</div>',unsafe_allow_html=True)

        submitted=st.form_submit_button("💾 SAVE ALL SETTINGS",use_container_width=True)
        if submitted:
            new_s={
                'scan_depth':ns_depth,'fast_tf':ns_fast,'slow_tf':ns_slow,'scan_modes':selected_modes,
                'min_score':ns_min,'j_imminent':ns_ji,'j_building':ns_jb,'j_early':ns_je,
                'j_filter_classes':ns_j_cls,'j_min_score':ns_j_min_sc,'j_require_technicals':ns_j_tech,
                'min_reasons':ns_minr,'btc_filter':ns_btc,'require_momentum':ns_mom,
                'cooldown_on':ns_cd,'cooldown_hrs':ns_cdh,'whale_min_usdt':ns_whale,'min_rr':ns_min_rr,
                'alert_min_score':ns_alert_score,'alert_longs':ns_al_long,'alert_shorts':ns_al_short,
                'alert_squeeze':ns_al_sq,'alert_breakout':ns_al_br,'alert_early':ns_al_ea,'alert_whale':ns_al_wh,
                'daily_summary_on':ns_ds_on,'daily_summary_hour':ns_ds_hr,
                'late_entry_chg_thresh':ns_late_thresh,'late_entry_penalty':ns_late_pen,
                'vol_exhaust_penalty':ns_exhaust_pen,'near_top_penalty':ns_near_top_pen,'use_4h_ob_for_sl':ns_use_4h_ob,
                'ob_ratio_low':ns_ob_l,'ob_ratio_mid':ns_ob_m,'ob_ratio_high':ns_ob_h,
                'pts_ob_low':ns_pts_ob_l,'pts_ob_mid':ns_pts_ob_m,'pts_ob_high':ns_pts_ob_h,
                'funding_low':ns_fr_l,'funding_mid':ns_fr_m,'funding_high':ns_fr_h,
                'pts_funding_low':ns_pts_fr_l,'pts_funding_mid':ns_pts_fr_m,'pts_funding_high':ns_pts_fr_h,
                'oi_price_chg_low':ns_oi_l,'oi_price_chg_high':ns_oi_h,'pts_oi_low':ns_pts_oi_l,'pts_oi_high':ns_pts_oi_h,
                'vol_surge_low':ns_vs_l,'vol_surge_mid':ns_vs_m,'vol_surge_high':ns_vs_h,
                'pts_vol_low':ns_pts_vs_l,'pts_vol_mid':ns_pts_vs_m,'pts_vol_high':ns_pts_vs_h,
                'liq_cluster_near':ns_lq_n,'liq_cluster_mid':ns_lq_m,'liq_cluster_far':ns_lq_f,
                'pts_liq_near':ns_pts_lq_n,'pts_liq_mid':ns_pts_lq_m,'pts_liq_far':ns_pts_lq_f,
                'vol24h_low':ns_v24_l,'vol24h_mid':ns_v24_m,'vol24h_high':ns_v24_h,
                'pts_vol24_low':ns_pts_v24_l,'pts_vol24_mid':ns_pts_v24_m,'pts_vol24_high':ns_pts_v24_h,
                'rsi_oversold':ns_rsi_os,'rsi_overbought':ns_rsi_ob,
                'pts_macd':ns_pts_macd,'pts_rsi':ns_pts_rsi,'pts_bb':ns_pts_bb,'pts_ema':ns_pts_ema,
                'pts_session':S['pts_session'],'pts_sentiment':ns_pts_sent,'pts_taker':ns_pts_taker,
                'pts_whale_near':ns_pts_wh_near,'pts_whale_mid':ns_pts_wh_mid,'pts_whale_far':ns_pts_wh_far,
                'min_vol_filter':ns_min_vol,'min_active_signals':ns_min_sigs,'dedup_symbols':ns_dedup,
                'atr_min_pct':ns_atr_min,'atr_max_pct':ns_atr_max,'spread_max_pct':ns_spread_max,
                'fng_long_threshold':ns_fng_lt,'fng_short_threshold':ns_fng_st,'mtf_confirm':ns_mtf,
                'pts_mtf':ns_pts_mtf,'pts_divergence':ns_pts_div,'pts_candle_pattern':ns_pts_cpat,
                'pts_oi_funding_combo':ns_pts_combo,'vol_surge_explosive':ns_vol_exp_t,'pts_vol_explosive':ns_pts_vol_e,
                'sentinel_score_threshold':ns_sent_score,'sentinel_batch_size':ns_sent_batch,'sentinel_check_interval':ns_sent_interval,
                'cls_breakout_oi_min':ns_br_oi,'cls_breakout_vol_min':ns_br_vol,'cls_breakout_score_min':ns_br_sc,
                'cls_squeeze_fund_min':ns_sq_fd,'cls_squeeze_ob_min':ns_sq_ob,
                'journal_autocheck_on':ns_aj_on,'journal_autocheck_mins':ns_aj_mins,
                'orderflow_lookback':ns_of_lb,'pts_orderflow':ns_pts_of,'pts_liq_map':ns_pts_lm,
                'listing_alert_pts':ns_pts_lst,'onchain_whale_min':ns_oc_min,'pts_onchain_whale':ns_pts_oc,
                'social_enabled':ns_social_en,'social_reddit_weight':ns_social_wt,
                'social_min_mentions':ns_social_min,'social_buzz_threshold':ns_social_buzz,'apify_token':ns_apify,
                'backtest_min_score':ns_bt_min_sc,'backtest_days':ns_bt_days,
                'dst_enabled':ns_dst_enabled,'dst_dema_len':ns_dst_dema_len,
                'dst_st_period':ns_dst_st_period,'dst_st_mult':ns_dst_st_mult,
                'dst_use_vol':ns_dst_use_vol,'dst_vol_mult':ns_dst_vol_mult,'dst_use_rsi':ns_dst_use_rsi,
                'dst_rsi_ob':ns_dst_rsi_ob,'dst_rsi_os':ns_dst_rsi_os,'dst_use_mom':ns_dst_use_mom,
                'dst_mom_bars':ns_dst_mom_bars,'dst_use_dema_dist':ns_dst_use_dema_dist,
                'dst_min_dema_dist':ns_dst_min_dema_dist,
                'dst_rsi_os':ns_dst_rsi_os,'dst_mom_bars':ns_dst_mom_bars,
                'dst_min_dema_dist':ns_dst_min_dema_dist,'dst_fresh_bars':ns_dst_fresh_bars,
                'dst_sl_atr':ns_dst_sl_atr,'dst_tp_atr':ns_dst_tp_atr,
                'dst_confirm_boost':ns_dst_boost,'dst_conflict_penalty':ns_dst_penalty,
                'kill_switch_on':ns_ks_on,'kill_zones':ns_kill_zones,'kill_switch_start':ns_ks_start,'kill_switch_end':ns_ks_end,
                'req_4h_fvg':ns_req_4h_fvg,'req_4h_ob':ns_req_4h_ob,'req_5m_align':ns_req_5m_align,
                'req_ob':ns_req_ob,'req_whale':ns_req_whale,'req_funding':ns_req_funding,
                'req_oi':ns_req_oi,'req_volume':ns_req_volume,'req_liq':ns_req_liq,
                'req_technicals':ns_req_technicals,'req_session':ns_req_session,
                'req_momentum':ns_req_momentum,'req_mtf':ns_req_mtf,'req_sentiment':ns_req_sentiment,
                'req_social':ns_req_social,'req_orderflow':ns_req_orderflow,'req_liq_map':ns_req_liq_map,
                'req_onchain':ns_req_onchain,'req_wyckoff':ns_req_wyckoff,'req_cvd':ns_req_cvd,
                'req_stophunt':ns_req_stophunt,
                'rs_btc_filter':ns_rs_btc,'rs_btc_min':ns_rs_btc_min,
                'atr_expansion_filter':ns_atr_exp,'atr_expansion_mult':ns_atr_exp_mult,
                'symbol_blacklist':ns_symbol_blacklist,'ms_refresh_interval':ns_ms_refresh,
                'gl_enabled':ns_gl_enabled,'gl_interval':ns_gl_interval,'gl_top_n':ns_gl_top_n,
                'gl_min_rr':ns_gl_min_rr,'gl_min_gain_pct':ns_gl_min_gain,'gl_min_loss_pct':ns_gl_min_loss,
                'gl_pullback_min':ns_gl_pb_min,'gl_pullback_max':ns_gl_pb_max,
                'gl_vol_expansion':ns_gl_vol_exp,'gl_rsi_ob':ns_gl_rsi_ob,'gl_rsi_os':ns_gl_rsi_os,
                'gl_alert_pullback':ns_gl_pullback,'gl_alert_breakout':ns_gl_breakout,
                'gl_alert_bounce':ns_gl_bounce,'gl_alert_breakdown':ns_gl_breakdown,
                'gl_alert_pregainer':ns_gl_pregainer,'gl_alert_preloser':ns_gl_preloser,
                'dst_timeframe':ns_dst_timeframe,'dst_use_watchlist':ns_dst_use_watchlist,'dst_watchlist':ns_dst_watchlist,
                'dst_use_trail':ns_dst_use_trail,'dst_trail_atr':ns_dst_trail_atr,
                'dst_partial':ns_dst_partial,'dst_partial_pct':ns_dst_partial_pct,
                'dst_partial_rr':ns_dst_partial_rr,'dst_breakeven':ns_dst_breakeven,'dst_be_rr':ns_dst_be_rr,
                'cmc_key':ns_cmc,'tg_token':ns_tg_tok,'tg_chat_id':ns_tg_cid,'discord_webhook':ns_dc_web,
                'okx_key':ns_okx_key,'okx_secret':ns_okx_sec,'okx_passphrase':ns_okx_pass,
                'gate_key':ns_gate_key,'gate_secret':ns_gate_sec,
                'groq_key':ns_groq_key,
                'auto_scan':q_auto,'auto_interval':q_auto_int,
            }
            save_settings(new_s)
            if ns_aj_force: st.session_state.journal_last_autocheck=0
            st.success("✅ Settings saved!"); st.balloons()
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: JOURNAL
# ═══════════════════════════════════════════════════════════════════════════
if nav=="📒 Journal":
    ensure_journal()
    col1,col2=st.columns([5,1])
    with col1: st.markdown('<div class="section-h">Trade Journal & Tracking Dashboard</div>',unsafe_allow_html=True)
    with col2:
        if st.button("🗑️ Clear"):
            if os.path.exists(JOURNAL_FILE): os.remove(JOURNAL_FILE)
            st.rerun()
    try: df_j=pd.read_csv(JOURNAL_FILE)
    except: df_j=pd.DataFrame()
    if df_j.empty:
        st.markdown('<div class="empty-st">📒 No signals logged yet</div>',unsafe_allow_html=True)
    else:
        df_j['ts']=pd.to_datetime(df_j['ts'],errors='coerce')
        now=datetime.now(); df24=df_j[df_j['ts']>=(now-timedelta(hours=24))]
        longs=len(df24[df24['type']=='LONG']); shorts=len(df24[df24['type']=='SHORT'])
        tps=len(df24[df24['status']=='TP']); sls=len(df24[df24['status']=='SL'])
        active=len(df24[df24['status']=='ACTIVE']); wr=(tps/(tps+sls)*100) if (tps+sls)>0 else 0
        untouched=len(df24[(df24['status']=='ACTIVE') & (df24.get('entry_touched',pd.Series(['0']*len(df24)))!='1')]) if 'entry_touched' in df24.columns else 0
        st.info(f"**Journal Logging Rules:** Classes: {S.get('j_filter_classes',['all'])} | Min score: {S.get('j_min_score',25)} | Required technicals: {S.get('j_require_technicals',[]) or 'None'} — Change in ⚙️ Settings")
        st.markdown(f"""<div class="stat-strip">
          <div><div class="ss-val">{len(df24)}</div><div class="ss-lbl">24h Total</div></div>
          <div><div class="ss-val">{longs}/{shorts}</div><div class="ss-lbl">Long/Short</div></div>
          <div><div class="ss-val" style="color:var(--amber);">{active}</div><div class="ss-lbl">Active</div></div>
          <div><div class="ss-val" style="color:#94a3b8;">{untouched}</div><div class="ss-lbl">Awaiting Entry</div></div>
          <div><div class="ss-val" style="color:var(--green);">{tps}</div><div class="ss-lbl">TP Hits</div></div>
          <div><div class="ss-val" style="color:var(--red);">{sls}</div><div class="ss-lbl">SL Hits</div></div>
          <div><div class="ss-val" style="color:var(--blue);">{wr:.1f}%</div><div class="ss-lbl">Win Rate</div></div>
        </div>""",unsafe_allow_html=True)
        st.caption("⚡ TP/SL tracking starts only after price touches the entry zone (within 0.5% of logged entry price)")
        fc1,fc2,fc3,fc4,fc5=st.columns(5)
        with fc1: jt=st.selectbox("Type",["ALL","LONG","SHORT"])
        with fc2: jc=st.selectbox("Class",["ALL","squeeze","breakout","whale_driven","early"])
        with fc3: js_stat=st.selectbox("Status",["ALL","ACTIVE","TP","SL"])
        with fc4: jx=st.selectbox("Exchange",["ALL","OKX","GATE","MEXC"])
        with fc5: js=st.selectbox("Sort",["ts","pump_score","symbol"])
        dv=df_j.copy()
        if jt!="ALL" and 'type' in dv.columns: dv=dv[dv['type']==jt]
        if jc!="ALL" and 'class' in dv.columns: dv=dv[dv['class']==jc]
        if js_stat!="ALL" and 'status' in dv.columns: dv=dv[dv['status']==js_stat]
        if jx!="ALL" and 'exchange' in dv.columns: dv=dv[dv['exchange']==jx]
        if js in dv.columns: dv=dv.sort_values(js,ascending=(js!='pump_score'))
        st.dataframe(dv,use_container_width=True,height=500)
        st.download_button("⬇️ Export CSV",dv.to_csv(index=False).encode(),
            file_name=f"APEX5_{datetime.now().strftime('%Y%m%d')}.csv",mime="text/csv")

    # ═══════════════════════════════════════════════════════════════════
    # G/L PERFORMANCE DASHBOARD
    # ═══════════════════════════════════════════════════════════════════
    st.markdown('<div class="section-h">📊 G/L Signal Performance Dashboard</div>', unsafe_allow_html=True)

    gl_perf_col1, gl_perf_col2 = st.columns([1,1])
    with gl_perf_col1:
        if st.button("🔄 Re-check Outcomes Now"):
            with st.spinner("Checking TP/SL hits..."):
                check_gl_outcomes()
            st.rerun()
    with gl_perf_col2:
        if os.path.exists(GL_PERF_FILE):
            with open(GL_PERF_FILE,'rb') as f:
                st.download_button("⬇️ Download G/L CSV", f,
                    file_name=f"gl_performance_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv")

    try:
        df_gl = pd.read_csv(GL_PERF_FILE)
    except:
        df_gl = pd.DataFrame()

    if df_gl.empty:
        st.markdown('<div class="empty-st">📊 No G/L signals logged yet — run a G/L scan first</div>', unsafe_allow_html=True)
    else:
        # ── Summary metrics ──────────────────────────────────────────
        total  = len(df_gl)
        wins   = len(df_gl[df_gl['outcome']=='WIN'])
        losses = len(df_gl[df_gl['outcome']=='LOSS'])
        pending= len(df_gl[df_gl['outcome']=='PENDING'])
        expired= len(df_gl[df_gl['outcome']=='EXPIRED'])
        closed = wins + losses
        wr     = round(wins/closed*100) if closed>0 else 0
        avg_pnl= round(df_gl[df_gl['outcome'].isin(['WIN','LOSS'])]['pnl_pct'].astype(float).mean(),2) if closed>0 else 0

        m1,m2,m3,m4,m5,m6 = st.columns(6)
        m1.metric("Total Signals", total)
        m2.metric("Wins ✅", wins)
        m3.metric("Losses ❌", losses)
        m4.metric("Pending ⏳", pending)
        m5.metric("Win Rate", f"{wr}%")
        m6.metric("Avg PnL", f"{avg_pnl:+.2f}%")

        st.markdown("---")

        # ── Per signal type breakdown ────────────────────────────────
        st.markdown('<div style="font-family:monospace;font-size:.75rem;font-weight:700;color:#64748b;padding:4px 0;">By Signal Type</div>', unsafe_allow_html=True)
        type_emojis = {
            'GAINER_PULLBACK':'🟢','GAINER_BREAKOUT':'🚀',
            'LOSER_BOUNCE':'🔄','LOSER_BREAKDOWN':'📉',
            'PRE_GAINER':'👀','PRE_LOSER':'⚠️'
        }
        type_cols = st.columns(3)
        col_idx = 0
        gl_stats = get_gl_stats()
        for sig_type, stat in gl_stats.items():
            emoji = type_emojis.get(sig_type,'')
            wr_t = stat['wr']
            color = '#059669' if wr_t>=60 else '#f59e0b' if wr_t>=40 else '#dc2626'
            bar = '█'*int(wr_t/10) + '░'*(10-int(wr_t/10))
            with type_cols[col_idx % 3]:
                st.markdown(f'''
                <div style="background:#f8fafc;border:1px solid {color};border-radius:8px;padding:10px;margin:4px 0;">
                  <div style="font-family:monospace;font-size:.65rem;font-weight:700;color:{color};">{emoji} {sig_type.replace("_"," ")}</div>
                  <div style="font-family:monospace;font-size:.75rem;color:{color};">{bar} {wr_t}%</div>
                  <div style="font-family:monospace;font-size:.6rem;color:#475569;">{stat["wins"]}W / {stat["losses"]}L | avg: {stat["avg_pnl"]:+.1f}%</div>
                  <div style="font-family:monospace;font-size:.55rem;color:#94a3b8;">{stat["pending"]} pending • {stat["expired"]} expired</div>
                </div>''', unsafe_allow_html=True)
            col_idx += 1

        st.markdown("---")

        # ── Full signal log table ────────────────────────────────────
        st.markdown('<div style="font-family:monospace;font-size:.75rem;font-weight:700;color:#64748b;padding:4px 0;">All Signals Log</div>', unsafe_allow_html=True)
        display_cols = ['timestamp_utc','timestamp_pkt','symbol','type','direction','entry','tp','sl','rr','chg_4h','rsi','outcome','pnl_pct','outcome_time']
        df_show = df_gl[[c for c in display_cols if c in df_gl.columns]].copy()
        df_show = df_show.sort_values('timestamp_utc', ascending=False)

        def color_outcome(val):
            if val == 'WIN': return 'color: #059669; font-weight:700'
            if val == 'LOSS': return 'color: #dc2626; font-weight:700'
            if val == 'PENDING': return 'color: #f59e0b'
            return 'color: #94a3b8'

        st.dataframe(
            df_show.style.applymap(color_outcome, subset=['outcome']),
            use_container_width=True, height=400)

    st.stop()


# ═══════════════════════════════════════════════════════════════════════════
# PAGE: BACKTEST
# ═══════════════════════════════════════════════════════════════════════════
if nav=="📊 Backtest":
    st.markdown('<div class="section-h">Backtest — Replay Journal Signals Against Historical OHLCV</div>',unsafe_allow_html=True)

    # ── MODE SELECTOR ─────────────────────────────────────────────────────
    bt_mode=st.radio("Backtest Source",
        ["📊 System Journal","📂 Upload External CSV"],
        horizontal=True)
    st.markdown("---")

    # ══════════════════════════════════════════════════════════════════════
    # MODE 2 — EXTERNAL CSV UPLOAD
    # ══════════════════════════════════════════════════════════════════════
    if bt_mode=="📂 Upload External CSV":
        st.markdown('<div class="section-h">External CSV Backtest — Upload Your Own Signal Data</div>',unsafe_allow_html=True)
        st.info("📂 **Upload any CSV with your historical signals.** Required columns: `symbol`, `exchange`, `type` (LONG/SHORT), `tp`, `sl`, `price`, `ts` (timestamp). All other columns are preserved in results.")

        uploaded=st.file_uploader("Upload Signal CSV",type=["csv"])
        if uploaded is None:
            st.markdown("""<div class="tab-desc">
            <b>Required CSV columns:</b><br>
            • <b>ts</b> — signal timestamp (e.g. 2026-03-04 02:31:13)<br>
            • <b>symbol</b> — coin symbol (e.g. BTC, ETH, PEPE)<br>
            • <b>exchange</b> — OKX, GATE, or MEXC<br>
            • <b>type</b> — LONG or SHORT<br>
            • <b>tp</b> — take profit price<br>
            • <b>sl</b> — stop loss price<br>
            • <b>price</b> — entry price at signal time<br>
            <br>
            All other columns (score, class, reasons etc) are optional but will appear in results.
            </div>""",unsafe_allow_html=True)
            st.stop()

        try:
            df_ext=pd.read_csv(uploaded)
        except Exception as e:
            st.error(f"Could not read CSV: {e}"); st.stop()

        # Validate required columns
        required=['symbol','exchange','type','tp','sl','price','ts']
        missing=[c for c in required if c not in df_ext.columns]
        if missing:
            st.error(f"Missing required columns: {missing}")
            st.info(f"Your CSV has these columns: {list(df_ext.columns)}")
            st.stop()

        st.success(f"✅ Loaded {len(df_ext)} signals from uploaded CSV")
        st.dataframe(df_ext.head(5),use_container_width=True)

        # Filters
        ef1,ef2,ef3,ef4,ef5=st.columns(5)
        with ef1: ext_min_sc=st.slider("Min score",1,100,50,key="ext_sc") if 'pump_score' in df_ext.columns else 0
        with ef2: ext_type=st.selectbox("Type filter",["ALL","LONG","SHORT"],key="ext_type")
        with ef3: ext_exch=st.selectbox("Exchange",["ALL","OKX","GATE","MEXC"],key="ext_exch")
        with ef4: ext_cls=st.selectbox("Class",["ALL","squeeze","breakout","whale_driven","early"],key="ext_cls") if 'class' in df_ext.columns else "ALL"
        with ef5: ext_candles=st.slider("Max candles",50,500,200,step=50,key="ext_cnd")

        if st.button("▶️ RUN EXTERNAL BACKTEST",use_container_width=True):
            df_ext['ts']=pd.to_datetime(df_ext['ts'],errors='coerce')
            ext_mask=pd.Series([True]*len(df_ext))
            if 'pump_score' in df_ext.columns and ext_min_sc>0:
                ext_mask=ext_mask&(pd.to_numeric(df_ext['pump_score'],errors='coerce')>=ext_min_sc)
            if ext_type!="ALL":
                ext_mask=ext_mask&(df_ext['type']==ext_type)
            if ext_exch!="ALL":
                ext_mask=ext_mask&(df_ext['exchange']==ext_exch)
            if ext_cls!="ALL" and 'class' in df_ext.columns:
                ext_mask=ext_mask&(df_ext['class']==ext_cls)
            df_ext_sub=df_ext[ext_mask].copy()
            if df_ext_sub.empty:
                st.warning("No signals match filters."); st.stop()

            st.info(f"Backtesting {len(df_ext_sub)} signals from uploaded CSV...")
            ext_results=[]; ext_prog=st.progress(0); ext_status=st.empty()

            async def _ext_bt_fetch(sym, exch, ts_str, tp, sl, sig_type, max_candles):
                try:
                    from datetime import datetime as _dt
                    ts_dt=_dt.fromisoformat(str(ts_str)) if ts_str else None
                    if ts_dt is None: return {'result':'ERROR','error':'bad timestamp'}
                    ts_ms=int(ts_dt.timestamp()*1000)
                    tf=S.get('fast_tf','15m')
                    if exch=="OKX":
                        ex=ccxt.okx({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                    elif exch=="GATE":
                        ex=ccxt.gateio({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                    else:
                        ex=ccxt.mexc({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                    await ex.load_markets()
                    raw=await ex.fetch_ohlcv(f"{sym}/USDT:USDT",tf,since=ts_ms,limit=max_candles)
                    await ex.close()
                    if not raw or len(raw)<3: return {'result':'ERROR','error':'no OHLCV data'}
                    df_r=pd.DataFrame(raw,columns=['ts','open','high','low','close','volume'])
                    for idx,row in df_r.iterrows():
                        h=float(row['high']); l=float(row['low'])
                        if sig_type=="LONG":
                            if h>=float(tp) and l<=float(sl):
                                return {'result':'TP'} if float(row['open'])<=float(tp) else {'result':'SL'}
                            if h>=float(tp): return {'result':'TP'}
                            if l<=float(sl): return {'result':'SL'}
                        else:
                            if l<=float(tp) and h>=float(sl):
                                return {'result':'TP'} if float(row['open'])>=float(tp) else {'result':'SL'}
                            if l<=float(tp): return {'result':'TP'}
                            if h>=float(sl): return {'result':'SL'}
                    return {'result':'OPEN'}
                except Exception as e:
                    return {'result':'ERROR','error':str(e)[:60]}

            total_ext=len(df_ext_sub)
            for i,(idx,row) in enumerate(df_ext_sub.iterrows()):
                ext_status.text(f"Backtesting {i+1}/{total_ext}: {row.get('symbol','?')} {row.get('type','')}...")
                try:
                    r=asyncio.run(_ext_bt_fetch(
                        str(row.get('symbol','')),
                        str(row.get('exchange','OKX')),
                        row.get('ts'),
                        row.get('tp',0),
                        row.get('sl',0),
                        row.get('type','LONG'),
                        ext_candles))
                    if r:
                        for col in df_ext_sub.columns:
                            r[col]=row.get(col,'')
                        ext_results.append(r)
                except Exception as e:
                    err_row={'result':'ERROR','error':str(e)[:50]}
                    for col in df_ext_sub.columns:
                        err_row[col]=row.get(col,'')
                    ext_results.append(err_row)
                ext_prog.progress((i+1)/total_ext)

            ext_status.empty(); ext_prog.empty()
            if not ext_results:
                st.warning("No results."); st.stop()

            df_ext_res=pd.DataFrame(ext_results)
            etp=len(df_ext_res[df_ext_res['result']=='TP'])
            esl=len(df_ext_res[df_ext_res['result']=='SL'])
            eop=len(df_ext_res[df_ext_res['result']=='OPEN'])
            eer=len(df_ext_res[df_ext_res['result']=='ERROR'])
            eclosed=etp+esl
            ewr=(etp/eclosed*100) if eclosed>0 else 0

            # Stat strip
            st.markdown(f"""<div class="stat-strip">
              <div><div class="ss-val">{len(df_ext_res)}</div><div class="ss-lbl">Total</div></div>
              <div><div class="ss-val" style="color:var(--green);">{etp}</div><div class="ss-lbl">TP Hits</div></div>
              <div><div class="ss-val" style="color:var(--red);">{esl}</div><div class="ss-lbl">SL Hits</div></div>
              <div><div class="ss-val" style="color:var(--blue);">{ewr:.1f}%</div><div class="ss-lbl">Win Rate</div></div>
              <div><div class="ss-val" style="color:var(--amber);">{eop}</div><div class="ss-lbl">Still Open</div></div>
              <div><div class="ss-val" style="color:var(--muted);">{eer}</div><div class="ss-lbl">Errors</div></div>
            </div>""",unsafe_allow_html=True)

            st.markdown("---")
            ec1,ec2,ec3=st.columns(3)

            # By direction
            with ec1:
                st.markdown("#### 📊 By Direction")
                tr=[]
                for tn in ['LONG','SHORT']:
                    sub=df_ext_res[df_ext_res['type']==tn]
                    if sub.empty: continue
                    t=len(sub[sub['result']=='TP']); s=len(sub[sub['result']=='SL'])
                    c=t+s; w=(t/c*100) if c>0 else 0
                    tr.append({'Direction':tn,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
                st.dataframe(pd.DataFrame(tr),use_container_width=True,hide_index=True) if tr else st.info("No data")

            # By exchange
            with ec2:
                st.markdown("#### 🏦 By Exchange")
                er=[]
                for en in ['OKX','GATE','MEXC']:
                    sub=df_ext_res[df_ext_res['exchange']==en]
                    if sub.empty: continue
                    t=len(sub[sub['result']=='TP']); s=len(sub[sub['result']=='SL'])
                    c=t+s; w=(t/c*100) if c>0 else 0
                    er.append({'Exchange':en,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
                st.dataframe(pd.DataFrame(er),use_container_width=True,hide_index=True) if er else st.info("No data")

            # By class if available
            with ec3:
                if 'class' in df_ext_res.columns:
                    st.markdown("#### 🏷️ By Class")
                    cr=[]
                    for cn in ['squeeze','breakout','whale_driven','early']:
                        sub=df_ext_res[df_ext_res['class']==cn]
                        if sub.empty: continue
                        t=len(sub[sub['result']=='TP']); s=len(sub[sub['result']=='SL'])
                        c=t+s; w=(t/c*100) if c>0 else 0
                        cr.append({'Class':cn,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
                    st.dataframe(pd.DataFrame(cr),use_container_width=True,hide_index=True) if cr else st.info("No data")
                else:
                    st.markdown("#### 🏆 Best Coins")
                    coin_r=[]
                    for coin in df_ext_res['symbol'].unique():
                        sub=df_ext_res[df_ext_res['symbol']==coin]
                        if len(sub)<2: continue
                        t=len(sub[sub['result']=='TP']); s=len(sub[sub['result']=='SL'])
                        c=t+s; w=(t/c*100) if c>0 else 0
                        coin_r.append({'Coin':coin,'Signals':len(sub),'TP':t,'SL':s,'Win%':w})
                    if coin_r:
                        df_cr=pd.DataFrame(coin_r).sort_values('Win%',ascending=False).head(10)
                        df_cr['Win%']=df_cr['Win%'].apply(lambda x:f"{x:.1f}%")
                        st.dataframe(df_cr,use_container_width=True,hide_index=True)
                    else:
                        st.info("Need 2+ signals per coin")

            st.markdown("---")
            st.markdown("#### 📋 Full Results — All Data")
            st.dataframe(df_ext_res,use_container_width=True,height=400)
            st.download_button(
                "⬇️ Export External Backtest CSV",
                df_ext_res.to_csv(index=False).encode(),
                file_name=f"APEX5_external_backtest_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv")

        st.stop()

    # ══════════════════════════════════════════════════════════════════════
    # MODE 1 — SYSTEM JOURNAL (original backtest — unchanged below)
    # ══════════════════════════════════════════════════════════════════════
    st.info("📊 **How it works:** Fetches OHLCV candles from signal timestamp and scans candle HIGH/LOW to find which hits first — TP or SL. 100% accurate regardless of when you run it.")

    if not os.path.exists(JOURNAL_FILE):
        st.warning("No journal file found. Run some scans first."); st.stop()
    try: df_bt=pd.read_csv(JOURNAL_FILE)
    except: df_bt=pd.DataFrame()
    if df_bt.empty:
        st.warning("Journal is empty — run scans first."); st.stop()

    # ── FILTERS ──────────────────────────────────────────────────────────
    bt1,bt2,bt3,bt4,bt5=st.columns(5)
    with bt1: bt_min_sc=st.slider("Min score",1,100,int(S.get('backtest_min_score',50)))
    with bt2: bt_days=st.slider("Days back",1,90,int(S.get('backtest_days',30)))
    with bt3: bt_cls=st.selectbox("Class filter",["ALL","squeeze","breakout","whale_driven","early"])
    with bt4: bt_type=st.selectbox("Type filter",["ALL","LONG","SHORT"])
    with bt5: bt_candles=st.slider("Max candles per signal",50,500,200,step=50)

    if st.button("▶️ RUN BACKTEST",use_container_width=True):
        df_bt['ts']=pd.to_datetime(df_bt['ts'],errors='coerce')
        cutoff=datetime.now()-timedelta(days=bt_days)
        mask=(df_bt['ts']>=cutoff)&(pd.to_numeric(df_bt['pump_score'],errors='coerce')>=bt_min_sc)
        if bt_cls!="ALL" and 'class' in df_bt.columns: mask=mask&(df_bt['class']==bt_cls)
        if bt_type!="ALL": mask=mask&(df_bt['type']==bt_type)
        df_sub=df_bt[mask].copy()
        if df_sub.empty:
            st.warning("No signals match filters."); st.stop()

        st.info(f"Backtesting {len(df_sub)} signals — scanning candle HIGH/LOW for each...")
        results_bt=[]; prog=st.progress(0); status_ph=st.empty()

        async def _bt_fetch(sym, exch, ts_str, tp, sl, sig_type, entry_price, max_candles):
            try:
                from datetime import datetime as _dt
                ts_dt=_dt.fromisoformat(str(ts_str)) if ts_str else None
                if ts_dt is None: return None
                ts_ms=int(ts_dt.timestamp()*1000)
                tf=S.get('fast_tf','15m')
                if exch=="OKX":
                    ex=ccxt.okx({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                elif exch=="GATE":
                    ex=ccxt.gateio({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                else:
                    ex=ccxt.mexc({'enableRateLimit':True,'rateLimit':100,'timeout':8000,'options':{'defaultType':'swap'}})
                await ex.load_markets()
                sym_ccxt=f"{sym}/USDT:USDT"
                raw=await ex.fetch_ohlcv(sym_ccxt,tf,since=ts_ms,limit=max_candles)
                await ex.close()
                if not raw or len(raw)<3: return None
                df_r=pd.DataFrame(raw,columns=['ts','open','high','low','close','volume'])

                # Scan every candle HIGH and LOW — never miss an intracandle hit
                for idx,row in df_r.iterrows():
                    h=float(row['high']); l=float(row['low'])
                    if sig_type=="LONG":
                        if h>=float(tp) and l<=float(sl):
                            if float(row['open'])<=float(tp): return {'result':'TP'}
                            else: return {'result':'SL'}
                        if h>=float(tp): return {'result':'TP'}
                        if l<=float(sl): return {'result':'SL'}
                    else:
                        if l<=float(tp) and h>=float(sl):
                            if float(row['open'])>=float(tp): return {'result':'TP'}
                            else: return {'result':'SL'}
                        if l<=float(tp): return {'result':'TP'}
                        if h>=float(sl): return {'result':'SL'}
                return {'result':'OPEN'}
            except Exception as e:
                return {'result':'ERROR','error':str(e)[:60]}

        total=len(df_sub)
        for i,(idx,row) in enumerate(df_sub.iterrows()):
            status_ph.text(f"Backtesting {i+1}/{total}: {row.get('symbol','?')} {row.get('type','')} score:{row.get('pump_score',0)}...")
            try:
                r=asyncio.run(_bt_fetch(
                    str(row.get('symbol','')),str(row.get('exchange','OKX')),
                    row.get('ts'),row.get('tp',0),row.get('sl',0),
                    row.get('type','LONG'),row.get('price',0),bt_candles))
                if r:
                    # Copy ALL journal columns into result row
                    for col in df_sub.columns:
                        r[col]=row.get(col,'')
                    results_bt.append(r)
            except Exception as e:
                err_row={'result':'ERROR','error':str(e)[:50]}
                for col in df_sub.columns:
                    err_row[col]=row.get(col,'')
                results_bt.append(err_row)
            prog.progress((i+1)/total)

        status_ph.empty(); prog.empty()
        if not results_bt:
            st.warning("No backtest results."); st.stop()

        df_res=pd.DataFrame(results_bt)
        tp_c=len(df_res[df_res['result']=='TP'])
        sl_c=len(df_res[df_res['result']=='SL'])
        op_c=len(df_res[df_res['result']=='OPEN'])
        er_c=len(df_res[df_res['result']=='ERROR'])
        closed=tp_c+sl_c
        wr=(tp_c/closed*100) if closed>0 else 0

        # ── OVERALL STAT STRIP ────────────────────────────────────────────
        st.markdown(f"""<div class="stat-strip">
          <div><div class="ss-val">{len(df_res)}</div><div class="ss-lbl">Total</div></div>
          <div><div class="ss-val" style="color:var(--green);">{tp_c}</div><div class="ss-lbl">TP Hits</div></div>
          <div><div class="ss-val" style="color:var(--red);">{sl_c}</div><div class="ss-lbl">SL Hits</div></div>
          <div><div class="ss-val" style="color:var(--blue);">{wr:.1f}%</div><div class="ss-lbl">Win Rate</div></div>
          <div><div class="ss-val" style="color:var(--amber);">{op_c}</div><div class="ss-lbl">Still Open</div></div>
          <div><div class="ss-val" style="color:var(--muted);">{er_c}</div><div class="ss-lbl">Errors</div></div>
        </div>""",unsafe_allow_html=True)

        st.markdown("---")

        col_a,col_b,col_c=st.columns(3)

        # ── BREAKDOWN BY SCORE RANGE ──────────────────────────────────────
        with col_a:
            st.markdown("#### 🎯 By Score Range")
            score_ranges=[(90,100,"90-100"),(75,89,"75-89"),(60,74,"60-74"),(45,59,"45-59"),(1,44,"<45")]
            sr_rows=[]
            for lo,hi,label in score_ranges:
                sub=df_res[(pd.to_numeric(df_res['pump_score'],errors='coerce')>=lo)&(pd.to_numeric(df_res['pump_score'],errors='coerce')<=hi)]
                if sub.empty: continue
                t=len(sub[sub['result']=='TP'])
                s=len(sub[sub['result']=='SL'])
                c=t+s; w=(t/c*100) if c>0 else 0
                sr_rows.append({'Score':label,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
            if sr_rows:
                st.dataframe(pd.DataFrame(sr_rows),use_container_width=True,hide_index=True)
            else:
                st.info("No data")

        # ── BREAKDOWN BY CLASS ────────────────────────────────────────────
        with col_b:
            st.markdown("#### 🏷️ By Signal Class")
            cls_rows=[]
            for cls_name in ['squeeze','breakout','whale_driven','early']:
                sub=df_res[df_res.get('class',df_res.get('cls',''))==cls_name] if 'class' in df_res.columns else df_res[df_res.get('cls','')==cls_name]
                if sub.empty: continue
                t=len(sub[sub['result']=='TP'])
                s=len(sub[sub['result']=='SL'])
                c=t+s; w=(t/c*100) if c>0 else 0
                cls_rows.append({'Class':cls_name,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
            if cls_rows:
                st.dataframe(pd.DataFrame(cls_rows),use_container_width=True,hide_index=True)
            else:
                st.info("No data")

        # ── BREAKDOWN BY DIRECTION ────────────────────────────────────────
        with col_c:
            st.markdown("#### 📊 By Direction")
            type_rows=[]
            for t_name in ['LONG','SHORT']:
                sub=df_res[df_res['type']==t_name]
                if sub.empty: continue
                t=len(sub[sub['result']=='TP'])
                s=len(sub[sub['result']=='SL'])
                c=t+s; w=(t/c*100) if c>0 else 0
                type_rows.append({'Direction':t_name,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
            if type_rows:
                st.dataframe(pd.DataFrame(type_rows),use_container_width=True,hide_index=True)
            else:
                st.info("No data")

        st.markdown("---")

        col_d,col_e=st.columns(2)

        # ── BREAKDOWN BY EXCHANGE ─────────────────────────────────────────
        with col_d:
            st.markdown("#### 🏦 By Exchange")
            ex_rows=[]
            for ex_name in ['OKX','GATE','MEXC']:
                sub=df_res[df_res['exchange']==ex_name]
                if sub.empty: continue
                t=len(sub[sub['result']=='TP'])
                s=len(sub[sub['result']=='SL'])
                c=t+s; w=(t/c*100) if c>0 else 0
                ex_rows.append({'Exchange':ex_name,'Signals':len(sub),'TP':t,'SL':s,'Win%':f"{w:.1f}%"})
            if ex_rows:
                st.dataframe(pd.DataFrame(ex_rows),use_container_width=True,hide_index=True)
            else:
                st.info("No data")

        # ── BEST PERFORMING COINS ─────────────────────────────────────────
        with col_e:
            st.markdown("#### 🏆 Best Coins (min 2 signals)")
            coin_rows=[]
            sym_col='symbol' if 'symbol' in df_res.columns else 'sym'
            if sym_col in df_res.columns:
                for coin in df_res[sym_col].unique():
                    sub=df_res[df_res[sym_col]==coin]
                    if len(sub)<2: continue
                    t=len(sub[sub['result']=='TP'])
                    s=len(sub[sub['result']=='SL'])
                    c=t+s; w=(t/c*100) if c>0 else 0
                    coin_rows.append({'Coin':coin,'Signals':len(sub),'TP':t,'SL':s,'Win%':w})
            if coin_rows:
                df_coin=pd.DataFrame(coin_rows).sort_values('Win%',ascending=False).head(10)
                df_coin['Win%']=df_coin['Win%'].apply(lambda x:f"{x:.1f}%")
                st.dataframe(df_coin,use_container_width=True,hide_index=True)
            else:
                st.info("Need 2+ signals per coin")

        st.markdown("---")

        # ── FULL RESULTS TABLE — ALL COLUMNS ─────────────────────────────
        st.markdown("#### 📋 All Results — Full Data")
        st.dataframe(df_res,use_container_width=True,height=400)
        st.download_button(
            "⬇️ Export Full Backtest CSV",
            df_res.to_csv(index=False).encode(),
            file_name=f"APEX5_backtest_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv")

    st.stop()

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: CATALYST INTELLIGENCE
# ═══════════════════════════════════════════════════════════════════════════
if nav=="🧠 Catalyst":
    st.markdown('<div class="section-h">🧠 Catalyst Intelligence — AI Narrative Scanner</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-family:monospace;font-size:.62rem;color:#64748b;margin-bottom:12px;">Powered by Groq LLaMA 3.1 · Identifies narrative-driven pump catalysts · Cross-validates with APEX5 signals</div>', unsafe_allow_html=True)

    groq_key = S.get('groq_key','') or "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"

    cat_col1, cat_col2, cat_col3 = st.columns([2,2,4])
    with cat_col1:
        cat_run = st.button("▶ Run Catalyst Scan", key='cat_run')
    with cat_col2:
        cat_auto = st.toggle("Auto every 30 min", st.session_state.get('cat_auto', False), key='cat_auto_toggle')
        st.session_state['cat_auto'] = cat_auto
    with cat_col3:
        cat_last = st.session_state.get('cat_last_update','–')
        st.markdown(f'<div style="font-family:monospace;font-size:.6rem;color:#64748b;padding-top:8px;">Last run: {cat_last}</div>', unsafe_allow_html=True)

    cat_last_ts = st.session_state.get('cat_last_ts', 0)
    cat_should_run = cat_run or cat_last_ts == 0 or (cat_auto and time.time() - cat_last_ts >= 1800)

    if cat_should_run:
        with st.spinner("🧠 Fetching news and analyzing catalysts..."):
            try:
                # ── Fetch news from CryptoPanic ───────────────────────
                import urllib.request, json as _json
                news_url = "https://cryptopanic.com/api/free/v1/posts/?auth_token=free&kind=news&public=true"
                try:
                    with urllib.request.urlopen(news_url, timeout=10) as r:
                        news_data = _json.loads(r.read())
                    headlines = []
                    for item in news_data.get('results', [])[:30]:
                        title = item.get('title','')
                        currencies = [c['code'] for c in item.get('currencies',[])]
                        if title:
                            headlines.append({'title': title, 'coins': currencies})
                except:
                    headlines = [{'title': 'No news fetched — using APEX5 signal coins for analysis', 'coins': []}]

                # ── Get current APEX5 signal coins ─────────────────────
                APEX5_coins = list(set([
                    r['symbol'].replace('/USDT:USDT','').replace('/USDT','')
                    for r in st.session_state.get('results', [])
                ] + [
                    s['symbol'].replace('/USDT:USDT','').replace('/USDT','')
                    for s in st.session_state.get('gl_results', [])
                ]))[:20]

                # ── Build prompt ──────────────────────────────────────
                news_text = "\n".join([f"- {h['title']} {('(' + ', '.join(h['coins']) + ')') if h['coins'] else ''}" for h in headlines[:25]])
                APEX5_text = ", ".join(APEX5_coins) if APEX5_coins else "BTC, ETH, SOL, BNB, ARB"

                prompt = f"""You are a Senior Crypto Market Analyst specializing in Catalyst-Driven Volatility.

RECENT NEWS HEADLINES:
{news_text}

COINS CURRENTLY IN APEX5 SCANNER: {APEX5_text}

ANALYSIS TASKS:
1. Identify High-Impact Catalysts: Flag mentions of structural changes, major ecosystem expansions, strategic partnerships, exchange listings, governance votes, whale movements.
2. Sentiment Divergence: Find coins where social chatter is spiking but price has not moved yet.
3. Red Flag Filtering: Filter out paid shills, pump and dump bot spam, low-quality altcoin news without concrete fundamental drivers.

Respond ONLY with a JSON array of up to 8 coins. No preamble, no markdown, no explanation. Just the raw JSON array:
[
  {{
    "symbol": "ACX",
    "pump_probability": "High",
    "primary_catalyst": "Brief description of specific news or event",
    "timeframe": "Immediate (0-4h)",
    "sentiment_score": 85,
    "red_flag": false,
    "red_flag_reason": "",
    "APEX5_match": true
  }}
]

pump_probability must be exactly: High, Medium, Low, or Filtered
timeframe must be exactly: Immediate (0-4h), Short-term (1-3 days), or Mid-term (1 week+)
APEX5_match should be true if the coin symbol appears in: {APEX5_text}
red_flag should be true only for clear shill/spam/pump-and-dump patterns"""

                # ── Call Groq API ─────────────────────────────────────
                import urllib.request, urllib.error
                groq_payload = _json.dumps({
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1500,
                    "temperature": 0.3
                }).encode('utf-8')

                groq_req = urllib.request.Request(
                    "https://api.groq.com/openai/v1/chat/completions",
                    data=groq_payload,
                    headers={
                        "Authorization": f"Bearer {groq_key}",
                        "Content-Type": "application/json"
                    }
                )
                with urllib.request.urlopen(groq_req, timeout=30) as r:
                    groq_data = _json.loads(r.read())

                raw_content = groq_data['choices'][0]['message']['content'].strip()
                raw_content = raw_content.replace('```json','').replace('```','').strip()
                catalyst_results = _json.loads(raw_content)

                st.session_state['cat_results'] = catalyst_results
                _utc, _pkt = _dual_time()
                st.session_state['cat_last_update'] = _pkt
                st.session_state['cat_last_ts'] = time.time()

            except Exception as ce:
                st.error(f"Catalyst scan error: {ce}")

    cat_results = st.session_state.get('cat_results', [])

    if cat_results:
        # ── Summary metrics ───────────────────────────────────────────
        c_high   = len([c for c in cat_results if c.get('pump_probability') == 'High'])
        c_med    = len([c for c in cat_results if c.get('pump_probability') == 'Medium'])
        c_low    = len([c for c in cat_results if c.get('pump_probability') == 'Low'])
        c_flag   = len([c for c in cat_results if c.get('red_flag')])

        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("High probability", c_high)
        sm2.metric("Medium probability", c_med)
        sm3.metric("Low probability", c_low)
        sm4.metric("Red flags filtered", c_flag)

        st.markdown("---")

        # ── Signal cards ──────────────────────────────────────────────
        prob_colors = {
            'High':     ('#f0fdf4','#059669','#059669'),
            'Medium':   ('#fffbeb','#d97706','#d97706'),
            'Low':      ('#f8fafc','#64748b','#94a3b8'),
            'Filtered': ('#fef2f2','#dc2626','#dc2626'),
        }

        card_cols = st.columns(2)
        for i, coin in enumerate(cat_results):
            prob = coin.get('pump_probability','Low')
            bg, border, text = prob_colors.get(prob, prob_colors['Low'])
            score = int(coin.get('sentiment_score', 0))
            bar_w = score
            APEX5_match = coin.get('APEX5_match', False)
            red_flag = coin.get('red_flag', False)
            sym = coin.get('symbol','–')
            catalyst = coin.get('primary_catalyst','–')
            timeframe = coin.get('timeframe','–')
            red_reason = coin.get('red_flag_reason','')

            APEX5_badge = f'<span style="font-family:monospace;font-size:11px;padding:2px 8px;border-radius:4px;background:#f0fdf4;color:#059669;border:0.5px solid #059669;">APEX5 match active</span>' if APEX5_match else '<span style="font-family:monospace;font-size:11px;color:#94a3b8;">No active APEX5 signal</span>'
            flag_badge = f'<span style="font-family:monospace;font-size:11px;padding:2px 8px;border-radius:4px;background:#fef2f2;color:#dc2626;border:0.5px solid #dc2626;">{red_reason}</span>' if red_flag else ''

            with card_cols[i % 2]:
                st.markdown(f'''<div style="background:{bg};border:0.5px solid {border};border-radius:10px;padding:14px 16px;margin-bottom:10px;">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
                  <div style="display:flex;align-items:center;gap:8px;">
                    <span style="font-family:monospace;font-size:14px;font-weight:700;color:{text};">{sym}</span>
                    <span style="font-family:monospace;font-size:11px;padding:2px 8px;border-radius:4px;background:{bg};color:{text};border:0.5px solid {border};">{prob}</span>
                  </div>
                  <span style="font-family:monospace;font-size:14px;font-weight:700;color:{text};">{score}/100</span>
                </div>
                <div style="height:4px;background:#e2e8f0;border-radius:2px;margin-bottom:10px;">
                  <div style="height:4px;width:{bar_w}%;background:{border};border-radius:2px;"></div>
                </div>
                <div style="font-family:monospace;font-size:12px;color:#475569;margin-bottom:8px;">{catalyst}</div>
                <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;">
                  <span style="font-family:monospace;font-size:11px;padding:2px 7px;border-radius:4px;background:#eff6ff;color:#0284c7;border:0.5px solid #bfdbfe;">{timeframe}</span>
                  {flag_badge}
                </div>
                <div style="border-top:0.5px solid #e2e8f0;padding-top:8px;">{APEX5_badge}</div>
                </div>''', unsafe_allow_html=True)

        # ── Cross-validation on APEX5 results ──────────────────────────
        st.markdown("---")
        st.markdown('<div style="font-family:monospace;font-size:.75rem;font-weight:700;color:#64748b;">Cross-validation — coins appearing in both APEX5 and Catalyst</div>', unsafe_allow_html=True)
        APEX5_confirmed = [c for c in cat_results if c.get('APEX5_match') and not c.get('red_flag') and c.get('pump_probability') in ['High','Medium']]
        if APEX5_confirmed:
            for coin in APEX5_confirmed:
                prob = coin.get('pump_probability','Medium')
                score = coin.get('sentiment_score',0)
                bg = '#f0fdf4' if prob == 'High' else '#fffbeb'
                border = '#059669' if prob == 'High' else '#d97706'
                text = '#059669' if prob == 'High' else '#d97706'
                st.markdown(f'''<div style="background:{bg};border:0.5px solid {border};border-radius:8px;padding:10px 14px;margin-bottom:6px;display:flex;align-items:center;justify-content:space-between;">
                <div>
                  <span style="font-family:monospace;font-size:12px;font-weight:700;color:{text};">Narrative confirmed — {coin["symbol"]} · {score}/100</span>
                  <div style="font-family:monospace;font-size:11px;color:{text};margin-top:2px;">{coin.get("primary_catalyst","–")} · {coin.get("timeframe","–")}</div>
                </div>
                <span style="font-family:monospace;font-size:11px;padding:2px 8px;border-radius:4px;background:white;color:{text};border:0.5px solid {border};">{coin["symbol"]}</span>
                </div>''', unsafe_allow_html=True)
        else:
            st.markdown('<div style="font-family:monospace;font-size:.65rem;color:#94a3b8;">No cross-validated signals yet — run APEX5 scan first then run Catalyst scan</div>', unsafe_allow_html=True)

    elif not cat_should_run:
        st.markdown('<div class="empty-st">🧠 Click ▶ Run Catalyst Scan to analyze current market narratives</div>', unsafe_allow_html=True)

    st.stop()

# ─── AUTO-JOURNAL CHECK ──────────────────────────────────────────────────────
# autocheck_journal_background(S)
check_daily_summary(S)

# ═══════════════════════════════════════════════════════════════════════════
# PAGE: SCANNER
# ═══════════════════════════════════════════════════════════════════════════
eff_s=S.copy()
eff_s.update({'scan_depth':q_depth,'min_score':q_min,'btc_filter':q_btc,'require_momentum':q_mom,
              'auto_scan':q_auto,'auto_interval':q_auto_int,'scan_modes':selected_modes})

col_btn,_=st.columns([2,5])
with col_btn: do_scan=st.button("⚡  RUN PUMP/DUMP SCAN",use_container_width=True)
if eff_s.get('auto_scan'): do_scan=True; time.sleep(0.3)

if do_scan:
    try:
        screener=PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''),okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''),okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''),gate_secret=eff_s.get('gate_secret',''))
        raw_results,btc_t,btc_p,btc_r,scan_errs=asyncio.run(screener.run(eff_s))
        st.session_state.last_raw_count=len(raw_results)
        st.session_state['last_coin_list']=[{'symbol':r['symbol'],'exchange':r.get('exchange','gate').lower()} for r in raw_results]
        st.session_state.prev_results={f"{r['symbol']}_{r['type']}":r['pump_score'] for r in st.session_state.results}
        st.session_state.results=[r for r in raw_results if r['pump_score']>=eff_s['min_score']]
        st.session_state.last_scan=datetime.now().strftime('%H:%M:%S')
        st.session_state.scan_count+=1
        st.session_state.btc_price=btc_p; st.session_state.btc_trend=btc_t
        try:
            _btc_1h = (btc_p - st.session_state.get('btc_price_prev_1h', btc_p)) / max(st.session_state.get('btc_price_prev_1h', btc_p), 1) * 100
            st.session_state['btc_1h_chg'] = round(_btc_1h, 2)
            if st.session_state.get('btc_price_prev_1h', 0) == 0:
                st.session_state['btc_price_prev_1h'] = btc_p
        except:
            st.session_state['btc_1h_chg'] = 0
        st.session_state.scan_errors=scan_errs

        # ── DUAL CONFIRM: mark coins flagged by both scanner + sentinel ───
        sentinel_syms={r['symbol'] for r in st.session_state.get('sentinel_results',[])}
        for r in st.session_state.results:
            prev=st.session_state.prev_results
            sym_key=f"{r['symbol']}_{r['type']}"
            r['is_new']=sym_key not in prev
            r['score_jump']=r['pump_score']-prev.get(sym_key,0) if prev.get(sym_key) else 0
            # DUAL CONFIRM logic: same coin in sentinel + scanner = score boost
            if r['symbol'] in sentinel_syms:
                r['dual_confirmed']=True
                r['pump_score']=min(100,r['pump_score']+10)
                if '🔥 DUAL CONFIRMED by Sentinel' not in r['reasons']:
                    r['reasons'].insert(0,'🔥 DUAL CONFIRMED by Sentinel — Scanner + Sentinel both flagged this coin (high conviction)')
            else:
                r['dual_confirmed']=False

        for r in st.session_state.results:
            ak=f"{r['symbol']}_{r['type']}_{datetime.now().hour}"
            lbl=pump_label(r['pump_score'],r['type'])
            score_bracket=r['pump_score']//15
            ak_recheck=f"{r['symbol']}_{r['type']}_{datetime.now().hour}_{score_bracket}"

            # ── JOURNAL LOGGING ──────────────────────────────────────────
            if ak not in st.session_state.logged_sigs:
                log_trade(r)  # log_trade handles all filter logic internally
                st.session_state.logged_sigs.add(ak)

            # ── NOTIFICATION CHECK ────────────────────────────────────────
            send_alert=False
            # ── Kill-Switch time filter ───────────────────────────────────
            current_utc_hour = datetime.now(timezone.utc).hour
            ks_on = eff_s.get('kill_switch_on', False)
            if ks_on:
                kill_zones = eff_s.get('kill_zones', [{"start":16,"end":19}])
                in_kill_zone = False
                for kz in kill_zones:
                    ks = int(kz.get('start', 16))
                    ke = int(kz.get('end', 19))
                    if ks <= ke:
                        if ks <= current_utc_hour < ke:
                            in_kill_zone = True; break
                    else:
                        if current_utc_hour >= ks or current_utc_hour < ke:
                            in_kill_zone = True; break
                if in_kill_zone:
                    continue
            if r['pump_score']>=eff_s.get('alert_min_score',60):
                type_ok=(r['type']=='LONG' and eff_s.get('alert_longs',True)) or (r['type']=='SHORT' and eff_s.get('alert_shorts',True))
                cls_ok=((r['cls']=='squeeze' and eff_s.get('alert_squeeze',True)) or
                        (r['cls']=='breakout' and eff_s.get('alert_breakout',True)) or
                        (r['cls']=='whale_driven' and eff_s.get('alert_whale',True)) or
                        (r['cls']=='early' and eff_s.get('alert_early',False)) or
                        r['cls'] not in ('squeeze','breakout','whale_driven','early'))
                if type_ok and cls_ok: send_alert=True
            # Force alert for dual-confirmed high-conviction signals
            if r.get('dual_confirmed') and r['pump_score']>=60: send_alert=True

            if send_alert:
                if 'alerted_sigs' not in st.session_state: st.session_state.alerted_sigs=set()
                if ak_recheck not in st.session_state.alerted_sigs:
                    bd_r=r.get('signal_breakdown',{})
                    sentiment=r.get('sentiment',{})
                    def epip(v,lo=5,hi=12):
                        if v>=hi: return "🟩"
                        if v>=lo: return "🟨"
                        return "⬛"
                    pips_str=(f"OB {epip(bd_r.get('ob_imbalance',0),4,14)} | FD {epip(bd_r.get('funding',0)+bd_r.get('funding_hist',0),3,15)} | OI {epip(bd_r.get('oi_spike',0),5,14)}\n"
                              f"VOL {epip(bd_r.get('vol_surge',0),3,10)} | LQ {epip(bd_r.get('liq_cluster',0),4,12)} | WH {epip(bd_r.get('whale_wall',0),4,7)} | SENT {epip(bd_r.get('sentiment',0),8,20)}\n"
                              f"MTF {epip(max(0,bd_r.get('mtf',0)),6,12)} | FLOW {epip(bd_r.get('orderflow',0),6,12)} | LISTING {epip(bd_r.get('listing',0),6,25)}")
                    # ALL reasons — not truncated
                    reasons_str="\n".join([f"▸ {rsn}" for rsn in r['reasons']])
                    sent_line=f"\n📊 {sentiment.get('source','Exchange')} L/S: {sentiment['top_long_pct']:.0f}% / {sentiment['top_short_pct']:.0f}% | Taker: {sentiment['taker_buy_pct']:.0f}%" if sentiment.get('available') else ""
                    mom_line="\n✅ MOMENTUM CONFIRMED" if r.get('momentum_confirmed') else ""
                    dual_line="\n🔥🔥 DUAL CONFIRMED — Scanner + Sentinel — HIGH CONVICTION 🔥🔥" if r.get('dual_confirmed') else ""
                    warn_line="\n⚠️ WARNINGS:\n"+"\n".join(r.get('warnings',[])) if r.get('warnings') else ""
                    _p=float(r.get('price') or 0); _t=float(r.get('tp') or 0); _s_=float(r.get('sl') or 0)
                    _tp1=float(r.get('tp1') or _t); _tp2=float(r.get('tp2') or _t); _tp3=float(r.get('tp3') or _t)
                    _elo=float(r.get('entry_lo') or _p); _ehi=float(r.get('entry_hi') or _p)
                    rr_ratio=f"{abs(_t-_p)/abs(_p-_s_):.1f}:1" if abs(_p-_s_)>0 else "N/A"
                    oi_ch=r.get('oi_change_6h',0); oi_line=f"\n📈 OI 6h: {oi_ch:+.1f}%" if abs(oi_ch)>=5 else ""
                    sig_line="📗 <b>LONG</b>" if r['type']=='LONG' else "📕 <b>SHORT</b>"
                    rsi_arrow={"RISING":"↑","FALLING":"↓","FLAT":"→"}.get(r.get('rsi_direction',''),"")
                    # Telegram
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        msg_tg=(f"{dual_line}\n" if dual_line else ""
                                +f'{sig_line} | <b>Score: {r["pump_score"]}/100</b> | {pump_label(r["pump_score"],r["type"])}\n'
                                +f'<b>═══ {r["symbol"]} — {r["cls"].upper()} ({r.get("exchange","MEXC")}) ═══</b>\n'
                                +f'RSI: {r.get("rsi",0):.1f}{rsi_arrow} | R:R: {rr_ratio}\n'
                                +f'📍 Entry Zone: ${_elo:.6f}–${_ehi:.6f}\n'
                                +f'🎯 TP1: ${_tp1:.6f} | TP2: ${_tp2:.6f} | TP3: ${_tp3:.6f}\n'
                                +f'🛑 SL: ${_s_:.6f}\n'
                                +f'{oi_line}{sent_line}{mom_line}{warn_line}\n\n'
                                +f'📊 Signals:\n{pips_str}\n\n'
                                +f'📝 All Reasons:\n{reasons_str}')
                        send_tg(eff_s['tg_token'],eff_s['tg_chat_id'],msg_tg)
                    # Discord
                    if eff_s.get('discord_webhook'):
                        dc_color=0x059669 if r['type']=='LONG' else 0xdc2626
                        sig_hdr=("📗 **LONG**" if r['type']=='LONG' else "📕 **SHORT**")+f" | Score: **{r['pump_score']}/100**"
                        _dst = r.get('dst_signal')
                        _dst_confirmed = r.get('dst_confirmed', False)
                        if _dst_confirmed and _dst:
                            dst_line = (f"\n🔵 **DEMA+ST CONFIRMED** ✅ | DIR: {_dst['direction']} | RSI: {_dst['rsi']} | VOL: {_dst['vol_ratio']}× | ST flip: {_dst['fresh_bars']} bars ago | R:R {_dst['rr']}")
                        elif _dst and not _dst_confirmed:
                            dst_line = (f"\n🔵 **DEMA+ST CONFLICT** ⚠️ | DEMA+ST says {_dst['direction']} (opposite to APEX5)")
                        elif not _dst:
                            dst_line = "\n🔵 **DEMA+ST** — no signal this candle"
                        else:
                            dst_line = ""
                        stats_text=(f"{dual_line}\n{sig_hdr}\n"
                                    f"**Exchange:** {r.get('exchange','MEXC')} | **RSI:** {r.get('rsi',0):.1f}{rsi_arrow} | **R:R:** {rr_ratio}\n"
                                    f"📍 **Entry Zone:** `${_elo:.6f}`–`${_ehi:.6f}`\n"
                                    f"🎯 **TP1:** `${_tp1:.6f}` | **TP2:** `${_tp2:.6f}` | **TP3:** `${_tp3:.6f}`\n"
                                    f"🛑 **SL:** `${_s_:.6f}`"
                                    +dst_line
                                    +(f"\n**OI 6h:** {oi_ch:+.1f}%" if abs(oi_ch)>=5 else "")
                                    +(f"\n{sentiment.get('source','')} L/S: {sentiment['top_long_pct']:.0f}% / {sentiment['top_short_pct']:.0f}% | Taker: {sentiment['taker_buy_pct']:.0f}%" if sentiment.get('available') else "")
                                    +(f"\n⚠️ "+' | '.join(r.get('warnings',[])) if r.get('warnings') else ""))
                        send_discord(eff_s['discord_webhook'],{
                            'title':f'{r["symbol"]} ({r["cls"].upper()}) — {r.get("exchange","MEXC")}',
                            'color':dc_color,
                            'description':stats_text,
                            'fields':[
                                {'name':'📊 Signal Breakdown','value':pips_str,'inline':False},
                                {'name':f'📝 All Reasons ({len(r["reasons"])})','value':reasons_str[:1024],'inline':False},
                                *([{'name':'📝 Reasons (continued)','value':reasons_str[1024:2048],'inline':False}] if len(reasons_str)>1024 else [])
                            ],
                            'footer':{'text':f'APEX5 Intelligence Terminal • {_dual_time()[0]} | {_dual_time()[1]}'}
                        })
                    st.session_state.alerted_sigs.add(ak_recheck)
                    _ch=[c for c,v in [('Discord',eff_s.get('discord_webhook')),('Telegram',eff_s.get('tg_token') and eff_s.get('tg_chat_id'))] if v]
                    _dual_note=' 🔥DUAL' if r.get('dual_confirmed') else ''
                    st.toast(f'📨{_dual_note} {r["symbol"]} → {"+".join(_ch) if _ch else "no channels configured"}',icon='📨' if _ch else '⚠️')
    except Exception as e:
        if any(x in str(e) for x in ["510","429","too frequent"]): st.error("🚦 Rate limited — wait 60s")
        else: st.error(f"Error: {e}")


# ─── SENTINEL RUNNER ─────────────────────────────────────────────────────────
if st.session_state.get('sentinel_active') and do_scan and st.session_state.scan_count>0:
    sent_ph=st.empty()
    sent_ph.markdown('<div style="background:#0f1117;border-radius:8px;padding:10px 18px;margin-bottom:10px;border:1px solid #7c3aed44;"><span style="color:#7c3aed;font-family:monospace;font-size:.65rem;font-weight:700;">🛰️ SENTINEL — TOP 100 SCANNING…</span></div>',unsafe_allow_html=True)
    try:
        try:
            import nest_asyncio as _na
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            try:
                _loop=asyncio.get_event_loop()
                if _loop.is_closed(): _loop=asyncio.new_event_loop(); asyncio.set_event_loop(_loop)
            except RuntimeError:
                _loop=asyncio.new_event_loop(); asyncio.set_event_loop(_loop)
            _na.apply(_loop)
        except: pass

        s_screener=PrePumpScreener(
            cmc_key=eff_s.get('cmc_key',''),okx_key=eff_s.get('okx_key',''),
            okx_secret=eff_s.get('okx_secret',''),okx_passphrase=eff_s.get('okx_passphrase',''),
            gate_key=eff_s.get('gate_key',''),gate_secret=eff_s.get('gate_secret',''))

        async def sentinel_tick(screener, s):
            btc_trend,btc_px,_=await screener.fetch_btc()
            await asyncio.gather(screener.okx.load_markets(),screener.mexc.load_markets(),screener.gate.load_markets(),return_exceptions=True)
            tk_okx,tk_mexc,tk_gate={},{},{}
            res_tk=await asyncio.gather(screener.okx.fetch_tickers(),screener.mexc.fetch_tickers(),screener.gate.fetch_tickers(),return_exceptions=True)
            if not isinstance(res_tk[0],Exception): tk_okx=res_tk[0]
            if not isinstance(res_tk[1],Exception): tk_mexc=res_tk[1]
            if not isinstance(res_tk[2],Exception): tk_gate=res_tk[2]
            all_swaps={}
            for sym,t in {**tk_mexc,**tk_gate,**tk_okx}.items():
                if sym.endswith(':USDT') and t.get('quoteVolume'):
                    exn='OKX' if sym in tk_okx else ('GATE' if sym in tk_gate else 'MEXC')
                    exo=screener.okx if exn=='OKX' else (screener.gate if exn=='GATE' else screener.mexc)
                    all_swaps[sym]={'vol':float(t['quoteVolume'] or 0),'exch_name':exn,'exch_obj':exo}
            sorted_swaps=sorted(all_swaps.items(),key=lambda x:x[1]['vol'],reverse=True)[:100]
            uni_size=len(sorted_swaps)
            checked=st.session_state.get('sentinel_total_checked',0)
            batch_sz=max(1,s.get('sentinel_batch_size',5))
            start=checked%max(1,uni_size); end=min(start+batch_sz,uni_size)
            coin_batch=sorted_swaps[start:end]
            new_sigs=[]
            for sym,data in coin_batch:
                try:
                    r=await screener.analyze(data['exch_name'],data['exch_obj'],sym,s,btc_trend)
                    if r and r['pump_score']>=s.get('sentinel_score_threshold',70): new_sigs.append(r)
                except: pass
            for exch in [screener.okx,screener.mexc,screener.gate]:
                try: await exch.close()
                except: pass
            return new_sigs,btc_px,uni_size

        new_sigs,s_btc,total_uni=asyncio.run(sentinel_tick(s_screener,eff_s))
        st.session_state.sentinel_total_checked=st.session_state.get('sentinel_total_checked',0)+eff_s.get('sentinel_batch_size',5)
        st.session_state.sentinel_last_check=datetime.now().strftime('%H:%M:%S')
        st.session_state.sentinel_universe_size=f"TOP {total_uni}"
        st.session_state.btc_price=s_btc

        if new_sigs:
            st.session_state.sentinel_signals_found=st.session_state.get('sentinel_signals_found',0)+len(new_sigs)
            if 'alerted_sigs' not in st.session_state: st.session_state.alerted_sigs=set()
            for r in new_sigs:
                st.toast(f"🛰️ {r['symbol']} — {r['pump_score']}/100 {r['type']}",icon="🚨")
                ak=f"{r['symbol']}_{r['type']}_{datetime.now().strftime('%Y%m%d%H')}_sent"
                if 'sentinel_results' not in st.session_state: st.session_state.sentinel_results=[]
                sent_syms={x['symbol'] for x in st.session_state.sentinel_results}
                if r['symbol'] not in sent_syms:
                    st.session_state.sentinel_results.insert(0,r)
                    # FIX: Sentinel signals go to journal too
                    log_trade(r)
                if ak not in st.session_state.alerted_sigs:
                    _p=float(r.get('price') or 0); _t=float(r.get('tp') or 0); _s_=float(r.get('sl') or 0)
                    rr_r=f"{abs(_t-_p)/abs(_p-_s_):.1f}:1" if abs(_p-_s_)>0 else "N/A"
                    # All reasons for sentinel alert too
                    rsns="\n".join([f"• {x}" for x in r['reasons']])
                    sig_line_sent="📗 LONG" if r['type']=='LONG' else "📕 SHORT"
                    _tp1=float(r.get('tp1') or _t); _tp2=float(r.get('tp2') or _t); _tp3=float(r.get('tp3') or _t)
                    _elo=float(r.get('entry_lo') or _p); _ehi=float(r.get('entry_hi') or _p)
                    if eff_s.get('tg_token') and eff_s.get('tg_chat_id'):
                        send_tg(eff_s['tg_token'],eff_s['tg_chat_id'],
                            f"<b>🛰️ [SENTINEL] {sig_line_sent} | Score: {r['pump_score']}/100</b>\n"
                            f"<b>{r['symbol']} — {r.get('cls','').upper()} | {r.get('exchange','')}</b>\n"
                            f"R:R {rr_r}\n"
                            f"📍 Entry Zone: ${_elo:.6f}–${_ehi:.6f}\n"
                            f"TP1: ${_tp1:.6f} | TP2: ${_tp2:.6f} | TP3: ${_tp3:.6f}\n"
                            f"🛑 SL: ${_s_:.6f}\n\n"
                            f"📝 All Reasons:\n{rsns}")
                    if eff_s.get('discord_webhook'):
                        dc_color_s=0x059669 if r['type']=='LONG' else 0xdc2626
                        send_discord(eff_s['discord_webhook'],{
                            "title":f"🛰️ SENTINEL: {r['symbol']} ({r['pump_score']}/100)",
                            "color":dc_color_s,
                            "description":(f"{'📗 **LONG**' if r['type']=='LONG' else '📕 **SHORT**'} | Score: **{r['pump_score']}/100**\n"
                                           f"**Exchange:** {r.get('exchange','')} | **Class:** {r.get('cls','').upper()} | **R:R:** {rr_r}\n"
                                           f"📍 Entry Zone: `${_elo:.6f}`–`${_ehi:.6f}`\n"
                                           f"TP1: `${_tp1:.6f}` | TP2: `${_tp2:.6f}` | TP3: `${_tp3:.6f}`\n"
                                           f"🛑 SL: `${_s_:.6f}`"),
                            "fields":[{'name':f'📝 All Reasons ({len(r["reasons"])})','value':"\n".join([f"▸ {x}" for x in r['reasons']])[:1024],'inline':False}],
                            "footer":{"text":f"APEX5 Sentinel — Top 100 | {_dual_time()[0]} | {_dual_time()[1]}"}})
                    st.session_state.alerted_sigs.add(ak)

        chk_f=st.session_state.get('sentinel_total_checked',0)
        sig_f=st.session_state.get('sentinel_signals_found',0)
        pct_done=int((chk_f%max(1,total_uni))/max(1,total_uni)*100)
        sent_ph.markdown(
            f'<div style="background:#0f1117;border-radius:8px;padding:10px 18px;margin-bottom:10px;border:1px solid #7c3aed66;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">'
            f'<span style="color:#7c3aed;font-family:monospace;font-size:.65rem;font-weight:700;">🛰️ SENTINEL LIVE — TOP {total_uni}</span>'
            f'<span style="font-family:monospace;font-size:.6rem;color:#9ca3af;">{chk_f} checked ({pct_done}% cycle)</span>'
            f'<span style="font-family:monospace;font-size:.62rem;color:#ff4444;font-weight:700;">{sig_f} signals found</span>'
            f'<span style="font-family:monospace;font-size:.58rem;color:#9ca3af;">Last: {st.session_state.get("sentinel_last_check","?")}</span>'
            f'</div><div style="height:3px;background:#1f2937;border-radius:2px;margin-top:6px;">'
            f'<div style="width:{pct_done}%;height:100%;background:#7c3aed;border-radius:2px;"></div></div></div>',
            unsafe_allow_html=True)
    except Exception as e:
        import traceback
        st.session_state.sentinel_active=False
        st.error(f"Sentinel error (auto-disabled): {e}")
        st.code(traceback.format_exc(),language="text")


# ─── RESULTS DISPLAY ─────────────────────────────────────────────────────────
results=st.session_state.results
errs=getattr(st.session_state,'scan_errors',[])
btc_t=getattr(st.session_state,'btc_trend',"NEUTRAL")

if not results:
    st.markdown('<div class="empty-st"><div style="font-size:2.5rem;opacity:.2;margin-bottom:12px;">🔥</div>Run a scan — or lower Min Score in sidebar</div>',unsafe_allow_html=True)
    if st.session_state.scan_count>0:
        with st.expander("🔍 Debug Info",expanded=True):
            raw=st.session_state.last_raw_count
            st.markdown(f"""**Scan #{st.session_state.scan_count}** at **{st.session_state.last_scan}**
| Check | Status |
|---|---|
| Raw coins found | **{raw}** |
| Min score filter | **{eff_s['min_score']}** — set to 1 to see everything |
| Min R:R | **{eff_s.get('min_rr',1.5)}** |
| Momentum required | **{'YES — turn off in sidebar' if eff_s.get('require_momentum') else 'NO'}** |
| BTC filter | **{btc_t}** — {"⚠️ BEARISH blocking LONGs!" if btc_t=='BEARISH' else "✅ OK"} |
| Errors | **{len(errs)}** out of {eff_s['scan_depth']} coins |""")
            if errs: st.code("\n".join(errs[:15]),language="text")
else:
    sq=[r for r in results if r['cls']=="squeeze"]
    br=[r for r in results if r['cls']=="breakout"]
    wh=[r for r in results if r['cls']=="whale_driven"]
    ea=[r for r in results if r['cls']=="early"]
    top=results[0]
    mom_count=sum(1 for r in results if r.get('momentum_confirmed'))
    dual_count=sum(1 for r in results if r.get('dual_confirmed'))
    penalty_count=sum(1 for r in results if r.get('warnings'))

    st.markdown(f"""<div class="stat-strip">
      <div><div class="ss-val" style="color:var(--red);">{len(sq)}</div><div class="ss-lbl">Squeeze</div></div>
      <div><div class="ss-val" style="color:var(--amber);">{len(br)}</div><div class="ss-lbl">Breakout</div></div>
      <div><div class="ss-val" style="color:var(--purple);">{len(wh)}</div><div class="ss-lbl">Whale Driven</div></div>
      <div><div class="ss-val" style="color:var(--blue);">{len(ea)}</div><div class="ss-lbl">Early</div></div>
      <div><div class="ss-val">{len(results)}</div><div class="ss-lbl">Total</div></div>
      <div><div class="ss-val">{st.session_state.last_raw_count}</div><div class="ss-lbl">Raw Scanned</div></div>
      <div><div class="ss-val" style="color:var(--green);">{mom_count}</div><div class="ss-lbl">Momentum Live</div></div>
      <div><div class="ss-val" style="color:#ff0000;font-weight:900;">{dual_count}</div><div class="ss-lbl">🔥 Dual Confirmed</div></div>
      <div><div class="ss-val" style="color:var(--red);">{penalty_count}</div><div class="ss-lbl">Penalised</div></div>
      <div><div class="ss-val" style="color:{pump_color(top['pump_score'])};">{top['symbol']}</div><div class="ss-lbl">Hottest ({top['pump_score']})</div></div>
    </div>""",unsafe_allow_html=True)

    # ── SENTINEL RESULTS (full cards) ─────────────────────────────────────
    sent_res=st.session_state.get('sentinel_results',[])
    if sent_res:
        st.markdown('<div style="background:linear-gradient(135deg,#1a0a2e,#0f1117);border:1px solid #7c3aed66;border-radius:10px;padding:10px 16px;margin-bottom:12px;"><span style="font-family:monospace;font-size:.65rem;font-weight:700;color:#a78bfa;">🛰️ SENTINEL LIVE — Full Signal Cards</span></div>',unsafe_allow_html=True)
        for _sr in sent_res[:10]:
            # Check if this sentinel signal is also in scanner results (dual confirm)
            scanner_syms={r['symbol'] for r in results}
            is_dual=_sr['symbol'] in scanner_syms
            render_card(_sr,_sr.get('pump_score',0)>=90,dual_confirmed=is_dual)
        if st.button("🗑️ Clear Sentinel Results",key="clr_sent"):
            st.session_state.sentinel_results=[]; st.rerun()
        st.markdown("---")

    tab_s,tab_b,tab_w,tab_e=st.tabs([
        f"🔴  SQUEEZE  ({len(sq)})",
        f"🟡  BREAKOUT  ({len(br)})",
        f"🐋  WHALE DRIVEN  ({len(wh)})",
        f"🔵  EARLY  ({len(ea)})"
    ])
    with tab_s:
        st.markdown('<div class="tab-desc"><b>About to Squeeze</b> — Funding extreme + OB heavily imbalanced. Trapped shorts/longs about to be liquidated. ⚠️ Cards with ⚠️ badges = late entry risk.</div>',unsafe_allow_html=True)
        [render_card(r,r.get('pump_score',0)>=90,r.get('dual_confirmed',False)) for r in sq] if sq else st.markdown('<div class="empty-st">⚡ No squeeze setups</div>',unsafe_allow_html=True)
    with tab_b:
        st.markdown('<div class="tab-desc"><b>Confirmed Breakout</b> — OI spiking with volume. New money entering. 🔷 FVG zones now used for SL placement.</div>',unsafe_allow_html=True)
        [render_card(r,r.get('pump_score',0)>=90,r.get('dual_confirmed',False)) for r in br] if br else st.markdown('<div class="empty-st">📈 No confirmed breakouts</div>',unsafe_allow_html=True)
    with tab_w:
        st.markdown('<div class="tab-desc"><b>Whale Driven</b> — Large wall + volume surge. Follow the whale.</div>',unsafe_allow_html=True)
        [render_card(r,r.get('pump_score',0)>=90,r.get('dual_confirmed',False)) for r in wh] if wh else st.markdown('<div class="empty-st">🐋 No whale setups</div>',unsafe_allow_html=True)
    with tab_e:
        st.markdown('<div class="tab-desc"><b>Early Signal</b> — Pre-pump alignment. Watchlist — often move into Squeeze/Breakout within 1–4h.</div>',unsafe_allow_html=True)
        [render_card(r,r.get('pump_score',0)>=90,r.get('dual_confirmed',False)) for r in ea] if ea else st.markdown('<div class="empty-st">📡 No early signals</div>',unsafe_allow_html=True)

# ── DST ISOLATED SCANNER SECTION ─────────────────────────────────────────
st.markdown('---')
st.markdown('<div style="font-family:monospace;font-weight:700;font-size:.85rem;color:#0284c7;padding:8px 0;">🔵 DEMA+ST INDEPENDENT SCANNER</div>', unsafe_allow_html=True)

dst_col1, dst_col2, dst_col3 = st.columns([2,2,4])
with dst_col1:
    dst_run = st.button("▶ Run Now", use_container_width=True)
with dst_col2:
    dst_interval = st.number_input("Interval (min)", 1, 60, int(eff_s.get('dst_interval', 5)), key='dst_interval_input')
with dst_col3:
    st.markdown(f'<div style="font-family:monospace;font-size:.6rem;color:#64748b;padding-top:8px;">Last scan: {st.session_state.get("dst_last_scan","–")} | Signals found: {len(st.session_state.get("dst_results",[]))} | 🟢 Auto-running every {int(eff_s.get("dst_interval",5))} min</div>', unsafe_allow_html=True)

# Auto-run on startup and every dst_interval minutes
dst_last_ts = st.session_state.get('dst_last_ts', 0)
dst_should_run = dst_run or (time.time() - dst_last_ts >= dst_interval * 60)

if dst_should_run:
    coin_list = st.session_state.get('last_coin_list', [])
    if not coin_list:
        st.warning("⚠️ Run an APEX5 scan first so DEMA+ST has a coin list to work with.")
    else:
        with st.spinner(f"🔵 DEMA+ST scanning {len(coin_list)} coins on {eff_s.get('dst_timeframe','5m')}..."):
            try:
                ccxt_mod, _ = _load_heavy()
                exchange_clients = {}
                for ex_id in ['gate','mexc','bybit','binance']:
                    try:
                        exchange_clients[ex_id] = getattr(ccxt_mod, ex_id)({'enableRateLimit': True})
                    except:
                        pass
                dst_signals = run_dst_scan(coin_list, eff_s, exchange_clients)
                st.session_state['dst_results'] = dst_signals
                st.session_state['dst_last_scan'] = datetime.now(timezone.utc).strftime('%H:%M:%S')
                st.session_state['dst_last_ts'] = time.time()
                st.session_state['dst_scan_count'] = st.session_state.get('dst_scan_count', 0) + 1

                # Send Discord alerts for new signals
                alerted = st.session_state.get('dst_alerted', set())
                for sig in dst_signals:
                    ak = f"{sig['symbol']}_{sig['direction']}_{sig['scan_time']}"
                    if ak not in alerted and eff_s.get('discord_webhook'):
                        send_dst_discord_alert(eff_s['discord_webhook'], sig)
                        alerted.add(ak)
                st.session_state['dst_alerted'] = alerted
                st.success(f"✅ DEMA+ST scan complete — {len(dst_signals)} signals found")
            except Exception as e:
                st.error(f"DST scan error: {e}")

# Display DST results
dst_results = st.session_state.get('dst_results', [])
if dst_results:
    st.markdown(f'<div style="font-family:monospace;font-size:.65rem;color:#0284c7;padding:4px 0;">🔵 {len(dst_results)} DEMA+ST signals — {st.session_state.get("dst_last_scan","–")} UTC</div>', unsafe_allow_html=True)
    for sig in dst_results:
        dir_color = "#059669" if sig['direction'] == 'LONG' else "#dc2626"
        dir_bg = "#f0fdf4" if sig['direction'] == 'LONG' else "#fef2f2"
        dir_border = "#86efac" if sig['direction'] == 'LONG' else "#fca5a5"
        st.markdown(f'''
        <div style="background:{dir_bg};border:1px solid {dir_border};border-left:4px solid {dir_color};border-radius:8px;padding:10px 14px;margin:6px 0;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span style="font-family:monospace;font-weight:700;font-size:.8rem;color:{dir_color};">🔵 {sig["symbol"]} — {sig["direction"]}</span>
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">{sig.get("exchange","–")} • {sig.get("tf","5m")} • {sig.get("scan_time","–")} | {sig.get("scan_time_pkt","–")}</span>
          </div>
          <div style="display:flex;gap:16px;flex-wrap:wrap;">
            <span style="font-family:monospace;font-size:.65rem;color:#1e293b;">📍 Entry: <b>${sig["close"]:.6f}</b></span>
            <span style="font-family:monospace;font-size:.65rem;color:#059669;">🎯 TP: <b>${sig["tp"]:.6f}</b></span>
            <span style="font-family:monospace;font-size:.65rem;color:#dc2626;">🛑 SL: <b>${sig["sl"]:.6f}</b></span>
            <span style="font-family:monospace;font-size:.65rem;color:#0284c7;">R:R <b>{sig["rr"]}</b></span>
          </div>
          <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:4px;">
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">RSI: {sig["rsi"]}</span>
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">VOL: {sig["vol_ratio"]}×</span>
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">ST flip: {sig["fresh_bars"]} bars ago</span>
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">DEMA: ${sig["dema"]:.6f}</span>
            <span style="font-family:monospace;font-size:.6rem;color:#64748b;">TP1: ${sig["tp1"]:.6f} | TP2: ${sig["tp2"]:.6f} | TP3: ${sig["tp3"]:.6f}</span>
          </div>
        </div>''', unsafe_allow_html=True)
else:
    st.markdown('<div style="font-family:monospace;font-size:.65rem;color:#94a3b8;padding:8px 0;">🔵 No DEMA+ST signals yet — click Run DEMA+ST Scan</div>', unsafe_allow_html=True)
# ── MARKET SENTIMENT DASHBOARD ────────────────────────────────────────────
st.markdown('---')
st.markdown('<div style="font-family:monospace;font-weight:700;font-size:.85rem;color:#0284c7;padding:8px 0;">🌍 MARKET PULSE</div>', unsafe_allow_html=True)

_ms_col1, _ms_col2 = st.columns([3,1])
with _ms_col1:
    _ms_run = st.button("🔄 Refresh Market Pulse", key='ms_refresh')
with _ms_col2:
    _ms_last = st.session_state.get('ms_last_update', '–')
    st.markdown(f'<div style="font-family:monospace;font-size:.6rem;color:#64748b;padding-top:8px;">Last update: {_ms_last}</div>', unsafe_allow_html=True)

_ms_auto = st.session_state.get('ms_last_ts', 0)
_ms_should_run = _ms_run or _ms_auto == 0 or (time.time() - _ms_auto >= 300)

if _ms_should_run:
    try:
        import ccxt as _ccxt
        _ex = _ccxt.gate({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
        _tickers = _ex.fetch_tickers()

        _ranked = []
        for _sym, _t in _tickers.items():
            _pct = float(_t.get('percentage') or 0)
            _vol = float(_t.get('quoteVolume') or 0)
            _last = float(_t.get('last') or 0)
            if _vol > 100000 and _last > 0:
                _ranked.append({'symbol': _sym, 'pct': _pct, 'vol': _vol})

        _total = len(_ranked)
        _bulls = len([r for r in _ranked if r['pct'] > 0])
        _bears = len([r for r in _ranked if r['pct'] < 0])
        _bull_pct = round(_bulls / _total * 100) if _total > 0 else 50

        _strong_up = len([r for r in _ranked if r['pct'] >= 5])
        _strong_dn = len([r for r in _ranked if r['pct'] <= -5])

        _top5 = sorted(_ranked, key=lambda x: x['vol'], reverse=True)[:5]
        _total_vol = sum(r['vol'] for r in _ranked)

        # Market direction label
        if _bull_pct >= 65: _mkt_label = "Strong Bull 🐂"; _mkt_color = "#059669"
        elif _bull_pct >= 55: _mkt_label = "Moderate Bull 📈"; _mkt_color = "#10b981"
        elif _bull_pct >= 45: _mkt_label = "Neutral ↔️"; _mkt_color = "#f59e0b"
        elif _bull_pct >= 35: _mkt_label = "Moderate Bear 📉"; _mkt_color = "#f97316"
        else: _mkt_label = "Strong Bear 🐻"; _mkt_color = "#dc2626"

        # Session recommendation
        if _bull_pct >= 60 and _strong_up > _strong_dn * 2:
            _rec = "🟢 GOOD TO TRADE — Bullish momentum active"
            _rec_color = "#059669"; _rec_bg = "#f0fdf4"
        elif _bull_pct >= 50 and _strong_up > _strong_dn:
            _rec = "🟡 NEUTRAL — Be selective, mixed signals"
            _rec_color = "#92400e"; _rec_bg = "#fffbeb"
        else:
            _rec = "🔴 HIGH RISK — Wait for clearer direction"
            _rec_color = "#dc2626"; _rec_bg = "#fef2f2"

        st.session_state['ms_data'] = {
            'bull_pct': _bull_pct, 'bears': _bears, 'bulls': _bulls,
            'total': _total, 'strong_up': _strong_up, 'strong_dn': _strong_dn,
            'top5': _top5, 'total_vol': _total_vol,
            'mkt_label': _mkt_label, 'mkt_color': _mkt_color,
            'rec': _rec, 'rec_color': _rec_color, 'rec_bg': _rec_bg
        }
        _utc, _pkt = _dual_time()
        st.session_state['ms_last_update'] = _pkt
        st.session_state['ms_last_ts'] = time.time()
    except Exception as _e:
        st.markdown(f'<div style="color:#dc2626;font-family:monospace;font-size:.65rem;">Market Pulse error: {_e}</div>', unsafe_allow_html=True)

_ms = st.session_state.get('ms_data', None)
if _ms:
    # ── Row 1: Key metrics ────────────────────────────────────────────
    _mc1, _mc2, _mc3, _mc4 = st.columns(4)
    with _mc1:
        st.markdown(f'''<div style="background:#f8fafc;border:1px solid {_ms["mkt_color"]};border-radius:8px;padding:10px;text-align:center;">
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">MARKET DIRECTION</div>
        <div style="font-family:monospace;font-size:.85rem;font-weight:700;color:{_ms["mkt_color"]};">{_ms["mkt_label"]}</div>
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">{_ms["bulls"]}↑ {_ms["bears"]}↓ of {_ms["total"]} coins</div>
        </div>''', unsafe_allow_html=True)
    with _mc2:
        st.markdown(f'''<div style="background:#f8fafc;border:1px solid #0284c7;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">BULL/BEAR RATIO</div>
        <div style="font-family:monospace;font-size:.85rem;font-weight:700;color:#0284c7;">{_ms["bull_pct"]}% Bullish</div>
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">{100-_ms["bull_pct"]}% Bearish</div>
        </div>''', unsafe_allow_html=True)
    with _mc3:
        st.markdown(f'''<div style="background:#f8fafc;border:1px solid #7c3aed;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">STRONG MOVERS</div>
        <div style="font-family:monospace;font-size:.85rem;font-weight:700;color:#7c3aed;">+5%: {_ms["strong_up"]} coins</div>
        <div style="font-family:monospace;font-size:.6rem;color:#dc2626;">-5%: {_ms["strong_dn"]} coins</div>
        </div>''', unsafe_allow_html=True)
    with _mc4:
        _vol_b = round(_ms["total_vol"] / 1e9, 1)
        st.markdown(f'''<div style="background:#f8fafc;border:1px solid #059669;border-radius:8px;padding:10px;text-align:center;">
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">TOTAL VOLUME</div>
        <div style="font-family:monospace;font-size:.85rem;font-weight:700;color:#059669;">${_vol_b}B</div>
        <div style="font-family:monospace;font-size:.6rem;color:#64748b;">24H GATE futures</div>
        </div>''', unsafe_allow_html=True)

    # ── Row 2: Bull/Bear progress bar ────────────────────────────────
    _bar_green = _ms["bull_pct"]
    _bar_red = 100 - _bar_green
    st.markdown(f'''
    <div style="margin:10px 0 4px;font-family:monospace;font-size:.6rem;color:#64748b;">Market Breadth</div>
    <div style="display:flex;height:12px;border-radius:6px;overflow:hidden;margin-bottom:4px;">
        <div style="width:{_bar_green}%;background:#059669;"></div>
        <div style="width:{_bar_red}%;background:#dc2626;"></div>
    </div>
    <div style="display:flex;justify-content:space-between;font-family:monospace;font-size:.58rem;color:#64748b;">
        <span>🟢 {_bar_green}% Bullish</span><span>🔴 {_bar_red}% Bearish</span>
    </div>''', unsafe_allow_html=True)

    # ── Row 3: Top 5 most active ──────────────────────────────────────
    st.markdown('<div style="font-family:monospace;font-size:.6rem;color:#64748b;margin-top:8px;margin-bottom:4px;">Top 5 Most Active Coins</div>', unsafe_allow_html=True)
    _t5cols = st.columns(5)
    for _i, _coin in enumerate(_ms['top5']):
        _sym = _coin['symbol'].replace('/USDT:USDT','').replace('/USDT','')
        _pct = _coin['pct']
        _pct_color = '#059669' if _pct >= 0 else '#dc2626'
        _vol_m = round(_coin['vol'] / 1e6, 1)
        with _t5cols[_i]:
            st.markdown(f'''<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:6px;text-align:center;">
            <div style="font-family:monospace;font-size:.65rem;font-weight:700;color:#0f1117;">{_sym}</div>
            <div style="font-family:monospace;font-size:.7rem;color:{_pct_color};">{_pct:+.1f}%</div>
            <div style="font-family:monospace;font-size:.55rem;color:#94a3b8;">${_vol_m}M</div>
            </div>''', unsafe_allow_html=True)

    # ── Row 4: Session recommendation ────────────────────────────────
    st.markdown(f'''
    <div style="background:{_ms["rec_bg"]};border:1px solid {_ms["rec_color"]};border-radius:8px;
    padding:10px 14px;margin-top:10px;font-family:monospace;font-size:.72rem;
    font-weight:700;color:{_ms["rec_color"]};">{_ms["rec"]}</div>''', unsafe_allow_html=True)

# ── GAINERS & LOSERS SCANNER SECTION ─────────────────────────────────────
st.markdown('---')
st.markdown('<div style="font-family:monospace;font-weight:700;font-size:.85rem;color:#7c3aed;padding:8px 0;">📊 GAINERS & LOSERS SCANNER — 4H Catalyst + 5m Entry</div>', unsafe_allow_html=True)
gl_col1, gl_col2, gl_col3 = st.columns([2,2,4])
with gl_col1:
    gl_run = st.button("▶ Run G/L Scan", use_container_width=True, key='gl_run_btn')
with gl_col2:
    gl_interval = st.number_input("Interval (min)", 1, 60, int(eff_s.get('gl_interval', 5)), key='gl_interval_input')
with gl_col3:
    st.markdown(f'<div style="font-family:monospace;font-size:.6rem;color:#64748b;padding-top:8px;">Last scan: {st.session_state.get("gl_last_scan","–")} | Signals: {len(st.session_state.get("gl_results",[]))} | 🟢 Auto every {int(eff_s.get("gl_interval",5))} min</div>', unsafe_allow_html=True)

gl_status = st.empty()
gl_last_ts = st.session_state.get('gl_last_ts', 0)
gl_never_run = gl_last_ts == 0
gl_should_run = gl_run or gl_never_run or (time.time() - gl_last_ts >= gl_interval * 60)

_gl_active_done = st.session_state.pop('gl_active_done', False)
_gl_pre_pending = st.session_state.pop('gl_pre_pending', False)
if _gl_pre_pending:
    # ── Pass 2 only — pre-signals ─────────────────────────────────
    try:
        ccxt_mod, _ = _load_heavy()
        exchange_clients = {}
        for ex_id in ['gate','mexc','bybit','binance']:
            try: exchange_clients[ex_id] = getattr(ccxt_mod, ex_id)({'enableRateLimit': True})
            except: pass
        s_pre = dict(eff_s)
        s_pre['gl_alert_pullback'] = False
        s_pre['gl_alert_breakout'] = False
        s_pre['gl_alert_bounce'] = False
        s_pre['gl_alert_breakdown'] = False
        gl_status.markdown('<div style="font-family:monospace;font-size:.65rem;color:#7c3aed;">👀 Scanning flat coins for Pre-Gainer/Pre-Loser...</div>', unsafe_allow_html=True)
        pre_signals = run_gl_scan([], s_pre, exchange_clients, status_placeholder=gl_status)
        if pre_signals:
            existing = st.session_state.get('gl_results', [])
            active_only = [s for s in existing if s.get('type') not in ['PRE_GAINER','PRE_LOSER']]
            st.session_state['gl_results'] = active_only + pre_signals
            for sig in pre_signals:
                log_gl_signal(sig)
            if eff_s.get('discord_webhook'):
                alerted = st.session_state.get('gl_alerted', set())
                for sig in pre_signals:
                    _sid = f"{sig['symbol']}_{sig['type']}"
                    if _sid not in alerted:
                        send_gl_discord_alert(eff_s['discord_webhook'], sig)
                        alerted.add(_sid)
                st.session_state['gl_alerted'] = alerted
        # ── Log pre-signals to history ────────────────────────────
        if pre_signals:
            history = st.session_state.get('gl_history', [])
            for sig in pre_signals:
                sig['_id'] = f"{sig['symbol']}_{sig['type']}_{sig.get('scan_time','')}"
                if sig['_id'] not in [h.get('_id') for h in history]:
                    history.insert(0, sig)
            st.session_state['gl_history'] = history[:10]
        gl_status.markdown('<div style="font-family:monospace;font-size:.65rem;color:#059669;">✅ Pre-signal scan complete</div>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"G/L pre-scan error: {e}")

elif gl_should_run and not _gl_active_done:
    with st.spinner(f"📊 Fetching live gainers/losers from exchange..."):
        try:
            ccxt_mod, _ = _load_heavy()
            exchange_clients = {}
            for ex_id in ['gate','mexc','bybit','binance']:
                try: exchange_clients[ex_id] = getattr(ccxt_mod, ex_id)({'enableRateLimit': True})
                except: pass
            # ── Run active signals first (top 20 only) — fast ─────────
            s_active = dict(eff_s)
            s_active['gl_alert_pregainer'] = False
            s_active['gl_alert_preloser'] = False
            gl_status.markdown('<div style="font-family:monospace;font-size:.65rem;color:#0284c7;">⚡ Scanning top gainers/losers for active signals...</div>', unsafe_allow_html=True)
            active_signals = run_gl_scan([], s_active, exchange_clients, status_placeholder=gl_status)
            # Fire active signals immediately — rerun to display before pre-signals
            if active_signals:
                existing = st.session_state.get('gl_results', [])
                st.session_state['gl_results'] = active_signals + [s for s in existing if s.get('type') in ['PRE_GAINER','PRE_LOSER']]
                st.session_state['gl_active_done'] = True
                st.session_state['gl_last_ts'] = time.time()
                st.session_state['gl_last_scan'] = _dual_time()[1]
                # ── Log to performance tracker ────────────────────────
                for sig in active_signals:
                    log_gl_signal(sig)
                try: check_gl_outcomes()
                except: pass
                # ── Maintain last 10 signals history ─────────────────
                history = st.session_state.get('gl_history', [])
                for sig in active_signals:
                    sig['_id'] = f"{sig['symbol']}_{sig['type']}_{sig.get('scan_time','')}"
                    if sig['_id'] not in [h.get('_id') for h in history]:
                        history.insert(0, sig)
                st.session_state['gl_history'] = history[:10]
                # ── Save to journal ───────────────────────────────────
                try:
                    ensure_journal()
                    _jt = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    with open(JOURNAL_FILE, 'a', newline='', encoding='utf-8') as jf:
                        w = csv.writer(jf)
                        for sig in active_signals:
                            w.writerow([
                                _jt, sig.get('symbol',''), sig.get('exchange','GATE'),
                                sig.get('label','GL'), '', sig.get('type',''),
                                sig.get('close',''), sig.get('tp',''), sig.get('sl',''),
                                f"4H:{sig.get('chg_4h','')}% RSI:{sig.get('rsi','')} VOL:{sig.get('vol_ratio','')}x RR:{sig.get('rr','')}",
                                'ACTIVE', '0','0','0','0'
                            ])
                except Exception as _je:
                    print(f"[GL] Journal save error: {_je}")
                # Send Discord alerts for active signals immediately
                if eff_s.get('discord_webhook'):
                    alerted = st.session_state.get('gl_alerted', set())
                    for sig in active_signals:
                        _sid = f"{sig['symbol']}_{sig['type']}"
                        if _sid not in alerted:
                            send_gl_discord_alert(eff_s['discord_webhook'], sig)
                            alerted.add(_sid)
                    st.session_state['gl_alerted'] = alerted
                st.session_state['gl_pre_pending'] = True
                st.rerun()
            else:
                # No active signals — still queue pre-signals
                st.session_state['gl_pre_pending'] = True
                st.rerun()

            # ── History/logging handled in active signals block above ─
            # active_signals already logged and history updated before rerun

            # ── Save G/L signals to journal — moved to active signals block ─

            # Discord alerts and success message handled in active signals block
            pass
        except Exception as e:
            st.error(f"G/L scan error: {e}")

# Display G/L results
# ── G/L Signal History (last 10) ─────────────────────────────────────
gl_history = st.session_state.get('gl_history', [])
if gl_history:
    st.markdown('<div style="font-family:monospace;font-weight:700;font-size:.78rem;color:#64748b;padding:8px 0 4px 0;">📋 Last 10 Signals — click ✕ to dismiss</div>', unsafe_allow_html=True)
    hist_cols = st.columns(min(len(gl_history), 5))
    to_remove = None
    type_colors_mini = {
        'GAINER_PULLBACK': ('#f0fdf4','#059669'),
        'GAINER_BREAKOUT': ('#f0fdf4','#16a34a'),
        'LOSER_BOUNCE':    ('#eff6ff','#0284c7'),
        'LOSER_BREAKDOWN': ('#fef2f2','#dc2626'),
        'PRE_GAINER':      ('#fffbeb','#d97706'),
        'PRE_LOSER':       ('#fff7ed','#ea580c'),
    }
    for i, sig in enumerate(gl_history):
        bg, color = type_colors_mini.get(sig.get('type',''), ('#f8fafc','#64748b'))
        tp_line = f"🎯 ${sig['tp']:.6f}" if sig.get('tp') else '🎯 —'
        sl_line = f"🛑 ${sig['sl']:.6f}" if sig.get('sl') else '🛑 —'
        rr_line = f"R:R {sig['rr']}" if sig.get('rr') else ''
        with hist_cols[i % 5]:
            st.markdown(f'''
            <div style="background:{bg};border:2px solid {color};border-radius:8px;padding:10px 12px;margin:2px 0;">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                <span style="font-family:monospace;font-size:.68rem;font-weight:700;color:{color};">{sig.get("emoji","")} {sig.get("symbol","").replace("/USDT:USDT","")}</span>
                <span style="font-family:monospace;font-size:.55rem;color:#94a3b8;">{sig.get("scan_time","–")} | {sig.get("scan_time_pkt","–")}</span>
              </div>
              <div style="font-family:monospace;font-size:.58rem;color:{color};font-weight:600;margin-bottom:4px;">{sig.get("label","")}</div>
              <div style="font-family:monospace;font-size:.6rem;color:#1e293b;margin-bottom:2px;">
                4H: <b>{sig.get("chg_4h",0):+.1f}%</b> &nbsp; 1H: <b>{sig.get("chg_1h",0):+.1f}%</b>
              </div>
              <div style="font-family:monospace;font-size:.6rem;color:#1e293b;margin-bottom:2px;">
                RSI: <b>{sig.get("rsi","")}</b> &nbsp; VOL: <b>{sig.get("vol_ratio","")}×</b>
              </div>
              <div style="font-family:monospace;font-size:.6rem;color:#1e293b;margin-bottom:2px;">
                Entry: <b>${sig.get("close",0):.6f}</b>
              </div>
              <div style="font-family:monospace;font-size:.58rem;color:{color};margin-bottom:2px;">
                {tp_line} &nbsp; {sl_line}
              </div>
              <div style="font-family:monospace;font-size:.58rem;color:#64748b;">{rr_line} &nbsp; {sig.get("exchange","")}</div>
            </div>''', unsafe_allow_html=True)
            if st.button("✕ dismiss", key=f'gl_hist_remove_{i}'):
                to_remove = i
    if to_remove is not None:
        gl_history.pop(to_remove)
        st.session_state['gl_history'] = gl_history
        st.rerun()
    st.markdown('---')

# ── G/L Performance Scoreboard ───────────────────────────────────────
gl_stats = get_gl_stats()
if any(v['total'] > 0 for v in gl_stats.values()):
    st.markdown('<div style="font-family:monospace;font-weight:700;font-size:.75rem;color:#64748b;padding:6px 0;">📊 G/L Success Rate Tracker</div>', unsafe_allow_html=True)
    type_emojis = {
        'GAINER_PULLBACK':'🟢','GAINER_BREAKOUT':'🚀',
        'LOSER_BOUNCE':'🔄','LOSER_BREAKDOWN':'📉',
        'PRE_GAINER':'👀','PRE_LOSER':'⚠️'
    }
    score_cols = st.columns(3)
    col_idx = 0
    for sig_type, stat in gl_stats.items():
        if stat['total'] == 0: continue
        emoji = type_emojis.get(sig_type,'')
        wr = stat['wr']
        bar_fill = int(wr/10)
        bar = '█'*bar_fill + '░'*(10-bar_fill)
        color = '#059669' if wr>=60 else '#f59e0b' if wr>=40 else '#dc2626'
        with score_cols[col_idx % 3]:
            st.markdown(f'''
            <div style="background:#f8fafc;border:1px solid {color};border-radius:6px;padding:8px 10px;margin:3px 0;">
              <div style="font-family:monospace;font-size:.65rem;font-weight:700;color:{color};">{emoji} {sig_type.replace("_"," ")}</div>
              <div style="font-family:monospace;font-size:.7rem;color:{color};">{bar} {wr}%</div>
              <div style="font-family:monospace;font-size:.6rem;color:#64748b;">{stat["wins"]}W {stat["losses"]}L | avg: {stat["avg_pnl"]:+.1f}%</div>
              <div style="font-family:monospace;font-size:.55rem;color:#94a3b8;">{stat["pending"]} pending | {stat["expired"]} expired</div>
            </div>''', unsafe_allow_html=True)
        col_idx += 1
    st.markdown('---')

gl_results = st.session_state.get('gl_results', [])
if gl_results:
    type_order = ['PRE_GAINER','GAINER_PULLBACK','GAINER_BREAKOUT','LOSER_BOUNCE','LOSER_BREAKDOWN','PRE_LOSER']
    type_colors_bg = {
        'GAINER_PULLBACK': ('#f0fdf4','#86efac','#059669'),
        'GAINER_BREAKOUT': ('#f0fdf4','#4ade80','#16a34a'),
        'LOSER_BOUNCE':    ('#eff6ff','#93c5fd','#0284c7'),
        'LOSER_BREAKDOWN': ('#fef2f2','#fca5a5','#dc2626'),
        'PRE_GAINER':      ('#fffbeb','#fcd34d','#d97706'),
        'PRE_LOSER':       ('#fff7ed','#fdba74','#ea580c'),
    }
    for sig_type in type_order:
        type_sigs = [r for r in gl_results if r['type'] == sig_type]
        if not type_sigs: continue
        bg, border, color = type_colors_bg.get(sig_type, ('#f8fafc','#e2e8f0','#64748b'))
        first = type_sigs[0]
        st.markdown(f'<div style="font-family:monospace;font-size:.65rem;font-weight:700;color:{color};padding:6px 0;">{first["emoji"]} {first["label"]} — {len(type_sigs)} signals</div>', unsafe_allow_html=True)
        for sig in type_sigs:
            tp_sl = f"🎯 TP: ${sig['tp']:.6f} | 🛑 SL: ${sig['sl']:.6f} | R:R {sig['rr']}" if sig.get('tp') else "👁 Watch for entry setup"
            st.markdown(f'''
            <div style="background:{bg};border:1px solid {border};border-left:4px solid {color};border-radius:8px;padding:10px 14px;margin:4px 0;">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">
                <span style="font-family:monospace;font-weight:700;font-size:.78rem;color:{color};">{sig["emoji"]} {sig["symbol"]} — {sig["direction"]}</span>
                <span style="font-family:monospace;font-size:.58rem;color:#64748b;">{sig.get("exchange","–")} • {sig.get("scan_time","–")} | {sig.get("scan_time_pkt","–")}</span>
              </div>
              <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:3px;">
                <span style="font-family:monospace;font-size:.63rem;color:#1e293b;">4H: <b>{sig["chg_4h"]:+.2f}%</b></span>
                <span style="font-family:monospace;font-size:.63rem;color:#1e293b;">1H: <b>{sig["chg_1h"]:+.2f}%</b></span>
                <span style="font-family:monospace;font-size:.63rem;color:#1e293b;">RSI: <b>{sig["rsi"]}</b></span>
                <span style="font-family:monospace;font-size:.63rem;color:#1e293b;">VOL: <b>{sig["vol_ratio"]}×</b></span>
                <span style="font-family:monospace;font-size:.63rem;color:#1e293b;">Entry: <b>${sig["close"]:.6f}</b></span>
              </div>
              <div style="font-family:monospace;font-size:.6rem;color:{color};">{tp_sl}</div>
            </div>''', unsafe_allow_html=True)
else:
    st.markdown('<div style="font-family:monospace;font-size:.65rem;color:#94a3b8;padding:8px 0;">📊 No G/L signals yet — click ▶ Run G/L Scan</div>', unsafe_allow_html=True)
if eff_s.get('gl_enabled', True):
    next_gl = int(eff_s.get('gl_interval', 5) * 60 - (time.time() - st.session_state.get('gl_last_ts', 0)))
    next_gl = max(0, next_gl)
    st.markdown(f'<div style="text-align:center;font-family:monospace;font-size:.62rem;color:#0284c7;padding:8px 0;">📊 G/L Auto-Scan — Next scan in {next_gl//60}m {next_gl%60}s</div>', unsafe_allow_html=True)
    if not eff_s.get('auto_scan'):
        time.sleep(30)
        st.rerun()

if eff_s.get('auto_scan'):
    st.markdown(f'<div style="text-align:center;font-family:monospace;font-size:.62rem;color:#9ca3af;padding:16px 0;">🤖 Auto-Pilot Active — Next scan in {eff_s["auto_interval"]} minute(s)</div>',unsafe_allow_html=True)
    time.sleep(eff_s['auto_interval']*60)
    st.rerun()
