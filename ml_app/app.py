import streamlit as st
import pandas as pd
import numpy as np
import joblib
import random

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Churn Radar",
    page_icon="📡",
    layout="wide",
)

# ── Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
    /* Dark background */
    .stApp { background-color: #0a0d14; color: #e8ecf4; }
    
    /* Cards */
    div[data-testid="stForm"] {
        background: #111620;
        border: 1px solid #1e2740;
        border-radius: 14px;
        padding: 20px;
    }
    
    /* Section headers */
    .section-title {
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 2px;
        text-transform: uppercase;
        color: #6b7a9e;
        border-bottom: 1px solid #1e2740;
        padding-bottom: 8px;
        margin: 20px 0 10px 0;
    }

    /* Result box */
    .result-churn {
        background: linear-gradient(135deg, rgba(247,95,95,0.1), rgba(247,95,95,0.03));
        border: 1px solid rgba(247,95,95,0.4);
        border-radius: 16px;
        padding: 28px 32px;
    }
    .result-stay {
        background: linear-gradient(135deg, rgba(79,218,154,0.1), rgba(79,218,154,0.03));
        border: 1px solid rgba(79,218,154,0.4);
        border-radius: 16px;
        padding: 28px 32px;
    }
    .result-verdict {
        font-size: 28px;
        font-weight: 700;
        margin: 8px 0;
    }
    .chip-neg {
        display:inline-block;
        background: rgba(247,95,95,0.1);
        border: 1px solid rgba(247,95,95,0.4);
        color: #f75f5f;
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 12px;
        margin: 3px;
    }
    .chip-pos {
        display:inline-block;
        background: rgba(79,218,154,0.1);
        border: 1px solid rgba(79,218,154,0.4);
        color: #4fda9a;
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 12px;
        margin: 3px;
    }
    /* Hide streamlit branding */
    #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Load model ────────────────────────────────────────────────
@st.cache_resource
def load_model():
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    model = joblib.load(os.path.join(base_dir, 'lgbm_model.pkl'))
    feature_cols = joblib.load(os.path.join(base_dir, 'feature_cols.pkl'))
    return model, feature_cols

try:
    model, feature_cols = load_model()
    model_loaded = True
except FileNotFoundError:
    model_loaded = False


# ── Random profiles ───────────────────────────────────────────
HIGH_RISK = [
    dict(tenure=3,  monthly=89.5, cltv=800,  gender='Female', senior='Yes', partner='No',  dep='No',  internet='Fiber optic', tech='No',  security='No',  contract='Month-to-month', pay='Electronic check',         paper='Yes'),
    dict(tenure=8,  monthly=74.0, cltv=1200, gender='Male',   senior='Yes', partner='No',  dep='No',  internet='Fiber optic', tech='No',  security='No',  contract='Month-to-month', pay='Mailed check',             paper='Yes'),
    dict(tenure=2,  monthly=95.0, cltv=600,  gender='Female', senior='No',  partner='Yes', dep='No',  internet='Fiber optic', tech='No',  security='No',  contract='Month-to-month', pay='Electronic check',         paper='Yes'),
]
LOW_RISK = [
    dict(tenure=60, monthly=45.0, cltv=5800, gender='Male',   senior='No',  partner='Yes', dep='Yes', internet='DSL',         tech='Yes', security='Yes', contract='Two year',        pay='Bank transfer (automatic)', paper='No'),
    dict(tenure=48, monthly=55.0, cltv=4400, gender='Female', senior='No',  partner='Yes', dep='No',  internet='DSL',         tech='Yes', security='Yes', contract='One year',        pay='Credit card (automatic)',   paper='Yes'),
    dict(tenure=36, monthly=60.0, cltv=3500, gender='Male',   senior='No',  partner='No',  dep='Yes', internet='DSL',         tech='No',  security='Yes', contract='Two year',        pay='Bank transfer (automatic)', paper='No'),
]
ALL_PROFILES = HIGH_RISK + LOW_RISK


# ── Preprocessing (same as notebook) ─────────────────────────
def preprocess(inputs: dict, feature_cols: list) -> pd.DataFrame:
    """
    Applies the same encoding pipeline used in training:
      1. Binary map  Yes/No → 1/0
      2. get_dummies on cat_cols
      3. align to training feature_cols
    """
    df = pd.DataFrame([inputs])

    # Binary columns
    binary_cols = ['partner', 'dependents', 'paperless_billing', 'senior_citizen', 'gender']
    yes_no_map = {'Yes': 1, 'No': 0, 'Male': 1, 'Female': 0}
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].map(yes_no_map)

    # One-hot encoding
    cat_cols = ['internet_service', 'tech_support', 'online_security',
                'payment_method', 'contract_type']
    df = pd.get_dummies(df, columns=cat_cols)

    # Align columns to training set (add missing cols as 0, drop extra)
    df = df.reindex(columns=feature_cols, fill_value=0)

    return df


# ── Header ────────────────────────────────────────────────────
col_logo, col_badge = st.columns([6, 1])
with col_logo:
    st.markdown("## 📡 ChurnRadar")
    st.markdown("<span style='color:#6b7a9e;font-size:14px'>Predict customer churn using your trained LightGBM model</span>", unsafe_allow_html=True)
with col_badge:
    if model_loaded:
        st.success("Model ✓")
    else:
        st.error("Model ✗")

if not model_loaded:
    st.error("""
    **الموديل مش موجود!**  
    شغّل الكود ده في الـ notebook الأول:
    ```python
    import joblib
    joblib.dump(lgbm, 'lgbm_model.pkl')
    joblib.dump(list(x_train.columns), 'feature_cols.pkl')
    ```
    وبعدين حط الملفين في نفس فولدر الـ app.
    """)
    st.stop()

st.divider()

# ── Random fill button ────────────────────────────────────────
if 'profile' not in st.session_state:
    st.session_state.profile = None

col_rand, col_clear, _ = st.columns([1, 1, 5])
with col_rand:
    if st.button("🎲 Random Fill", use_container_width=True):
        st.session_state.profile = random.choice(ALL_PROFILES)
with col_clear:
    if st.button("✕ Clear", use_container_width=True):
        st.session_state.profile = None

p = st.session_state.profile or {}

# ── Form ──────────────────────────────────────────────────────
with st.form("predict_form"):

    # ── Numeric ───────────────────────────────────────────────
    st.markdown('<div class="section-title">📊 Usage Metrics</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    tenure        = c1.number_input("Tenure (months)",      min_value=0,   max_value=120, value=int(p.get('tenure', 0)),          step=1)
    monthly       = c2.number_input("Monthly Charges ($)",  min_value=0.0, max_value=500.0, value=float(p.get('monthly', 0.0)),   step=0.5)
    cltv          = c3.number_input("CLTV",                 min_value=0,   max_value=20000, value=int(p.get('cltv', 0)),           step=50)

    # ── Demographics ──────────────────────────────────────────
    st.markdown('<div class="section-title">👤 Demographics</div>', unsafe_allow_html=True)
    d1, d2, d3, d4 = st.columns(4)
    gender        = d1.selectbox("Gender",          ['Male', 'Female'],   index=['Male','Female'].index(p.get('gender','Male')))
    senior        = d2.selectbox("Senior Citizen",  ['No', 'Yes'],        index=['No','Yes'].index(p.get('senior','No')))
    partner       = d3.selectbox("Has Partner",     ['No', 'Yes'],        index=['No','Yes'].index(p.get('partner','No')))
    dependents    = d4.selectbox("Has Dependents",  ['No', 'Yes'],        index=['No','Yes'].index(p.get('dep','No')))

    # ── Services ─────────────────────────────────────────────
    st.markdown('<div class="section-title">🌐 Services</div>', unsafe_allow_html=True)
    s1, s2, s3 = st.columns(3)
    internet_opts = ['DSL', 'Fiber optic', 'No']
    tech_opts     = ['No', 'Yes', 'No internet service']
    sec_opts      = ['No', 'Yes', 'No internet service']
    internet      = s1.selectbox("Internet Service", internet_opts, index=internet_opts.index(p.get('internet','DSL')))
    tech_support  = s2.selectbox("Tech Support",     tech_opts,     index=tech_opts.index(p.get('tech','No')))
    online_sec    = s3.selectbox("Online Security",  sec_opts,      index=sec_opts.index(p.get('security','No')))

    # ── Contract & Payment ────────────────────────────────────
    st.markdown('<div class="section-title">💳 Contract & Payment</div>', unsafe_allow_html=True)
    p1, p2, p3 = st.columns(3)
    contract_opts = ['Month-to-month', 'One year', 'Two year']
    pay_opts      = ['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)']
    contract      = p1.selectbox("Contract Type",    contract_opts, index=contract_opts.index(p.get('contract','Month-to-month')))
    payment       = p2.selectbox("Payment Method",   pay_opts,      index=pay_opts.index(p.get('pay','Electronic check')))
    paperless     = p3.selectbox("Paperless Billing", ['No', 'Yes'],index=['No','Yes'].index(p.get('paper','No')))

    submitted = st.form_submit_button("⚡ Predict Churn", use_container_width=True, type="primary")


# ── Prediction ────────────────────────────────────────────────
if submitted:
    raw = {
        'tenure_months':    tenure,
        'monthly_charges':  monthly,
        'cltv':             cltv,
        'gender':           gender,
        'senior_citizen':   senior,
        'partner':          partner,
        'dependents':       dependents,
        'internet_service': internet,
        'tech_support':     tech_support,
        'online_security':  online_sec,
        'contract_type':    contract,
        'payment_method':   payment,
        'paperless_billing': paperless,
    }

    X = preprocess(raw, feature_cols)
    prob = model.predict_proba(X)[0][1]
    pred = model.predict(X)[0]
    pct  = round(prob * 100, 1)

    is_churn = (pred == 1) or (pred == 'Yes')

    st.divider()

    # Result card
    css_class = "result-churn" if is_churn else "result-stay"
    icon      = "⚠️" if is_churn else "✅"
    verdict   = "Likely to Churn" if is_churn else "Likely to Stay"
    color     = "#f75f5f" if is_churn else "#4fda9a"
    msg       = "Customer needs immediate attention!" if is_churn else "Customer appears stable."

    st.markdown(f"""
    <div class="{css_class}">
        <div style="font-size:11px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;color:{color};">
            {'🔴 HIGH RISK' if is_churn else '🟢 LOW RISK'}
        </div>
        <div class="result-verdict">{icon} {verdict}</div>
        <div style="color:#6b7a9e;font-size:14px;margin-bottom:20px;">
            Churn probability: <strong style="color:#e8ecf4">{pct}%</strong> — {msg}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Gauge
    st.progress(prob, text=f"Churn Risk: {pct}%")

    # Risk factors
    st.markdown("**Risk Factors:**")
    chips = ""
    if contract == 'Month-to-month':     chips += '<span class="chip-neg">↑ Month-to-month contract</span>'
    if contract == 'Two year':           chips += '<span class="chip-pos">↓ 2-year contract</span>'
    if tech_support == 'No':             chips += '<span class="chip-neg">↑ No tech support</span>'
    if tech_support == 'Yes':            chips += '<span class="chip-pos">↓ Has tech support</span>'
    if online_sec == 'No':              chips += '<span class="chip-neg">↑ No online security</span>'
    if online_sec == 'Yes':             chips += '<span class="chip-pos">↓ Has online security</span>'
    if internet == 'Fiber optic':        chips += '<span class="chip-neg">↑ Fiber optic (high bill)</span>'
    if payment == 'Electronic check':    chips += '<span class="chip-neg">↑ Electronic check</span>'
    if tenure >= 24:                     chips += f'<span class="chip-pos">↓ {tenure}m tenure</span>'
    if tenure < 12 and tenure > 0:       chips += f'<span class="chip-neg">↑ Low tenure ({tenure}m)</span>'
    if partner == 'Yes':                 chips += '<span class="chip-pos">↓ Has partner</span>'
    if dependents == 'Yes':              chips += '<span class="chip-pos">↓ Has dependents</span>'
    if senior == 'Yes':                  chips += '<span class="chip-neg">↑ Senior citizen</span>'
    st.markdown(chips, unsafe_allow_html=True)

    # Raw details (expandable)
    with st.expander("🔍 Model Input (after encoding)"):
        st.dataframe(X, use_container_width=True)
