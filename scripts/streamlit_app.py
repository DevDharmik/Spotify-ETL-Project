"""
Spotify ETL Pipeline — Interactive Dashboard
=============================================
Dynamic 3D dashboard with theme switcher.
Author: Dharmik Champaneri
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Spotify ETL Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Themes ────────────────────────────────────────────────────────────────────

THEMES = {
    "🟢 Spotify Dark": {
        "bg":        "#0a0a0f",
        "surface":   "#111118",
        "surface2":  "#1a1a2e",
        "accent":    "#1DB954",
        "accent2":   "#1ed760",
        "text":      "#ffffff",
        "muted":     "#888888",
        "particle":  ["#1DB954","#1ed760","#17a349","#ffffff"],
        "gradient":  "linear-gradient(135deg,#1DB954 0%,#1ed760 50%,#17a349 100%)",
        "chart_seq": ["#0d4a1f","#1DB954","#1ed760"],
        "chart_bg":  "#0d0d14",
    },
    "💜 Neon Purple": {
        "bg":        "#0d0010",
        "surface":   "#130018",
        "surface2":  "#1e0030",
        "accent":    "#bf5fff",
        "accent2":   "#d98aff",
        "text":      "#ffffff",
        "muted":     "#9988aa",
        "particle":  ["#bf5fff","#d98aff","#7700cc","#ffffff"],
        "gradient":  "linear-gradient(135deg,#bf5fff 0%,#d98aff 50%,#7700cc 100%)",
        "chart_seq": ["#1e0030","#bf5fff","#d98aff"],
        "chart_bg":  "#130018",
    },
    "🔵 Ocean Blue": {
        "bg":        "#020b18",
        "surface":   "#071628",
        "surface2":  "#0c2340",
        "accent":    "#00b4d8",
        "accent2":   "#48cae4",
        "text":      "#ffffff",
        "muted":     "#7799bb",
        "particle":  ["#00b4d8","#48cae4","#0077b6","#ffffff"],
        "gradient":  "linear-gradient(135deg,#00b4d8 0%,#48cae4 50%,#0077b6 100%)",
        "chart_seq": ["#023e8a","#00b4d8","#48cae4"],
        "chart_bg":  "#071628",
    },
    "🔴 Crimson": {
        "bg":        "#0f0005",
        "surface":   "#1a000a",
        "surface2":  "#2a0010",
        "accent":    "#ff3860",
        "accent2":   "#ff6b8a",
        "text":      "#ffffff",
        "muted":     "#aa7788",
        "particle":  ["#ff3860","#ff6b8a","#cc0033","#ffffff"],
        "gradient":  "linear-gradient(135deg,#ff3860 0%,#ff6b8a 50%,#cc0033 100%)",
        "chart_seq": ["#4a0010","#ff3860","#ff6b8a"],
        "chart_bg":  "#1a000a",
    },
    "☀️ Light Mode": {
        "bg":        "#f5f5f5",
        "surface":   "#ffffff",
        "surface2":  "#eef2ff",
        "accent":    "#1DB954",
        "accent2":   "#17a349",
        "text":      "#111111",
        "muted":     "#666666",
        "particle":  ["#1DB954","#17a349","#0f7a35","#333333"],
        "gradient":  "linear-gradient(135deg,#1DB954 0%,#17a349 100%)",
        "chart_seq": ["#0f7a35","#1DB954","#1ed760"],
        "chart_bg":  "#f0f0f0",
    },
}

# ── Colour helper ─────────────────────────────────────────────────────────────
# Plotly colorscales ONLY accept: named colours, 6-digit hex, or rgb()/rgba().
# They do NOT accept 8-digit hex (e.g. "#1DB95499"). Use this helper everywhere
# a semi-transparent accent colour is needed inside a Plotly colorscale.

def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    """Convert a 6-digit hex colour + alpha float → 'rgba(r,g,b,a)' string."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


# ── Sidebar Theme Picker ───────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:10px 0 16px;'>
      <div style='font-size:2.5rem;'>🎵</div>
      <div style='font-size:1rem;font-weight:600;margin-top:6px;'>Spotify ETL</div>
      <div style='font-size:0.75rem;opacity:0.5;margin-top:2px;'>Pipeline Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    theme_name = st.selectbox("🎨 Theme", list(THEMES.keys()), index=0)
    T = THEMES[theme_name]

    st.markdown("---")
    st.markdown("**🎛️ Filters**")

# ── Inject Theme CSS + Particles ──────────────────────────────────────────────

particle_colors = str(T["particle"]).replace("'", '"')

st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
  .stApp {{ background: {T["bg"]}; }}
  #MainMenu, footer, header {{ visibility: hidden; }}

  #particles-js {{
    position: fixed; top:0; left:0;
    width:100%; height:100%;
    z-index:0; pointer-events:none;
  }}

  .metric-card {{
    background: linear-gradient(135deg, {T["surface2"]} 0%, {T["surface"]} 100%);
    border: 1px solid {T["accent"]}55;
    border-radius: 16px; padding: 20px 24px;
    text-align: center;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
    position: relative; overflow: hidden;
  }}
  .metric-card::after {{
    content:''; position:absolute;
    top:-60%; left:-60%; width:220%; height:220%;
    background: radial-gradient(circle, {T["accent"]}08 0%, transparent 60%);
    pointer-events:none;
  }}
  .metric-card:hover {{
    transform: translateY(-5px);
    box-shadow: 0 12px 40px {T["accent"]}33;
    border-color: {T["accent"]}99;
  }}
  .metric-value {{ font-size:2.2rem; font-weight:700; color:{T["accent"]}; margin:0; line-height:1.2; }}
  .metric-label {{ font-size:0.82rem; color:{T["muted"]}; margin-top:6px; text-transform:uppercase; letter-spacing:0.08em; }}
  .metric-delta  {{ font-size:0.76rem; color:{T["accent2"]}; margin-top:4px; }}

  .section-header {{ font-size:1.1rem; font-weight:600; color:{T["text"]}; margin-bottom:4px; }}
  .section-sub    {{ font-size:0.82rem; color:{T["muted"]}; margin-bottom:16px; }}

  .hero-title {{
    font-size:2.8rem; font-weight:700;
    background: {T["gradient"]};
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    background-clip:text; margin:0; line-height:1.1;
  }}
  .hero-sub   {{ font-size:1rem; color:{T["muted"]}; margin-top:8px; }}
  .hero-badge {{
    display:inline-block;
    background: {T["accent"]}22;
    border:1px solid {T["accent"]}66;
    color:{T["accent"]}; font-size:0.75rem;
    padding:4px 14px; border-radius:99px;
    margin-top:12px; font-weight:500;
    animation: pulse 2.5s ease-in-out infinite;
  }}
  @keyframes pulse {{
    0%,100% {{ box-shadow: 0 0 0 0 {T["accent"]}44; }}
    50%      {{ box-shadow: 0 0 0 8px {T["accent"]}00; }}
  }}

  .custom-divider {{
    border:none; height:1px;
    background:linear-gradient(90deg,transparent,{T["accent"]}55,transparent);
    margin:24px 0;
  }}

  .pipeline-step {{
    background: {T["surface"]};
    border:1px solid {T["accent"]}22;
    border-radius:14px; padding:18px;
    text-align:center;
    transition: all 0.3s ease;
    position:relative; overflow:hidden;
  }}
  .pipeline-step::before {{
    content:''; position:absolute;
    bottom:-30px; left:50%; transform:translateX(-50%);
    width:60px; height:60px; border-radius:50%;
    background: radial-gradient(circle, {T["accent"]}15, transparent);
    transition: all 0.4s;
  }}
  .pipeline-step:hover {{
    border-color:{T["accent"]}88;
    box-shadow: 0 8px 30px {T["accent"]}22;
    transform: translateY(-3px);
  }}
  .pipeline-step:hover::before {{
    width:120px; height:120px; bottom:-20px;
  }}
  .pipeline-icon  {{ font-size:2rem; margin-bottom:8px; }}
  .pipeline-label {{ font-size:0.88rem; font-weight:700; color:{T["text"]}; letter-spacing:0.05em; }}
  .pipeline-desc  {{ font-size:0.74rem; color:{T["muted"]}; margin-top:5px; }}

  [data-testid="stSidebar"] {{
    background: {T["surface"]} !important;
    border-right:1px solid {T["accent"]}22;
  }}
  [data-testid="stSidebar"] * {{ color:{T["text"]} !important; }}

  ::-webkit-scrollbar       {{ width:5px; }}
  ::-webkit-scrollbar-track {{ background:{T["bg"]}; }}
  ::-webkit-scrollbar-thumb {{ background:{T["accent"]}; border-radius:3px; }}

  .chart-wrapper {{
    border-radius:14px;
    padding:2px;
    background: linear-gradient(135deg, {T["accent"]}44, transparent, {T["accent2"]}33);
    margin-bottom:8px;
  }}
</style>

<div id="particles-js"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/particles.js/2.0.0/particles.min.js"></script>
<script>
window.addEventListener('load', function() {{
  if (typeof particlesJS === 'undefined') return;
  particlesJS('particles-js', {{
    particles: {{
      number: {{ value: 90, density: {{ enable: true, value_area: 800 }} }},
      color:  {{ value: {particle_colors} }},
      shape:  {{ type: 'circle' }},
      opacity: {{ value: 0.4, random: true,
        anim: {{ enable: true, speed: 1, opacity_min: 0.05, sync: false }}
      }},
      size: {{ value: 3, random: true,
        anim: {{ enable: true, speed: 2, size_min: 0.5, sync: false }}
      }},
      line_linked: {{
        enable: true, distance: 120,
        color: '{T["accent"]}', opacity: 0.15, width: 1
      }},
      move: {{
        enable: true, speed: 0.7, direction: 'none',
        random: true, straight: false, out_mode: 'out', bounce: false
      }}
    }},
    interactivity: {{
      detect_on: 'canvas',
      events: {{
        onhover: {{ enable: true, mode: 'grab' }},
        onclick: {{ enable: true, mode: 'bubble' }},
        resize: true
      }},
      modes: {{
        grab:   {{ distance: 180, line_linked: {{ opacity: 0.5 }} }},
        bubble: {{ distance: 200, size: 8, duration: 0.4 }}
      }}
    }},
    retina_detect: true
  }});
}});
</script>
""", unsafe_allow_html=True)


# ── Load Data ─────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "../data/spotify_cleaned.csv")
    df   = pd.read_csv(path)

    rename_map = {}
    for c in ["track_name","name","song_name","title","track"]:
        if c in df.columns and "track_name" not in df.columns:
            rename_map[c] = "track_name"; break
    for c in ["artists","artist_name","artist","performer"]:
        if c in df.columns and "artist_name" not in df.columns:
            rename_map[c] = "artist_name"; break
    for c in ["album_name","album"]:
        if c in df.columns and "album" not in df.columns:
            rename_map[c] = "album"; break
    for c in ["track_genre","genre","playlist_genre","genres"]:
        if c in df.columns and "genre" not in df.columns:
            rename_map[c] = "genre"; break
    for c in ["duration_ms","duration"]:
        if c in df.columns and "duration_ms" not in df.columns:
            rename_map[c] = "duration_ms"; break
    if rename_map:
        df = df.rename(columns=rename_map)

    if "duration_ms" in df.columns and "duration_min" not in df.columns:
        df["duration_min"] = (df["duration_ms"] / 60000).round(2)
    if "explicit" in df.columns:
        df["explicit"] = df["explicit"].astype(bool)
    else:
        df["explicit"] = False
    if "release_year" not in df.columns:
        for c in ["release_date","year"]:
            if c in df.columns:
                df["release_year"] = pd.to_datetime(df[c], errors="coerce").dt.year
                break
        if "release_year" not in df.columns:
            df["release_year"] = 2020

    for col in ["track_name","artist_name","album","genre"]:
        if col not in df.columns: df[col] = "Unknown"
    if "popularity"   not in df.columns: df["popularity"]   = 50
    if "duration_min" not in df.columns: df["duration_min"] = 3.5
    for col in ["danceability","energy","valence","acousticness","speechiness"]:
        if col not in df.columns:
            df[col] = np.random.uniform(0.3, 0.9, len(df)).round(3)

    df = df.dropna(subset=["track_name","artist_name"])
    df["release_year"] = df["release_year"].fillna(2020).astype(int)
    df["popularity"]   = pd.to_numeric(df["popularity"], errors="coerce").fillna(50).astype(int)
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"❌ Could not load dataset: {e}")
    st.stop()

# ── Sidebar Filters ───────────────────────────────────────────────────────────

with st.sidebar:
    genres = sorted(df["genre"].dropna().unique())
    selected_genres = st.multiselect("Genre", genres, default=genres)

    yr_min = int(df["release_year"].dropna().min())
    yr_max = int(df["release_year"].dropna().max())
    if yr_min == yr_max:
        yr_max = yr_min + 1
    year_range = st.slider("Release Year", yr_min, yr_max, (yr_min, yr_max), step=1)

    pop_range     = st.slider("Popularity", 0, 100, (0, 100))
    show_explicit = st.toggle("Include Explicit Tracks", value=True)

    st.markdown("---")
    st.markdown(f"""
    <div style='font-size:0.75rem;'>
      <div style='margin-bottom:5px;opacity:0.5;'>📦 {len(df):,} total tracks loaded</div>
      <div style='margin-bottom:5px;'>🔗 <a href='https://github.com/DevDharmik/Spotify-ETL-Project' style='color:{T["accent"]};'>GitHub</a></div>
      <div>👤 <a href='https://linkedin.com/in/dharmikchampaneri' style='color:{T["accent"]};'>Dharmik Champaneri</a></div>
    </div>
    """, unsafe_allow_html=True)

# ── Apply Filters ─────────────────────────────────────────────────────────────

filtered = df[
    (df["genre"].isin(selected_genres)) &
    (df["release_year"].between(*year_range)) &
    (df["popularity"].between(*pop_range))
]
if not show_explicit:
    filtered = filtered[filtered["explicit"] == False]
if len(filtered) == 0:
    st.warning("⚠️ No tracks match your filters.")
    st.stop()

# ── Chart theme helper ────────────────────────────────────────────────────────

def chart_layout(height=380, margin=None):
    m = margin or dict(l=0, r=0, t=10, b=0)
    return dict(
        paper_bgcolor=T["bg"],
        plot_bgcolor=T["chart_bg"],
        font_color=T["text"],
        font_family="Inter",
        margin=m,
        height=height,
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color=T["muted"]),
        xaxis=dict(color=T["muted"], gridcolor=T["surface2"]),
        yaxis=dict(color=T["muted"], gridcolor=T["surface2"]),
    )

# ── Hero ──────────────────────────────────────────────────────────────────────

st.markdown(f"""
<div style='padding:10px 0 4px;'>
  <p class='hero-title'>🎵 Spotify ETL Dashboard</p>
  <p class='hero-sub'>End-to-end pipeline · Extract · Transform · Load · Visualise</p>
  <span class='hero-badge'>▶ Real Data · PostgreSQL · Python · pandas · SQLAlchemy</span>
</div>
<hr class='custom-divider'>
""", unsafe_allow_html=True)

# ── KPI Cards ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
for col, icon, val, label, delta in [
    (c1,"🎵", f"{len(filtered):,}",                    "Total Tracks",   f"of {len(df):,} loaded"),
    (c2,"🎤", f"{filtered['artist_name'].nunique():,}", "Unique Artists",  "in dataset"),
    (c3,"⭐", f"{filtered['popularity'].mean():.1f}",  "Avg Popularity",  "out of 100"),
    (c4,"⏱️", f"{filtered['duration_min'].mean():.2f}","Avg Duration",    "minutes"),
    (c5,"🔥", f"{filtered['explicit'].sum():,}",        "Explicit Tracks", f"{filtered['explicit'].mean()*100:.0f}% of total"),
]:
    with col:
        st.markdown(f"""
        <div class='metric-card'>
          <div style='font-size:1.6rem;margin-bottom:8px;'>{icon}</div>
          <p class='metric-value'>{val}</p>
          <p class='metric-label'>{label}</p>
          <p class='metric-delta'>{delta}</p>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div style='margin-top:30px;'></div>", unsafe_allow_html=True)

# ── Pipeline Flow ─────────────────────────────────────────────────────────────

st.markdown("<p class='section-header'>⚡ ETL Pipeline Flow</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>From raw CSV to structured database to live dashboard</p>", unsafe_allow_html=True)

p1,a1,p2,a2,p3,a3,p4 = st.columns([2,0.3,2,0.3,2,0.3,2])
for col, icon, label, desc in [
    (p1,"📥","EXTRACT",   "Raw CSV → pandas DataFrame"),
    (p2,"🔧","TRANSFORM", "Clean · Deduplicate · Type-cast"),
    (p3,"🗄️", "LOAD",      "PostgreSQL via SQLAlchemy"),
    (p4,"📊","VISUALISE", "Live Streamlit dashboard"),
]:
    with col:
        st.markdown(f"""
        <div class='pipeline-step'>
          <div class='pipeline-icon'>{icon}</div>
          <div class='pipeline-label'>{label}</div>
          <div class='pipeline-desc'>{desc}</div>
        </div>
        """, unsafe_allow_html=True)
for arr in [a1,a2,a3]:
    with arr:
        st.markdown(f"<div style='text-align:center;font-size:1.5rem;color:{T['accent']};padding-top:18px;'>➜</div>",
                    unsafe_allow_html=True)

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

# ── 3D Scatter + Top Artists ──────────────────────────────────────────────────

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown("<p class='section-header'>🌐 3D Audio Feature Space</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Popularity · Energy · Danceability — rotate to explore</p>", unsafe_allow_html=True)
    sample = filtered.sample(min(500, len(filtered)), random_state=42)
    fig1 = px.scatter_3d(
        sample, x="popularity", y="energy", z="danceability",
        color="genre", size="popularity", size_max=12, opacity=0.8,
        hover_data=["track_name","artist_name"],
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig1.update_layout(
        paper_bgcolor=T["bg"], font_color=T["text"],
        font_family="Inter", margin=dict(l=0,r=0,t=10,b=0), height=460,
        scene=dict(
            bgcolor=T["chart_bg"],
            xaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                       color=T["muted"], title="Popularity"),
            yaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                       color=T["muted"], title="Energy"),
            zaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                       color=T["muted"], title="Danceability"),
        ),
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color=T["muted"]),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown("<p class='section-header'>🎤 Top 10 Artists</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Most tracks in the loaded dataset</p>", unsafe_allow_html=True)
    top = filtered["artist_name"].value_counts().head(10).reset_index()
    top.columns = ["artist","count"]
    fig2 = px.bar(top, x="count", y="artist", orientation="h",
                  color="count", color_continuous_scale=T["chart_seq"])
    fig2.update_layout(**chart_layout(460))
    fig2.update_layout(coloraxis_showscale=False,
                       yaxis=dict(categoryorder="total ascending",
                                  color=T["muted"], gridcolor=T["surface2"]))
    fig2.update_traces(marker_line_width=0,
                       hovertemplate="<b>%{y}</b><br>Tracks: %{x}<extra></extra>")
    st.plotly_chart(fig2, use_container_width=True)

# ── 3D Surface — Popularity × Danceability × Energy ──────────────────────────
# FIX: Plotly colorscales reject 8-digit hex (e.g. "#1DB95499").
#      All stop colours must be 6-digit hex, named colours, or rgba() strings.

st.markdown("<p class='section-header'>🏔️ 3D Surface — Popularity Landscape</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Average popularity across energy and danceability buckets</p>", unsafe_allow_html=True)

e_bins  = pd.cut(filtered["energy"],       bins=15, labels=False)
d_bins  = pd.cut(filtered["danceability"], bins=15, labels=False)
pivot   = filtered.copy()
pivot["e_bin"] = e_bins
pivot["d_bin"] = d_bins
surface_data = pivot.groupby(["e_bin","d_bin"])["popularity"].mean().unstack(fill_value=0)

# Build colorscale with proper rgba() strings (not 8-digit hex)
surf_colorscale = [
    [0.0, T["surface2"]],                          # 6-digit hex — valid ✓
    [0.4, hex_to_rgba(T["accent"], 0.6)],           # rgba()      — valid ✓
    [1.0, T["accent2"]],                            # 6-digit hex — valid ✓
]

fig_surf = go.Figure(data=[go.Surface(
    z=surface_data.values,
    colorscale=surf_colorscale,
    showscale=True,
    contours=dict(
        z=dict(show=True, usecolormap=True, highlightcolor=T["accent2"], project_z=True)
    ),
)])
fig_surf.update_layout(
    paper_bgcolor=T["bg"], font_color=T["text"], font_family="Inter",
    margin=dict(l=0, r=0, t=20, b=0), height=480,
    scene=dict(
        bgcolor=T["chart_bg"],
        xaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                   color=T["muted"], title="Danceability Bucket"),
        yaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                   color=T["muted"], title="Energy Bucket"),
        zaxis=dict(backgroundcolor=T["chart_bg"], gridcolor=T["surface2"],
                   color=T["muted"], title="Avg Popularity"),
        camera=dict(eye=dict(x=1.6, y=-1.6, z=0.8)),
    ),
)
st.plotly_chart(fig_surf, use_container_width=True)

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

# ── Popularity Distribution + Tracks Per Year ─────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.markdown("<p class='section-header'>📈 Popularity Distribution</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Score spread across all loaded tracks</p>", unsafe_allow_html=True)
    fig3 = px.histogram(filtered, x="popularity", nbins=40,
                        color_discrete_sequence=[T["accent"]])
    fig3.update_layout(**chart_layout(300))
    fig3.update_traces(marker_line_width=0,
                       hovertemplate="Popularity: %{x}<br>Tracks: %{y}<extra></extra>")
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown("<p class='section-header'>📅 Tracks Released Per Year</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Volume by release year after ETL load</p>", unsafe_allow_html=True)
    yearly = filtered.groupby("release_year").size().reset_index(name="count")
    fig4 = px.area(yearly, x="release_year", y="count",
                   color_discrete_sequence=[T["accent"]], line_shape="spline")
    fig4.update_traces(fill="tozeroy", fillcolor=hex_to_rgba(T["accent"], 0.13),
                       line_color=T["accent"], line_width=2.5)
    fig4.update_layout(**chart_layout(300))
    st.plotly_chart(fig4, use_container_width=True)

# ── Donut + Radar ─────────────────────────────────────────────────────────────

col5, col6 = st.columns(2)

with col5:
    st.markdown("<p class='section-header'>🎸 Tracks by Genre</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Genre distribution in pipeline output</p>", unsafe_allow_html=True)
    gc = filtered["genre"].value_counts().reset_index()
    gc.columns = ["genre","count"]
    fig5 = px.pie(gc, names="genre", values="count",
                  color_discrete_sequence=px.colors.qualitative.Bold, hole=0.55)
    fig5.update_layout(paper_bgcolor=T["bg"], font_color=T["text"],
                       font_family="Inter", margin=dict(l=0,r=0,t=10,b=0), height=320,
                       legend=dict(bgcolor="rgba(0,0,0,0)", font_color=T["muted"]))
    fig5.update_traces(textfont_color=T["text"],
                       hovertemplate="<b>%{label}</b><br>%{value} · %{percent}<extra></extra>")
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.markdown("<p class='section-header'>🕸️ Audio Features by Genre</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Danceability · Energy · Valence · Acousticness</p>", unsafe_allow_html=True)
    feat_cols = [c for c in ["danceability","energy","valence","acousticness","speechiness"]
                 if c in filtered.columns]
    radar = filtered.groupby("genre")[feat_cols].mean().reset_index()
    cats  = [c.capitalize() for c in feat_cols]
    palette = px.colors.qualitative.Bold
    fig6  = go.Figure()
    for i, row in radar.iterrows():
        v = [row[c] for c in feat_cols]
        raw_color = palette[i % len(palette)]
        # Build a safe fill colour — convert rgb(...) to rgba(..., 0.08)
        if raw_color.startswith("rgb(") and not raw_color.startswith("rgba("):
            fill_color = raw_color.replace("rgb(", "rgba(").replace(")", ",0.08)")
        else:
            fill_color = hex_to_rgba(T["accent"], 0.06)
        fig6.add_trace(go.Scatterpolar(
            r=v+[v[0]], theta=cats+[cats[0]],
            fill="toself", name=row["genre"],
            line_color=raw_color,
            fillcolor=fill_color,
            line_width=1.8, opacity=0.9,
        ))
    fig6.update_layout(
        polar=dict(
            bgcolor=T["chart_bg"],
            radialaxis=dict(visible=True, range=[0,1], color=T["muted"], gridcolor=T["surface2"]),
            angularaxis=dict(color=T["text"], gridcolor=T["surface2"]),
        ),
        paper_bgcolor=T["bg"], font_color=T["text"], font_family="Inter",
        margin=dict(l=20,r=20,t=10,b=10), height=320,
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color=T["muted"], font_size=11),
    )
    st.plotly_chart(fig6, use_container_width=True)

# ── Mood Map ──────────────────────────────────────────────────────────────────

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>✨ Mood Map — Energy vs Valence</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>High energy + high valence = happy bangers · Low + low = dark/calm</p>", unsafe_allow_html=True)

fig7 = px.scatter(
    filtered.sample(min(600, len(filtered)), random_state=1),
    x="energy", y="valence", color="genre",
    size="popularity", size_max=14, opacity=0.72,
    hover_data=["track_name","artist_name","popularity"],
    color_discrete_sequence=px.colors.qualitative.Bold,
)
fig7.update_layout(**chart_layout(380))
for x, y, txt in [
    (0.12, 0.93, "😴 Calm & Happy"),
    (0.82, 0.93, "🔥 Energetic & Happy"),
    (0.12, 0.05, "😢 Calm & Sad"),
    (0.82, 0.05, "😤 Intense & Dark"),
]:
    fig7.add_annotation(x=x, y=y, xref="paper", yref="paper",
                        text=txt, showarrow=False,
                        font=dict(color=T["muted"], size=11, family="Inter"))
fig7.add_vline(x=0.5, line_dash="dot", line_color=hex_to_rgba(T["accent"], 0.27), line_width=1)
fig7.add_hline(y=0.5, line_dash="dot", line_color=hex_to_rgba(T["accent"], 0.27), line_width=1)
st.plotly_chart(fig7, use_container_width=True)

# ── Parallel Coordinates ──────────────────────────────────────────────────────
# FIX: same 8-digit hex issue — use rgba() for the mid-stop colour.

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>🔀 Parallel Coordinates — Multi-Feature Explorer</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Drag the axes to filter — see how features relate across all tracks</p>", unsafe_allow_html=True)

par_cols = [c for c in ["popularity","danceability","energy","valence","acousticness","duration_min"]
            if c in filtered.columns]

# Build colorscale with proper colour formats
parcoords_colorscale = [
    [0.0, T["surface2"]],                   # 6-digit hex — valid ✓
    [0.5, T["accent"]],                     # 6-digit hex — valid ✓
    [1.0, T["accent2"]],                    # 6-digit hex — valid ✓
]

fig8 = go.Figure(data=go.Parcoords(
    line=dict(
        color=filtered["popularity"],
        colorscale=parcoords_colorscale,
        showscale=True,
        cmin=0, cmax=100,
    ),
    dimensions=[dict(label=c.replace("_"," ").title(),
                     values=filtered[c]) for c in par_cols],
    labelcolor=T["text"],
))
fig8.update_layout(
    paper_bgcolor=T["bg"], font_color=T["text"], font_family="Inter",
    margin=dict(l=60, r=40, t=40, b=20), height=380,
)
st.plotly_chart(fig8, use_container_width=True)

# ── Data Table ────────────────────────────────────────────────────────────────

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>🗃️ PostgreSQL Output Preview</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Cleaned and structured data as loaded into the database</p>", unsafe_allow_html=True)

show_cols = [c for c in ["track_name","artist_name","album","genre",
                          "release_year","duration_min","popularity",
                          "danceability","energy","valence","explicit"]
             if c in filtered.columns]
st.dataframe(
    filtered[show_cols].head(100).reset_index(drop=True),
    use_container_width=True, height=340,
)

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown(f"""
<hr class='custom-divider'>
<div style='text-align:center;color:{T["muted"]};font-size:0.8rem;padding-bottom:24px;'>
  Built by
  <a href='https://linkedin.com/in/dharmikchampaneri' style='color:{T["accent"]};font-weight:500;'>Dharmik Champaneri</a>
  &nbsp;·&nbsp;
  <a href='https://github.com/DevDharmik/Spotify-ETL-Project' style='color:{T["accent"]};'>GitHub</a>
  &nbsp;·&nbsp; M.Sc. Data Science · UE Germany
</div>
""", unsafe_allow_html=True)
