"""
Spotify ETL Pipeline — Interactive Dashboard
=============================================
Author: Dharmik Champaneri
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ── Page Config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Spotify ETL Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS + Particles ───────────────────────────────────────────────────────────

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .stApp { background: #0a0a0f; }
  #MainMenu, footer, header { visibility: hidden; }
  #particles-js {
    position: fixed; top: 0; left: 0;
    width: 100%; height: 100%;
    z-index: 0; pointer-events: none;
  }
  .metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid rgba(29,185,84,0.3);
    border-radius: 16px; padding: 20px 24px;
    text-align: center;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
  }
  .metric-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 8px 32px rgba(29,185,84,0.2);
  }
  .metric-value { font-size:2.2rem; font-weight:700; color:#1DB954; margin:0; line-height:1.2; }
  .metric-label { font-size:0.85rem; color:#aaa; margin-top:6px; text-transform:uppercase; letter-spacing:0.08em; }
  .metric-delta { font-size:0.78rem; color:#1DB954; margin-top:4px; }
  .section-header { font-size:1.1rem; font-weight:600; color:#fff; margin-bottom:4px; }
  .section-sub { font-size:0.82rem; color:#888; margin-bottom:16px; }
  .hero-title {
    font-size:2.6rem; font-weight:700;
    background: linear-gradient(135deg,#1DB954 0%,#1ed760 50%,#17a349 100%);
    -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    background-clip:text; margin:0; line-height:1.1;
  }
  .hero-sub { font-size:1rem; color:#888; margin-top:8px; }
  .hero-badge {
    display:inline-block; background:rgba(29,185,84,0.15);
    border:1px solid rgba(29,185,84,0.4); color:#1DB954;
    font-size:0.75rem; padding:4px 12px; border-radius:99px;
    margin-top:12px; font-weight:500;
  }
  .custom-divider {
    border:none; height:1px;
    background:linear-gradient(90deg,transparent,rgba(29,185,84,0.4),transparent);
    margin:24px 0;
  }
  .pipeline-step {
    background:#111118; border:1px solid rgba(255,255,255,0.08);
    border-radius:12px; padding:16px 18px; text-align:center;
    transition:border-color 0.3s;
  }
  .pipeline-step:hover { border-color:rgba(29,185,84,0.5); }
  .pipeline-icon { font-size:1.8rem; margin-bottom:8px; }
  .pipeline-label { font-size:0.88rem; font-weight:600; color:#fff; }
  .pipeline-desc { font-size:0.75rem; color:#888; margin-top:4px; }
  [data-testid="stSidebar"] {
    background:#0d0d14 !important;
    border-right:1px solid rgba(29,185,84,0.15);
  }
  ::-webkit-scrollbar { width:6px; }
  ::-webkit-scrollbar-track { background:#0a0a0f; }
  ::-webkit-scrollbar-thumb { background:#1DB954; border-radius:3px; }
</style>

<div id="particles-js"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/particles.js/2.0.0/particles.min.js"></script>
<script>
window.addEventListener('load', function() {
  if (typeof particlesJS !== 'undefined') {
    particlesJS('particles-js', {
      particles: {
        number: { value: 80, density: { enable: true, value_area: 900 } },
        color: { value: ['#1DB954','#17a349','#1ed760','#ffffff'] },
        shape: { type: 'circle' },
        opacity: { value: 0.35, random: true,
          anim: { enable: true, speed: 0.8, opacity_min: 0.05, sync: false }
        },
        size: { value: 2.5, random: true,
          anim: { enable: true, speed: 2, size_min: 0.5, sync: false }
        },
        line_linked: { enable: true, distance: 130, color: '#1DB954', opacity: 0.12, width: 1 },
        move: { enable: true, speed: 0.8, direction: 'none', random: true,
                straight: false, out_mode: 'out', bounce: false }
      },
      interactivity: {
        detect_on: 'canvas',
        events: {
          onhover: { enable: true, mode: 'grab' },
          onclick: { enable: true, mode: 'push' },
          resize: true
        },
        modes: {
          grab: { distance: 160, line_linked: { opacity: 0.4 } },
          push: { particles_nb: 3 }
        }
      },
      retina_detect: true
    });
  }
});
</script>
""", unsafe_allow_html=True)


# ── Load Real Data ────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    """Load actual Spotify dataset from data folder."""
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "../data/spotify_cleaned.csv")
    df   = pd.read_csv(path)

    # ── Normalise common column name variants ──────────────────────────────
    rename_map = {}

    # track name
    for c in ["track_name", "name", "song_name", "title", "track"]:
        if c in df.columns and "track_name" not in df.columns:
            rename_map[c] = "track_name"; break

    # artist
    for c in ["artists", "artist_name", "artist", "performer"]:
        if c in df.columns and "artist_name" not in df.columns:
            rename_map[c] = "artist_name"; break

    # album
    for c in ["album_name", "album"]:
        if c in df.columns and "album" not in df.columns:
            rename_map[c] = "album"; break

    # genre
    for c in ["track_genre", "genre", "playlist_genre", "genres"]:
        if c in df.columns and "genre" not in df.columns:
            rename_map[c] = "genre"; break

    # duration
    for c in ["duration_ms", "duration"]:
        if c in df.columns and "duration_ms" not in df.columns:
            rename_map[c] = "duration_ms"; break

    if rename_map:
        df = df.rename(columns=rename_map)

    # ── Derived columns ────────────────────────────────────────────────────
    if "duration_ms" in df.columns and "duration_min" not in df.columns:
        df["duration_min"] = (df["duration_ms"] / 60000).round(2)

    if "explicit" in df.columns:
        df["explicit"] = df["explicit"].astype(bool)
    else:
        df["explicit"] = False

    if "release_year" not in df.columns:
        for c in ["release_date", "year"]:
            if c in df.columns:
                df["release_year"] = pd.to_datetime(
                    df[c], errors="coerce"
                ).dt.year
                break
        if "release_year" not in df.columns:
            df["release_year"] = 2020

    # Fill essential cols if missing
    for col in ["track_name", "artist_name", "album", "genre"]:
        if col not in df.columns:
            df[col] = "Unknown"

    if "popularity" not in df.columns:
        df["popularity"] = 50

    if "duration_min" not in df.columns:
        df["duration_min"] = 3.5

    for col in ["danceability", "energy", "valence"]:
        if col not in df.columns:
            df[col] = np.random.uniform(0.3, 0.9, len(df)).round(3)

    # Clean
    df = df.dropna(subset=["track_name", "artist_name"])
    df["release_year"] = df["release_year"].fillna(2020).astype(int)
    df["popularity"]   = pd.to_numeric(df["popularity"], errors="coerce").fillna(50).astype(int)

    return df


# ── Load ──────────────────────────────────────────────────────────────────────

try:
    df = load_data()
except Exception as e:
    st.error(f"❌ Could not load dataset: {e}")
    st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:12px 0 20px;'>
      <div style='font-size:2.5rem;'>🎵</div>
      <div style='font-size:1rem;font-weight:600;color:#1DB954;margin-top:6px;'>Spotify ETL</div>
      <div style='font-size:0.75rem;color:#666;margin-top:2px;'>Pipeline Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**🎛️ Filters**")

    genres = sorted(df["genre"].dropna().unique())
    selected_genres = st.multiselect("Genre", options=genres, default=genres)

    yr_min = int(df["release_year"].dropna().min())
    yr_max = int(df["release_year"].dropna().max())

    if yr_min == yr_max:
       yr_max = yr_min + 1

    year_range = st.slider(
    "Release Year",
    min_value=yr_min,
    max_value=yr_max,
    value=(yr_min, yr_max),
    step=1,
    )

    pop_range = st.slider("Popularity", 0, 100, (0, 100))

    show_explicit = st.toggle("Include Explicit Tracks", value=True)

    st.markdown("<hr style='border-color:rgba(255,255,255,0.08);margin:20px 0;'>",
                unsafe_allow_html=True)
    st.markdown(f"""
    <div style='font-size:0.75rem;color:#555;'>
      <div style='margin-bottom:4px;'>📦 <span style='color:#888;'>{len(df):,} total tracks loaded</span></div>
      <div style='margin-bottom:6px;'>🔗 <a href='https://github.com/DevDharmik/Spotify-ETL-Project' style='color:#1DB954;'>GitHub Repo</a></div>
      <div>👤 <a href='https://linkedin.com/in/dharmikchampaneri' style='color:#1DB954;'>Dharmik Champaneri</a></div>
    </div>
    """, unsafe_allow_html=True)


# ── Filter ────────────────────────────────────────────────────────────────────

filtered = df[
    (df["genre"].isin(selected_genres)) &
    (df["release_year"].between(*year_range)) &
    (df["popularity"].between(*pop_range))
]
if not show_explicit:
    filtered = filtered[filtered["explicit"] == False]

if len(filtered) == 0:
    st.warning("⚠️ No tracks match your filters. Try adjusting the sidebar.")
    st.stop()


# ── Hero ──────────────────────────────────────────────────────────────────────

st.markdown("""
<div style='padding:10px 0 4px;'>
  <p class='hero-title'>🎵 Spotify ETL Dashboard</p>
  <p class='hero-sub'>End-to-end pipeline · Extract · Transform · Load · Visualise</p>
  <span class='hero-badge'>▶ Real Data · PostgreSQL · Python · pandas · SQLAlchemy</span>
</div>
<hr class='custom-divider'>
""", unsafe_allow_html=True)


# ── KPI Cards ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
kpis = [
    (c1, "🎵", f"{len(filtered):,}",
          "Total Tracks", f"of {len(df):,} loaded"),
    (c2, "🎤", f"{filtered['artist_name'].nunique():,}",
          "Artists", "unique in dataset"),
    (c3, "⭐", f"{filtered['popularity'].mean():.1f}",
          "Avg Popularity", "out of 100"),
    (c4, "⏱️", f"{filtered['duration_min'].mean():.2f} min",
          "Avg Duration", "per track"),
    (c5, "🔥", f"{filtered['explicit'].sum():,}",
          "Explicit Tracks",
          f"{filtered['explicit'].mean()*100:.0f}% of total"),
]
for col, icon, val, label, delta in kpis:
    with col:
        st.markdown(f"""
        <div class='metric-card'>
          <div style='font-size:1.5rem;margin-bottom:6px;'>{icon}</div>
          <p class='metric-value'>{val}</p>
          <p class='metric-label'>{label}</p>
          <p class='metric-delta'>{delta}</p>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)


# ── Pipeline Flow ─────────────────────────────────────────────────────────────

st.markdown("<p class='section-header'>⚡ ETL Pipeline Flow</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Track the journey from raw CSV to structured PostgreSQL database</p>",
            unsafe_allow_html=True)

p1, a1, p2, a2, p3, a3, p4 = st.columns([2, 0.4, 2, 0.4, 2, 0.4, 2])
for col, icon, label, desc in [
    (p1, "📥", "EXTRACT",   "Raw CSV → pandas DataFrame"),
    (p2, "🔧", "TRANSFORM", "Clean · Deduplicate · Type-cast"),
    (p3, "🗄️",  "LOAD",      "Write to PostgreSQL via SQLAlchemy"),
    (p4, "📊", "VISUALISE", "Live insights via Streamlit"),
]:
    with col:
        st.markdown(f"""
        <div class='pipeline-step'>
          <div class='pipeline-icon'>{icon}</div>
          <div class='pipeline-label'>{label}</div>
          <div class='pipeline-desc'>{desc}</div>
        </div>
        """, unsafe_allow_html=True)

for arr in [a1, a2, a3]:
    with arr:
        st.markdown(
            "<div style='text-align:center;font-size:1.4rem;color:#1DB954;padding-top:16px;'>➜</div>",
            unsafe_allow_html=True)

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)


# ── 3D Scatter + Top Artists ──────────────────────────────────────────────────

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown("<p class='section-header'>🌐 3D Audio Feature Space</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Popularity · Energy · Danceability — coloured by Genre</p>",
                unsafe_allow_html=True)
    sample = filtered.sample(min(400, len(filtered)), random_state=42)
    fig1 = px.scatter_3d(
        sample,
        x="popularity", y="energy", z="danceability",
        color="genre", size="popularity",
        size_max=10, opacity=0.75,
        hover_data=["track_name", "artist_name"],
        color_discrete_sequence=px.colors.qualitative.Bold,
        labels={"popularity": "Popularity",
                "energy": "Energy", "danceability": "Danceability"},
    )
    fig1.update_layout(
        paper_bgcolor="#0a0a0f", font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=420,
        scene=dict(
            bgcolor="#0d0d14",
            xaxis=dict(backgroundcolor="#0d0d14", gridcolor="#1a1a2e", color="#888"),
            yaxis=dict(backgroundcolor="#0d0d14", gridcolor="#1a1a2e", color="#888"),
            zaxis=dict(backgroundcolor="#0d0d14", gridcolor="#1a1a2e", color="#888"),
        ),
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#aaa"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown("<p class='section-header'>🎤 Top 10 Artists</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Most tracks in the loaded dataset</p>",
                unsafe_allow_html=True)
    top = (filtered["artist_name"]
           .value_counts().head(10)
           .reset_index())
    top.columns = ["artist", "count"]
    fig2 = px.bar(
        top, x="count", y="artist", orientation="h",
        color="count",
        color_continuous_scale=["#0d4a1f", "#1DB954", "#1ed760"],
    )
    fig2.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=420,
        coloraxis_showscale=False,
        yaxis=dict(categoryorder="total ascending", color="#aaa", gridcolor="#111118"),
        xaxis=dict(color="#aaa", gridcolor="#111118"),
    )
    fig2.update_traces(
        marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>Tracks: %{x}<extra></extra>"
    )
    st.plotly_chart(fig2, use_container_width=True)


# ── Popularity Distribution + Tracks Per Year ─────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.markdown("<p class='section-header'>📈 Popularity Distribution</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Score spread across all loaded tracks</p>",
                unsafe_allow_html=True)
    fig3 = px.histogram(
        filtered, x="popularity", nbins=40,
        color_discrete_sequence=["#1DB954"],
    )
    fig3.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300, bargap=0.04,
        xaxis=dict(color="#888", gridcolor="#111118", title="Popularity Score"),
        yaxis=dict(color="#888", gridcolor="#111118", title="Track Count"),
    )
    fig3.update_traces(
        marker_line_width=0,
        hovertemplate="Popularity: %{x}<br>Tracks: %{y}<extra></extra>"
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown("<p class='section-header'>📅 Tracks Released Per Year</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Volume by release year after ETL load</p>",
                unsafe_allow_html=True)
    yearly = (filtered.groupby("release_year")
              .size().reset_index(name="count"))
    fig4 = px.area(
        yearly, x="release_year", y="count",
        color_discrete_sequence=["#1DB954"], line_shape="spline",
    )
    fig4.update_traces(
        fill="tozeroy", fillcolor="rgba(29,185,84,0.15)",
        line_color="#1DB954", line_width=2.5,
        hovertemplate="Year: %{x}<br>Tracks: %{y}<extra></extra>"
    )
    fig4.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        xaxis=dict(color="#888", gridcolor="#111118", title="Year"),
        yaxis=dict(color="#888", gridcolor="#111118", title="Track Count"),
    )
    st.plotly_chart(fig4, use_container_width=True)


# ── Genre Donut + Audio Radar ─────────────────────────────────────────────────

col5, col6 = st.columns(2)

with col5:
    st.markdown("<p class='section-header'>🎸 Tracks by Genre</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Genre distribution in the pipeline output</p>",
                unsafe_allow_html=True)
    gc = (filtered["genre"].value_counts()
          .reset_index())
    gc.columns = ["genre", "count"]
    fig5 = px.pie(
        gc, names="genre", values="count",
        color_discrete_sequence=px.colors.sequential.Greens_r,
        hole=0.5,
    )
    fig5.update_layout(
        paper_bgcolor="#0a0a0f", font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#aaa"),
    )
    fig5.update_traces(
        textfont_color="#fff",
        hovertemplate="<b>%{label}</b><br>%{value} tracks · %{percent}<extra></extra>"
    )
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.markdown("<p class='section-header'>🕸️ Audio Features by Genre</p>",
                unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Danceability · Energy · Valence</p>",
                unsafe_allow_html=True)
    radar = (filtered.groupby("genre")[["danceability","energy","valence"]]
             .mean().reset_index())
    cats   = ["Danceability", "Energy", "Valence"]
    colors = ["#1DB954","#1ed760","#17a349","#0f7a35",
              "#0d6b2f","#0a5c28","#074d21","#1DB954","#1ed760"]
    fig6   = go.Figure()
    for i, row in radar.iterrows():
        v = [row["danceability"], row["energy"], row["valence"]]
        fig6.add_trace(go.Scatterpolar(
            r=v + [v[0]], theta=cats + [cats[0]],
            fill="toself", name=row["genre"],
            line_color=colors[i % len(colors)],
            fillcolor=f"rgba(29,185,84,{0.04 + i*0.02})",
            line_width=1.5, opacity=0.85,
        ))
    fig6.update_layout(
        polar=dict(
            bgcolor="#0d0d14",
            radialaxis=dict(visible=True, range=[0,1],
                            color="#555", gridcolor="#1a1a2e"),
            angularaxis=dict(color="#888", gridcolor="#1a1a2e"),
        ),
        paper_bgcolor="#0a0a0f", font_color="#ccc", font_family="Inter",
        margin=dict(l=20, r=20, t=10, b=10), height=300,
        legend=dict(bgcolor="rgba(0,0,0,0)",
                    font_color="#aaa", font_size=11),
    )
    st.plotly_chart(fig6, use_container_width=True)


# ── Energy vs Valence Scatter ─────────────────────────────────────────────────

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>✨ Energy vs Valence — Mood Map</p>",
            unsafe_allow_html=True)
st.markdown("<p class='section-sub'>High energy + high valence = happy bangers · Low energy + low valence = sad/calm</p>",
            unsafe_allow_html=True)

fig7 = px.scatter(
    filtered.sample(min(500, len(filtered)), random_state=1),
    x="energy", y="valence",
    color="genre", size="popularity",
    size_max=12, opacity=0.7,
    hover_data=["track_name", "artist_name", "popularity"],
    color_discrete_sequence=px.colors.qualitative.Bold,
)
fig7.update_layout(
    paper_bgcolor="#0a0a0f", plot_bgcolor="#0d0d14",
    font_color="#ccc", font_family="Inter",
    margin=dict(l=0, r=0, t=10, b=0), height=350,
    xaxis=dict(color="#888", gridcolor="#1a1a2e", title="Energy"),
    yaxis=dict(color="#888", gridcolor="#1a1a2e", title="Valence (Positivity)"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#aaa"),
)
# Quadrant labels
for x, y, txt in [
    (0.15, 0.92, "😴 Calm & Happy"),
    (0.82, 0.92, "🔥 Energetic & Happy"),
    (0.15, 0.05, "😢 Calm & Sad"),
    (0.82, 0.05, "😤 Intense & Dark"),
]:
    fig7.add_annotation(
        x=x, y=y, xref="paper", yref="paper",
        text=txt, showarrow=False,
        font=dict(color="rgba(255,255,255,0.3)", size=11, family="Inter"),
    )
st.plotly_chart(fig7, use_container_width=True)


# ── Data Table ────────────────────────────────────────────────────────────────

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>🗃️ PostgreSQL Output Preview</p>",
            unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Cleaned and structured data as loaded into the database</p>",
            unsafe_allow_html=True)

show_cols = [c for c in [
    "track_name", "artist_name", "album", "genre",
    "release_year", "duration_min", "popularity",
    "danceability", "energy", "valence", "explicit"
] if c in filtered.columns]

st.dataframe(
    filtered[show_cols].head(100).reset_index(drop=True),
    use_container_width=True, height=340,
)


# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("""
<hr class='custom-divider'>
<div style='text-align:center;color:#444;font-size:0.8rem;padding-bottom:20px;'>
  Built by
  <a href='https://linkedin.com/in/dharmikchampaneri' style='color:#1DB954;'>Dharmik Champaneri</a>
  &nbsp;·&nbsp;
  <a href='https://github.com/DevDharmik/Spotify-ETL-Project' style='color:#1DB954;'>GitHub</a>
  &nbsp;·&nbsp; M.Sc. Data Science · UE Germany
</div>
""", unsafe_allow_html=True)
