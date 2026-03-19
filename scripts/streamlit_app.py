"""
Spotify ETL Pipeline — Interactive Dashboard
=============================================
3D & animated Streamlit dashboard for the Spotify ETL project.
Author: Dharmik Champaneri
"""

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

# ── Particle Animation + Custom CSS ───────────────────────────────────────────

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
    border: 1px solid rgba(29, 185, 84, 0.3);
    border-radius: 16px; padding: 20px 24px;
    text-align: center;
    transition: transform 0.3s ease, box-shadow 0.3s ease;
    position: relative; overflow: hidden;
  }
  .metric-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 8px 32px rgba(29, 185, 84, 0.2);
  }
  .metric-value {
    font-size: 2.2rem; font-weight: 700;
    color: #1DB954; margin: 0; line-height: 1.2;
  }
  .metric-label {
    font-size: 0.85rem; color: #aaaaaa;
    margin-top: 6px; text-transform: uppercase; letter-spacing: 0.08em;
  }
  .metric-delta { font-size: 0.78rem; color: #1DB954; margin-top: 4px; }
  .section-header {
    font-size: 1.1rem; font-weight: 600; color: #ffffff;
    margin-bottom: 4px;
  }
  .section-sub { font-size: 0.82rem; color: #888888; margin-bottom: 16px; }
  .hero-title {
    font-size: 2.6rem; font-weight: 700;
    background: linear-gradient(135deg, #1DB954 0%, #1ed760 50%, #17a349 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; margin: 0; line-height: 1.1;
  }
  .hero-sub { font-size: 1rem; color: #888888; margin-top: 8px; }
  .hero-badge {
    display: inline-block;
    background: rgba(29,185,84,0.15);
    border: 1px solid rgba(29,185,84,0.4);
    color: #1DB954; font-size: 0.75rem;
    padding: 4px 12px; border-radius: 99px;
    margin-top: 12px; font-weight: 500;
  }
  .custom-divider {
    border: none; height: 1px;
    background: linear-gradient(90deg, transparent, rgba(29,185,84,0.4), transparent);
    margin: 24px 0;
  }
  .pipeline-step {
    background: #111118;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px; padding: 16px 18px;
    text-align: center; transition: border-color 0.3s;
  }
  .pipeline-step:hover { border-color: rgba(29,185,84,0.5); }
  .pipeline-icon { font-size: 1.8rem; margin-bottom: 8px; }
  .pipeline-label { font-size: 0.88rem; font-weight: 600; color: #ffffff; }
  .pipeline-desc { font-size: 0.75rem; color: #888888; margin-top: 4px; }
  [data-testid="stSidebar"] {
    background: #0d0d14 !important;
    border-right: 1px solid rgba(29,185,84,0.15);
  }
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #0a0a0f; }
  ::-webkit-scrollbar-thumb { background: #1DB954; border-radius: 3px; }
</style>

<div id="particles-js"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/particles.js/2.0.0/particles.min.js"></script>
<script>
  window.addEventListener('load', function() {
    if (typeof particlesJS !== 'undefined') {
      particlesJS('particles-js', {
        particles: {
          number: { value: 80, density: { enable: true, value_area: 900 } },
          color: { value: ['#1DB954', '#17a349', '#1ed760', '#ffffff'] },
          shape: { type: 'circle' },
          opacity: { value: 0.35, random: true,
            anim: { enable: true, speed: 0.8, opacity_min: 0.05, sync: false }
          },
          size: { value: 2.5, random: true,
            anim: { enable: true, speed: 2, size_min: 0.5, sync: false }
          },
          line_linked: {
            enable: true, distance: 130, color: '#1DB954',
            opacity: 0.12, width: 1
          },
          move: {
            enable: true, speed: 0.8, direction: 'none',
            random: true, straight: false, out_mode: 'out', bounce: false
          }
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


# ── Data ──────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    np.random.seed(42)
    n = 500
    artists = [
        "The Weeknd", "Drake", "Taylor Swift", "Ed Sheeran", "Billie Eilish",
        "Dua Lipa", "Post Malone", "Ariana Grande", "Bad Bunny", "Harry Styles",
        "Olivia Rodrigo", "Justin Bieber", "Doja Cat", "SZA", "Kendrick Lamar",
        "Coldplay", "BTS", "Adele", "Eminem", "Rihanna"
    ]
    genres  = ["Pop", "Hip-Hop", "R&B", "Rock", "Electronic", "Latin", "K-Pop"]
    albums  = [f"Album {i}" for i in range(1, 40)]
    return pd.DataFrame({
        "track_id":     [f"id_{i:04d}" for i in range(n)],
        "track_name":   [f"Track {i}" for i in range(n)],
        "artist_name":  np.random.choice(artists, n),
        "album":        np.random.choice(albums, n),
        "genre":        np.random.choice(genres, n),
        "release_year": np.random.randint(2015, 2025, n),
        "duration_min": np.round(np.random.uniform(1.5, 6.5, n), 2),
        "popularity":   np.random.randint(20, 100, n),
        "explicit":     np.random.choice([True, False], n, p=[0.35, 0.65]),
        "danceability": np.round(np.random.uniform(0.2, 1.0, n), 3),
        "energy":       np.round(np.random.uniform(0.2, 1.0, n), 3),
        "valence":      np.round(np.random.uniform(0.1, 1.0, n), 3),
    })

df = load_data()

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding:12px 0 20px;'>
      <div style='font-size:2.5rem;'>🎵</div>
      <div style='font-size:1rem; font-weight:600; color:#1DB954; margin-top:6px;'>Spotify ETL</div>
      <div style='font-size:0.75rem; color:#666; margin-top:2px;'>Pipeline Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**🎛️ Filters**")
    selected_genres = st.multiselect(
        "Genre", options=sorted(df["genre"].unique()),
        default=sorted(df["genre"].unique())
    )
    year_range  = st.slider("Release Year", 2015, 2024, (2015, 2024))
    pop_range   = st.slider("Popularity",   0,    100,  (0, 100))
    show_explicit = st.toggle("Include Explicit Tracks", value=True)

    st.markdown("<hr style='border-color:rgba(255,255,255,0.08);margin:20px 0;'>", unsafe_allow_html=True)
    st.markdown("""
    <div style='font-size:0.75rem; color:#555;'>
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

# ── Hero ──────────────────────────────────────────────────────────────────────

st.markdown("""
<div style='padding:10px 0 4px;'>
  <p class='hero-title'>🎵 Spotify ETL Dashboard</p>
  <p class='hero-sub'>End-to-end pipeline · Extract · Transform · Load · Visualise</p>
  <span class='hero-badge'>▶ Live Pipeline · PostgreSQL · Python · pandas · SQLAlchemy</span>
</div>
<hr class='custom-divider'>
""", unsafe_allow_html=True)

# ── KPI Cards ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
for col, icon, val, label, delta in [
    (c1, "🎵", f"{len(filtered):,}",               "Total Tracks",   f"of {len(df):,} loaded"),
    (c2, "🎤", f"{filtered['artist_name'].nunique()}","Artists",       "unique in dataset"),
    (c3, "⭐", f"{filtered['popularity'].mean():.1f}","Avg Popularity","out of 100"),
    (c4, "⏱️", f"{filtered['duration_min'].mean():.2f} min","Avg Duration","per track"),
    (c5, "🔥", f"{filtered['explicit'].sum():,}",   "Explicit Tracks",f"{filtered['explicit'].mean()*100:.0f}% of total"),
]:
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
st.markdown("<p class='section-sub'>Track the journey from raw CSV to structured PostgreSQL database</p>", unsafe_allow_html=True)

p1, a1, p2, a2, p3, a3, p4 = st.columns([2, 0.4, 2, 0.4, 2, 0.4, 2])
for col, icon, label, desc in [
    (p1, "📥", "EXTRACT",   "Raw CSV ingested via pandas"),
    (p2, "🔧", "TRANSFORM", "Clean · Deduplicate · Type-cast"),
    (p3, "🗄️",  "LOAD",      "Write to PostgreSQL via SQLAlchemy"),
    (p4, "📊", "VISUALISE", "Insights via Streamlit dashboard"),
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
        st.markdown("<div style='text-align:center;font-size:1.4rem;color:#1DB954;padding-top:16px;'>➜</div>",
                    unsafe_allow_html=True)

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)

# ── 3D Scatter + Top Artists ──────────────────────────────────────────────────

col1, col2 = st.columns([3, 2])

with col1:
    st.markdown("<p class='section-header'>🌐 3D Popularity Space</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Popularity · Duration · Energy — coloured by Genre</p>", unsafe_allow_html=True)
    fig = px.scatter_3d(
        filtered.sample(min(300, len(filtered)), random_state=42),
        x="popularity", y="duration_min", z="energy",
        color="genre", size="popularity", size_max=10, opacity=0.75,
        hover_data=["track_name", "artist_name"],
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig.update_layout(
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
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("<p class='section-header'>🎤 Top Artists</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Most represented artists in the dataset</p>", unsafe_allow_html=True)
    top = filtered["artist_name"].value_counts().head(10).reset_index()
    top.columns = ["artist", "count"]
    fig2 = px.bar(top, x="count", y="artist", orientation="h",
                  color="count", color_continuous_scale=["#0d4a1f","#1DB954","#1ed760"])
    fig2.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=420,
        coloraxis_showscale=False,
        yaxis=dict(categoryorder="total ascending", color="#aaa", gridcolor="#111118"),
        xaxis=dict(color="#aaa", gridcolor="#111118"),
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Histogram + Area Chart ────────────────────────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.markdown("<p class='section-header'>📈 Popularity Distribution</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>How popular are tracks across the dataset?</p>", unsafe_allow_html=True)
    fig3 = px.histogram(filtered, x="popularity", nbins=30,
                        color_discrete_sequence=["#1DB954"])
    fig3.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300, bargap=0.05,
        xaxis=dict(color="#888", gridcolor="#111118"),
        yaxis=dict(color="#888", gridcolor="#111118"),
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown("<p class='section-header'>📅 Tracks Per Year</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Volume of tracks loaded by release year</p>", unsafe_allow_html=True)
    yearly = filtered.groupby("release_year").size().reset_index(name="count")
    fig4 = px.area(yearly, x="release_year", y="count",
                   color_discrete_sequence=["#1DB954"], line_shape="spline")
    fig4.update_traces(fill="tozeroy", fillcolor="rgba(29,185,84,0.15)",
                       line_color="#1DB954", line_width=2.5)
    fig4.update_layout(
        paper_bgcolor="#0a0a0f", plot_bgcolor="#0a0a0f",
        font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        xaxis=dict(color="#888", gridcolor="#111118"),
        yaxis=dict(color="#888", gridcolor="#111118"),
    )
    st.plotly_chart(fig4, use_container_width=True)

# ── Donut + Radar ─────────────────────────────────────────────────────────────

col5, col6 = st.columns(2)

with col5:
    st.markdown("<p class='section-header'>🎸 Tracks by Genre</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Distribution of genres in the pipeline output</p>", unsafe_allow_html=True)
    gc = filtered["genre"].value_counts().reset_index()
    gc.columns = ["genre", "count"]
    fig5 = px.pie(gc, names="genre", values="count",
                  color_discrete_sequence=px.colors.sequential.Greens_r, hole=0.5)
    fig5.update_layout(
        paper_bgcolor="#0a0a0f", font_color="#ccc", font_family="Inter",
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#aaa"),
    )
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.markdown("<p class='section-header'>🕸️ Audio Features by Genre</p>", unsafe_allow_html=True)
    st.markdown("<p class='section-sub'>Danceability · Energy · Valence radar</p>", unsafe_allow_html=True)
    radar = filtered.groupby("genre")[["danceability","energy","valence"]].mean().reset_index()
    cats  = ["Danceability","Energy","Valence"]
    colors= ["#1DB954","#1ed760","#17a349","#0f7a35","#0d6b2f","#0a5c28","#074d21"]
    fig6  = go.Figure()
    for i, row in radar.iterrows():
        v = [row["danceability"], row["energy"], row["valence"]]
        fig6.add_trace(go.Scatterpolar(
            r=v+[v[0]], theta=cats+[cats[0]],
            fill="toself", name=row["genre"],
            line_color=colors[i % len(colors)],
            fillcolor=f"rgba(29,185,84,{0.04+i*0.02})",
            line_width=1.5, opacity=0.85,
        ))
    fig6.update_layout(
        polar=dict(
            bgcolor="#0d0d14",
            radialaxis=dict(visible=True, range=[0,1], color="#555", gridcolor="#1a1a2e"),
            angularaxis=dict(color="#888", gridcolor="#1a1a2e"),
        ),
        paper_bgcolor="#0a0a0f", font_color="#ccc", font_family="Inter",
        margin=dict(l=20, r=20, t=10, b=10), height=300,
        legend=dict(bgcolor="rgba(0,0,0,0)", font_color="#aaa", font_size=11),
    )
    st.plotly_chart(fig6, use_container_width=True)

# ── Data Table ────────────────────────────────────────────────────────────────

st.markdown("<hr class='custom-divider'>", unsafe_allow_html=True)
st.markdown("<p class='section-header'>🗃️ PostgreSQL Output Preview</p>", unsafe_allow_html=True)
st.markdown("<p class='section-sub'>Cleaned and structured data as it appears after the ETL load step</p>", unsafe_allow_html=True)

st.dataframe(
    filtered[["track_id","track_name","artist_name","album","genre",
              "release_year","duration_min","popularity","explicit"]].head(50).reset_index(drop=True),
    use_container_width=True, height=320,
)

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("""
<hr class='custom-divider'>
<div style='text-align:center; color:#444; font-size:0.8rem; padding-bottom:20px;'>
  Built by <a href='https://linkedin.com/in/dharmikchampaneri' style='color:#1DB954;'>Dharmik Champaneri</a>
  &nbsp;·&nbsp;
  <a href='https://github.com/DevDharmik/Spotify-ETL-Project' style='color:#1DB954;'>GitHub</a>
  &nbsp;·&nbsp; M.Sc. Data Science · UE Germany
</div>
""", unsafe_allow_html=True)
