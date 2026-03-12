import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------

st.set_page_config(
    page_title="Spotify Analytics Dashboard",
    page_icon="🎵",
    layout="wide"
)

st.title("🎵 Spotify Data Engineering Dashboard")
st.markdown("Interactive analytics powered by PostgreSQL and Streamlit")

# ---------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------

engine = create_engine(
    "postgresql://postgres:Dharm%4007@localhost:5432/spotify_db"
)

@st.cache_data
def load_data():
    query = "SELECT * FROM spotify_tracks"
    df = pd.read_sql(query, engine)
    return df

df = load_data()

# ---------------------------------------------------
# SIDEBAR FILTERS
# ---------------------------------------------------

st.sidebar.header("Filters")

artist_filter = st.sidebar.multiselect(
    "Artist",
    df["artist_name"].unique()
)

popularity_filter = st.sidebar.slider(
    "Popularity",
    int(df["popularity"].min()),
    int(df["popularity"].max()),
    (20,80)
)

duration_filter = st.sidebar.slider(
    "Duration (ms)",
    int(df["duration_ms"].min()),
    int(df["duration_ms"].max()),
    (
        int(df["duration_ms"].min()),
        int(df["duration_ms"].max())
    )
)

search_track = st.sidebar.text_input("Search Track")

# ---------------------------------------------------
# APPLY FILTERS
# ---------------------------------------------------

filtered_df = df.copy()

if artist_filter:
    filtered_df = filtered_df[
        filtered_df["artist_name"].isin(artist_filter)
    ]

filtered_df = filtered_df[
    (filtered_df["popularity"] >= popularity_filter[0]) &
    (filtered_df["popularity"] <= popularity_filter[1])
]

filtered_df = filtered_df[
    (filtered_df["duration_ms"] >= duration_filter[0]) &
    (filtered_df["duration_ms"] <= duration_filter[1])
]

if search_track:
    filtered_df = filtered_df[
        filtered_df["track_name"].str.contains(search_track, case=False)
    ]

# ---------------------------------------------------
# METRICS
# ---------------------------------------------------

st.subheader("Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Tracks",
    len(filtered_df)
)

col2.metric(
    "Total Artists",
    filtered_df["artist_name"].nunique()
)

col3.metric(
    "Average Popularity",
    round(filtered_df["popularity"].mean(),2)
)

col4.metric(
    "Average Duration (sec)",
    round(filtered_df["duration_ms"].mean()/1000,2)
)

st.divider()

# ---------------------------------------------------
# DATA TABLE
# ---------------------------------------------------

st.subheader("Track Dataset")

st.dataframe(
    filtered_df,
    use_container_width=True
)

# Download option
csv = filtered_df.to_csv(index=False)

st.download_button(
    "Download Filtered Data",
    csv,
    "spotify_filtered.csv",
    "text/csv"
)

st.divider()

# ---------------------------------------------------
# POPULARITY DISTRIBUTION
# ---------------------------------------------------

st.subheader("Popularity Distribution")

fig_pop = px.histogram(
    filtered_df,
    x="popularity",
    nbins=40,
    title="Track Popularity Distribution"
)

st.plotly_chart(fig_pop, use_container_width=True)

# ---------------------------------------------------
# TOP ARTISTS
# ---------------------------------------------------

st.subheader("Top Artists")

artist_counts = (
    filtered_df["artist_name"]
    .value_counts()
    .head(10)
)

fig_artist = px.bar(
    artist_counts,
    x=artist_counts.index,
    y=artist_counts.values,
    labels={
        "x":"Artist",
        "y":"Track Count"
    }
)

st.plotly_chart(fig_artist, use_container_width=True)

# ---------------------------------------------------
# AUDIO FEATURES ANALYSIS
# ---------------------------------------------------

st.subheader("Audio Feature Analysis")

feature = st.selectbox(
    "Select Feature",
    [
        "danceability",
        "energy",
        "loudness",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "valence",
        "tempo"
    ]
)

fig_feature = px.scatter(
    filtered_df,
    x=feature,
    y="popularity",
    color="artist_name",
    title=f"{feature} vs Popularity"
)

st.plotly_chart(fig_feature, use_container_width=True)

# ---------------------------------------------------
# DURATION ANALYSIS
# ---------------------------------------------------

st.subheader("Track Duration Analysis")

fig_duration = px.box(
    filtered_df,
    y="duration_ms",
    title="Track Duration Distribution"
)

st.plotly_chart(fig_duration, use_container_width=True)

# ---------------------------------------------------
# TOP TRACKS
# ---------------------------------------------------

st.subheader("Top 10 Most Popular Tracks")

top_tracks = (
    filtered_df
    .sort_values("popularity", ascending=False)
    .head(10)
)

fig_tracks = px.bar(
    top_tracks,
    x="track_name",
    y="popularity",
    color="artist_name"
)

st.plotly_chart(fig_tracks, use_container_width=True)

# ---------------------------------------------------
# FOOTER
# ---------------------------------------------------

st.markdown("---")

st.markdown(
"""
Built with:

- Python
- PostgreSQL
- Streamlit
- Plotly

ETL Pipeline → Database → Interactive Dashboard
"""
)
