-- ============================================================
-- Spotify ETL Pipeline — PostgreSQL Schema
-- Author: Dharmik Champaneri
-- ============================================================

CREATE TABLE IF NOT EXISTS spotify_tracks (
    track_id        VARCHAR(50)     PRIMARY KEY,
    track_name      VARCHAR(255)    NOT NULL,
    artist_name     VARCHAR(255),
    album           VARCHAR(255),
    release_date    DATE,
    duration_min    FLOAT,
    popularity      INTEGER         CHECK (popularity BETWEEN 0 AND 100),
    explicit        BOOLEAN         DEFAULT FALSE,
    loaded_at       TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast artist lookups
CREATE INDEX IF NOT EXISTS idx_artist_name
    ON spotify_tracks (artist_name);

-- Index for date-range queries
CREATE INDEX IF NOT EXISTS idx_release_date
    ON spotify_tracks (release_date);
