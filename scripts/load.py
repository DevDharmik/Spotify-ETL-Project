from sqlalchemy import create_engine

def load_data(df):

    # PostgreSQL connection
    engine = create_engine(
        "postgresql://postgres:Dharm%4007@localhost:5432/spotify_db"
    )

    # Load dataframe to PostgreSQL table
    df.to_sql(
        "spotify_tracks",
        engine,
        if_exists="replace",
        index=False
    )

    print("Data loaded into PostgreSQL")