import pandas as pd

def extract_data():

    df = pd.read_csv("E:\\spotify-etl-pipeline\\data\\spotify_tracks.csv")

    return df