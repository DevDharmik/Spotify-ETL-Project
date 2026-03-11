import pandas as pd

def transform_data(df):

    # remove unnecessary column if it exists
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    # remove duplicates
    df = df.drop_duplicates(subset=['track_id'])

    # convert duration to minutes
    df['duration_min'] = df['duration_ms'] / 60000

    # create popularity level
    df['popularity_level'] = pd.cut(
        df['popularity'],
        bins=[0,40,70,100],
        labels=['Low','Medium','High']
    )

    return df