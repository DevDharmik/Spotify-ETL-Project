from extract import extract_data
from transform import transform_data
from load import load_data


def run_pipeline():

    print("Starting ETL pipeline...")

    df = extract_data()
    print("Data extracted successfully")

    df_clean = transform_data(df)
    print("Data transformed")

    load_data(df_clean)
    print("Data loaded into PostgreSQL")

    print("ETL pipeline completed!")


if __name__ == "__main__":
    run_pipeline()