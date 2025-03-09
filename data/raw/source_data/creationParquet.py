import pandas as pd

def add_columns_and_create_parquet():
    # data = pd.read_csv("customer_churn_dataset_kaggle.csv")
    #
    # # Generate event timestamps (1 per row, backwards from now)
    # timestamps = pd.date_range(
    #     end=pd.Timestamp.now(), periods=len(data), freq="D"
    # ).to_series().reset_index(drop=True)
    #
    # data["event_timestamp"] = timestamps
    #
    # # Convert to datetime to ensure consistency
    # data["event_timestamp"] = pd.to_datetime(data["event_timestamp"])
    #
    # print(data.dtypes)  # Verify column types
    # print(data.head())   # Verify sample data
    #
    # # Save to Parquet
    # data.to_parquet("customer_churn_dataset_kaggle.parquet", index=False)


    df = pd.read_parquet("customer_churn_dataset_kaggle.parquet")
    df.info()
    print(df)


if __name__ == "__main__":
    add_columns_and_create_parquet()
