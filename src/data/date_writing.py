import pandas as pd


def write_interim_csv(write_df: pd.DataFrame, file_name: str):
    """
    Helper function to write an interim dataset to a CSV. Writes to ./data/interim.

    Parameters
    ----------
    write_df : pd.DataFrame
        dataframe to write to a csv file
    file_name : str
        name of the csv file
    """
    write_df.to_csv(path_or_buf=f"./data/interim/{file_name}.csv")
