"""
_summary_
"""
from typing import Dict, List, Tuple
import zipfile
import pandas as pd

from src.data.date_writing import write_interim_csv


def load_compressed_monthly_data(
    file_path: str, prefix: str = "", suffix: str = "", skip_files_names: List[str] = []
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Reads CSV files from a compressed zip file and returns a pandas dataframe.

    Parameters
    ----------
    file_path : str
        The path to the zip file containing the CSVs.
    prefix : str, optional
        The prefix that the CSV files should start with, by default ""
    suffix : str, optional
        The suffix that the CSV files should end with, by default ""
    skip_file_names : List[str], optional
        A list of files that should be skipped, because they have been already extracted
        from a different zip folder covering a more recent date range, by default []

    Returns
    -------
    pd.DataFrame
        A dataframe containing all the data from the CSV files, with a new column "Month_Year"
        indicating the source month and year of each row.
    """
    # create an empty list to store dataframes
    df_list = []

    # open the zip file and iterate over the CSV files
    with zipfile.ZipFile(file_path) as z:
        file_name_list = z.namelist()

        for file_name in file_name_list:
            # check if the file is a CSV
            if (
                file_name.startswith(prefix)
                and file_name.endswith(suffix)
                and file_name not in skip_files_names
            ):
                # read the CSV into a dataframe
                file_df = pd.read_csv(z.open(file_name))

                # extract the month and year from the file name
                month_year_str = file_name.split(".")[0][-6:]

                # add a new column to the dataframe with the month and year
                file_df["date"] = pd.to_datetime(month_year_str, format="%b_%y")

                # append the dataframe to the list
                df_list.append(file_df)

    # concatenate all dataframes in the list into a single dataframe
    result = pd.concat(df_list, ignore_index=True)

    return (result, file_name_list)


def load_compressed_qof_data(
    file_path: str,
    suffix: str = ".csv",
) -> Dict[str, pd.DataFrame]:
    """
    _summary_

    Parameters
    ----------
    file_path : str
        _description_

    Returns
    -------
    dict[str, pd.DataFrame]
        _description_
    """
    df_dict = {}

    QOF_range_str = file_path[-8:-4]

    with zipfile.ZipFile(file_path) as z:
        for file_name in z.namelist():
            if file_name.endswith(suffix):
                index = file_name.find(QOF_range_str)

                df_key = file_name[:index] if index != -1 else ""

                file_df = pd.read_csv((z.open(file_name)), encoding="cp1252")

                file_df["DATE_RANGE"] = QOF_range_str

                df_dict[df_key[:-1]] = file_df

    return df_dict


def get_raw_appointments_gp_daily_df() -> pd.DataFrame:
    """
    _summary_

    Returns
    -------
    pd.DataFrame
        _description_
    """
    month_year_list = ["Mar_23", "Sep_22"]
    skip_files_names = []
    compressed_monthly_data_list = []

    for month_year in month_year_list:
        file_path = f"data/raw/Appointments_GP_Daily_CSV_{month_year}.zip"

        compressed_monthly_data_df, skip_files_names = load_compressed_monthly_data(
            file_path=file_path,
            prefix="SUB_ICB_LOCATION_CSV_",
            suffix=".csv",
            skip_files_names=skip_files_names,
        )

        compressed_monthly_data_list.append(compressed_monthly_data_df)

    raw_appointments_gp_daily_df = pd.concat(
        compressed_monthly_data_list, ignore_index=True
    )

    return raw_appointments_gp_daily_df


def get_qof_datasets(
    qof_range_list: List[str] = ["1920", "2021", "2122"]
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    _summary_

    Parameters
    ----------
    qof_range_list : list[str], optional
        _description_, by default ["1920", "2021", "2122"]

    Returns
    -------
    dict[str, dict[str, pd.DataFrame]]
        _description_
    """
    qof_datasets = {}

    qof_range_list.reverse()

    for qof_range in qof_range_list:
        file_path = f"data/raw/QOF_{qof_range}.zip"

        qof_datasets[qof_range] = load_compressed_qof_data(file_path=file_path)

    return qof_datasets


if __name__ == "__main__":
    # raw_appointments_gp_daily_df = get_raw_appointments_gp_daily_df()

    # write_interim_csv(
    #     write_df=raw_appointments_gp_daily_df, file_name="raw_appointments_gp_daily_df"
    # )

    print(get_qof_datasets()["2122"].keys())
