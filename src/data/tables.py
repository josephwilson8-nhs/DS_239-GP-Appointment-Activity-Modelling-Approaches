"""
Join data together to build tables
"""
from typing import List, Optional
import pandas as pd
from src.data import data_loading
from src.data.date_writing import write_interim_csv


def get_sub_icb_daily_df(
    return_subset_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    _summary_

    Parameters
    ----------
    return_subset_columns : List[str], optional
        Columns that you want to return, by default None, which will return all columns

    Returns
    -------
    pd.DataFrame
        Dataframe of the joined sub-icb daily data
    """
    raw_appointments_gp_daily_df = data_loading.get_raw_appointments_gp_daily_df()

    raw_appointments_gp_daily_df["Appointment_Date"] = pd.to_datetime(
        raw_appointments_gp_daily_df["Appointment_Date"], format="%d%b%Y"
    )

    if return_subset_columns:
        sub_icb_daily_df = raw_appointments_gp_daily_df.copy()[return_subset_columns]
    else:
        sub_icb_daily_df = raw_appointments_gp_daily_df.copy()

    return sub_icb_daily_df


# ! Current not functional as there are changes in column names between years... unhelpfully
def get_mapped_practice_prevalence_df(
    return_subset_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    _summary_

    Parameters
    ----------
    return_subset_columns : Optional[List[str]], optional
        _description_, by default None

    Returns
    -------
    pd.DataFrame
        _description_
    """
    raw_qof_datasets = data_loading.get_qof_datasets()

    prevalence_list = []
    mapping_df = pd.DataFrame()
    practice_codes = []

    for year, data_dict in raw_qof_datasets.items():
        prevalence_sub_df = data_dict["PREVALENCE"]

        prevalence_list.append(prevalence_sub_df)

        new_mapping_df = data_dict["MAPPING_NHS_GEOGRAPHIES"]

        if mapping_df.empty:
            mapping_df = new_mapping_df
            practice_codes = mapping_df["PRACTICE_CODE"]
        else:
            new_practice_code_mask = ~new_mapping_df["PRACTICE_CODE"].isin(
                practice_codes
            )
            new_filtered_mapping_df = new_mapping_df[new_practice_code_mask]
            mapping_df = pd.concat([mapping_df, new_filtered_mapping_df])

        print(year, "\n", mapping_df.shape[0])

    prevalence_df = pd.concat(prevalence_list)

    mapped_practice_prevalence_df = pd.merge(
        left=prevalence_df, right=mapping_df, how="left", on="PRACTICE_CODE"
    )

    print(mapped_practice_prevalence_df.isna().sum())

    return mapped_practice_prevalence_df


if __name__ == "__main__":
    sub_icb_daily_df = get_sub_icb_daily_df()
    print(sub_icb_daily_df.dtypes)
