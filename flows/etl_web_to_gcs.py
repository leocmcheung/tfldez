import argparse
from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from typing import List
# from prefect.tasks import task_input_hash
from params import get_start_date, get_end_date, get_url, get_start_wk_number
from datetime import timedelta, date

# _DEBUG = True

@task(name="write_gcs", description='Write file from local to gcs', log_prints=False)
def write_gcs(local_path: Path, file_name: str) -> None:

    """
        Upload locally saved file to GCS
        Args:
            path: File location
            prefix: the folder location on storage
    """
    gcs_path = f'{file_name}.parquet'
    print(f'tfl-gcs {gcs_path}')

    gcs_block = GcsBucket.load("tfl-gcs")
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)

    return

@task(name='write_local', description='Writes the file into a local folder')
def write_local(df: pd.DataFrame, file_path: Path) -> Path:
    """
        Write DataFrame out locally as csv file
        Args:
            df: dataframe chunk
            folder: the download data folder
            file_name: the local file name
    """

    path = Path(f"data/{file_path}.parquet")
    df.to_parquet(path, comparession="gzip")

    return path

@flow(name='etl_web_to_local', description='Download MTA File in chunks')
def etl_web_to_local(name: str) -> Path:
    """
       Download a file
       Args:
            name : the file name

    """

    # skip an existent file
    path = f"../data/"
    file_path = Path(f"{path}/{name}.csv")
    if os.path.exists(file_path):
            print(f'{name} already processed')
            return file_path

    url = get_url()
    file_url = f'{url}/{name}.csv'
    print(file_url)
    # os.system(f'wget {url} -O {name}.csv')
    # return
    print(f"Tfl Bike Data was downloaded to {file_path}")

    return file_path

@task(name='get_file_date', description='Resolves the last file drop date')
def get_file_date(curr_date: date = date.today()) -> str:
    if curr_date.weekday() != 2:
        days_to_sat = (curr_date.weekday() - 5) % 7
        curr_date = curr_date - timedelta(days=days_to_sat)

    year_tag = str(curr_date.year)[2:4]
    file_name = f'{year_tag}{curr_date.month:02}{curr_date.day:02}'
    return file_name


@task(name='get_the_file_name', description='Formatting dates to fit download path')
def get_the_filename(year: int, month: int, day: int = 1) -> List[str]:
    """
        Args:
            year : the selected year
            month : the selected month
            day:  the file day
    """
    start_date = get_start_date()
    end_date = get_end_date()

    wanted_date = date(year, month, day)
    wanted_date_fmt = wanted_date.strftime('%d%b%Y')
    wanted_date_plus6 = (wanted_date + timedelta(days=6)).strftime('%d%b%Y')
    start_wk = get_start_wk_number()
    week_elapsed = (wanted_date - start_date) / 7
    file_name = f'{week_elapsed+start_wk}JourneyExtract{wanted_date_fmt}-{wanted_date_plus6}'
    return file_name



@task(name='valid_task', description='Validate the tasks input paranmeter')
def valid_task(year: int, month: int, day: int = 1) -> bool:
    """
        Validates the input parameters for the request
         Args:
            year : the selected year
            month : the selected month
            day: file day
    """
    isValid = False
    if month > 0 and month < 13:
        curr_date = date(year, month, day)
        min_date = get_start_date()
        max_date = get_end_date()
        isValid =  curr_date >= min_date and curr_date < max_date and curr_date <= date.today()

    print(f'task request status {isValid} input {year}-{month}')
    return isValid


@flow (name="TflBike Batch flow", description="TfL Multiple File Batch Data Flow. Defaults to the latest Wednesday")
def main_flow(year: int = 0 , month: int = 0, day: int = 0) -> None:
    """
        Main ETL function
    """
    url_prefix = get_url()
    filename = get_the_filename(year, month, day)
    url = f"{url_prefix}{filename}.csv"
    df = pd.read_csv(url)
    # Maybe clean a bit
    path = write_local(df,filename)
    write_gcs(path, filename)

if __name__ == '__main__':
    """main entry point with argument parser"""
    print('Processing data load')
    parser = argparse.ArgumentParser(description='Download the Tfl bike data')

    parser.add_argument('--year', required=True, help='File year')
    parser.add_argument('--month', required=True, help='File month')
    parser.add_argument('--day', required=False, help='File day')
    args = parser.parse_args()

    year = int(args.year)
    month = int(args.month)
    day = 1 if args.day == None else int(args.day)

    main_flow(year, month, day)

# Link files have this format
# http://web.mta.info/developers/data/nyct/turnstile/turnstile_230318.txt
# files are published every Sunday format turnstile_yyMMdd.txt
# python3 etl_web_to_gcs.py --year 2023 --month 3 --day
