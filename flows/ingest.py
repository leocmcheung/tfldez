import requests
import pandas as pd
from datetime import timedelta, date
from prefect import flow, task
import os
from prefect_gcp.cloud_storage import GcsBucket
# gcp_cloud_storage_bucket_block = GcsBucket.load("tfldez-bucket")
# gcp_block = GcpCredentials.load("tfldez-gcp-creds")

@task(name='start_and_end_dates', description = 'Getting start and end dates')
def start_end_dates(start_date):

    '''
    Take in a date and convert it into Tfl filename format as start date
    Also calculate the end date by adding 6 days
    '''
    if start_date == date(2022,9,7):
        start_date_fmt = start_date.strftime('%d%b%Y')
        end_date = start_date + timedelta(days=4)
        end_date_fmt = end_date.strftime('%d%b%Y')
        return f'{start_date_fmt}-{end_date_fmt}', end_date

    start_date_fmt = start_date.strftime('%d%b%Y')
    end_date = start_date + timedelta(days=6)
    end_date_fmt = end_date.strftime('%d%b%Y')

    return f'{start_date_fmt}-{end_date_fmt}', end_date

@task(name='Calc_ID', description='Calculate ID prefix')
def calc_id(start_date):
    '''
    Calculate the prefix ID for each csv file, for reference the file
    starting on 3 Jan 2018 is numbered 91
    Using this as ref point for later prefix
    Note there is a double ID 246 for both 23 Dec 2020 and 30 Dec 2020
    thus the if statement
    '''
    dataset_id = (91 if start_date < date(2020, 12, 30) else 90) + \
        round((start_date - date(2018, 1, 3)).days / 7)
    print(f'The dataset ID is {dataset_id}')
    return dataset_id

@task(name='download', description='Download the file and store in csv form')
def download_file(dataset_id, dates):
    '''
    Concatenating dataset_id and dates to produce the correct filename,
    and download the file in csv format
    '''

    file = f'{dataset_id}JourneyDataExtract{dates}'
    file_ext = '.csv'
    file_wext = f'{file}{file_ext}'
    if file_wext in os.listdir('../data/'):
        print('File already exists :)')
        return file_wext
    url = f"https://cycling.data.tfl.gov.uk/usage-stats/{file_wext}"
    print(f'Downloading {url} ...')

    dl = requests.get(url, file_wext)
    open(f'../data/{file_wext}', 'wb').write(dl.content)

    print('Done')
    return file_wext


@task(name='data_clean')
def data_clean(filename) -> pd.DataFrame:
    '''
    Clean the data according to when the dataset was created:
    - If dataset was before 12 Sep 2022 do the old-style data cleaning old_cleaning()
    - From 12 Sep 2022 electric bikes were introduced and dataset format was
      updated. Datasets would be cleaned using new_cleaning()
    '''
    if filename.endswith('.csv'):
        df = pd.read_csv(f'../data/{filename}')
        if len(df.columns) <= 9:
            print(f'Data cleaning on {filename}')
            return old_cleaning(filename) # function returns df
        print(f'Data cleaning on {filename}')
        return newstyle_cleaning(filename) # function returns df
    return NameError


def old_cleaning(file_path) -> pd.DataFrame:
    '''
    For datasets before 12 Sep 2022 there are 9 columns.
    1. Fill the start and end station IDs and names. One of the datasets do not
       have 'EndStation Id' somehow so make a dummy column of values 999999
    2. Fill the Bike Id NA values
    3. Convert date values to datetime format
    4. Ensure journey duration column is in integer dtype
    5. Rename the columns for easy BQ reading and SQL querying
    6. Create a new column 'bike_model' which denotes the type of bike hired (
       which would be CLASSIC as electric bikes were introduced)
    '''
    df = pd.read_csv(f'../data/{file_path}')
    df['EndStation Id'] = df['EndStation Id'].fillna(999999).astype('int64') \
        if 'EndStation Id' in df.columns else 999999
    df['EndStation Name'] = df['EndStation Name'].fillna('unknown').replace(" ,", ",")
    df['StartStation Id'] = df['StartStation Id'].fillna(999999).astype('int64')
    df['StartStation Name'] = df['StartStation Name'].fillna('unknown').replace(" ,", ",")
    df['Bike Id'] = df['Bike Id'].fillna(0)
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['End Date'] = pd.to_datetime(df['End Date'])
    df['Duration'] = df['Duration'].astype('int64')
    df.rename(columns={
        'Rental Id': 'rental_id',
        'Start Date': 'start_date',
        'StartStation Id': 'startstation_id',
        'StartStation Name': 'startstation_name',
        'End Date': 'end_date',
        'EndStation Id': 'endstation_id',
        'EndStation Name': 'endstation_name',
        'Bike Id': 'bike_id',
        'Duration': 'duration'
    }, inplace=True)
    df['bike_model'] = 'CLASSIC'
    return df

def newstyle_cleaning(file_path) -> pd.DataFrame:
    '''
    For datasets after 12 Sep 2022 there are 11 columns.
    1. Remove 'Total duration' column as it is essentially the same as
       'Total duration (ms)' data-wise
    2. Convert 'Total duration (ms)' data from miliseconds to minutes
    3. Fill the start and end station IDs and names. Several station IDs were in
       alternative forms; converted them to integers
    4. Fill the Bike Id NA values
    5. Convert date values to datetime format
    6. Rename the columns for easy BQ reading and SQL querying
    '''
    df = pd.read_csv(f'../data/{file_path}')
    df.drop(columns='Total duration', inplace=True)
    df['Total duration (ms)'] = round(df['Total duration (ms)']/1000).astype('int64')
    df['Start station number'] = df['Start station number'].replace('300006-1', '300006') \
        .replace('200217old2', '200217').replace('001057_old', '001057') \
            .fillna(999999).astype('int64')
    df['End station number'] = df['End station number'].replace('300006-1', '300006') \
        .replace('200217old2', '200217').replace('001057_old', '001057') \
            .fillna(999999).astype('int64')
    df['End station'] = df['End station'].fillna('unknown').replace(" ,", ",")
    df['Start station'] = df['Start station'].fillna('unknown').replace(" ,", ",")
    df['Bike number'] = df['Bike number'].fillna(0)
    df['Start date'] = pd.to_datetime(df['Start date'])
    df['End date'] = pd.to_datetime(df['End date'])
    df.rename(columns={
        'Number': 'rental_id',
        'Start date':'start_date',
        'Start station number': 'startstation_id',
        'Start station': 'startstation_name',
        'End date': 'end_date',
        'End station number':'endstation_id',
        'End station': 'endstation_name',
        'Bike number': 'bike_id',
        'Bike model': 'bike_model',
        'Total duration (ms)': 'duration'
    }, inplace=True)
    return df

@task(name='save_to_parquet')
def save_to_parquet(df, start):
    print(f'Saving {start} as parquet format...')
    df.to_parquet(f'../data/{start}.parquet', compression='gzip')
    print('Done')
    return

@task(name='write_to_GCS', description='Upload the file onto GCS Data Lake')
def write_gcs(file):
    '''
    Please set the gcs block name as tfldez-bucket on Prefect
    '''
    gcs_block = GcsBucket.load("tfldez-bucket")
    print(f'Uploading {file} onto GCS Bucket')
    gcs_block.upload_from_path(from_path=f'../data/{file}', to_path=file)
    return


@flow(name='etl_web_to_local_and_gcs', description='ETL the TfL bike data both locally and to GCS')
def etl_ingest():
    start = date(2018, 1, 3) # project collects data starting from 2018-01-03
    print(start)
    while start < date(2023, 3, 31): # the last date of available data is week starting 2023-03-27
        dates, end_date = start_end_dates(start)
        print(f'Getting dataset with start date {dates}...')
        dataset_id = calc_id(start)
        if 110 < dataset_id < 121: # for dataset_id between these value (year 2018) June and July do not follow 3-letter abbrevations
            dates = dates.replace("Jun","June").replace("Jul","July")
        filename = download_file(dataset_id, dates)
        df = data_clean(filename)
        save_to_parquet(df, start) # parquet files are 1/10 of size compared to csv
        if filename.endswith('.csv'): os.system(f'rm ../data/{filename}') # remove local csv files to save space
        write_gcs(f'{start}.parquet')
        start = end_date + timedelta(days=1)


if __name__ == "__main__":
    etl_ingest()
