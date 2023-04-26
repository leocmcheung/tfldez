import requests
import pandas as pd
from datetime import timedelta, date
from prefect import flow, task
import os
from prefect_gcp import GcpCredentials
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
    Calculate the prefix ID for each csv file, for reference the first file
    starting on 5 Jan 2022 is numbered 299.
    Using this as ref point for later prefix
    '''

    dataset_id = 299 + round((start_date - date(2022, 1, 5)).days / 7)
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
        print('File already exists :) ')
        return file_wext
    if f'{file}.parquet' in os.listdir('../data/'):
        print('File has been downloaded and cleaned! Proceed to next')
        return f'{file}.parquet'
    url = f"https://cycling.data.tfl.gov.uk/usage-stats/{file_wext}"
    print(f'Downloading {url} ...')

    dl = requests.get(url, file_wext)
    open(f'../data/{file_wext}', 'wb').write(dl.content)

    print('Done')
    return file_wext


@task(name='data_clean')
def data_clean(filename):
    if filename.endswith('.csv'):
        df = pd.read_csv(f'../data/{filename}')
        if len(df.columns) <= 9:
            print(f'Data cleaning on {filename}')
            return old_cleaning(filename)
        print(f'Dataa cleaning on {filename}')
        return newstyle_cleaning(filename)

    if filename.endswith('.parquet'):
        print(f'Cleaning is already done on {filename}')
        return pd.read_parquet(f'../data/{filename}')


def old_cleaning(file_path):
    df = pd.read_csv(f'../data/{file_path}')
    df['EndStation Id'] = df['EndStation Id'].fillna(999999).astype('int64') \
        if 'EndStation Id' in df.columns else 999999
    df['EndStation Name'] = df['EndStation Name'].fillna('unknown')
    df['EndStation Name'] = df['EndStation Name'].replace(" ,", ",")
    df['StartStation Id'] = df['EndStation Id'].fillna(999999).astype('int64')
    df['StartStation Name'] = df['StartStation Name'].fillna('unknown')
    df['StartStation Name'] = df['StartStation Name'].replace(" ,", ",")
    df['Bike Id'] = df['Bike Id'].fillna(0)
    df['start_hour'] = pd.to_datetime(df['Start Date']).dt.time
    df['Start Date'] = pd.to_datetime(df['Start Date']).dt.date
    df['end_hour'] = pd.to_datetime(df['End Date']).dt.time
    df['End Date'] = pd.to_datetime(df['End Date']).dt.date
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

def newstyle_cleaning(file_path):
    df = pd.read_csv(f'../data/{file_path}')
    df.drop(columns='Total duration', inplace=True)
    df['Total duration (ms)'] = round(df['Total duration (ms)']/60)
    df['Start station number'] = df['Start station number'].replace('300006-1', '300006') \
        .replace('200217old2', '200217').replace('001057_old', '001057') \
            .fillna(999999).astype('int64')
    df['End station number'] = df['End station number'].replace('300006-1', '300006') \
        .replace('200217old2', '200217').replace('001057_old', '001057') \
            .fillna(999999).astype('int64')
    df['End station'] = df['End station'].fillna('unknown')
    df['End station'] = df['End station'].replace(" ,", ",")
    df['Start station'] = df['Start station'].fillna('unknown')
    df['Start station'] = df['Start station'].replace(" ,", ",")
    df['Bike number'] = df['Bike number'].fillna(0)
    df['start_hour'] = pd.to_datetime(df['Start date']).dt.time
    df['Start date'] = pd.to_datetime(df['Start date']).dt.date
    df['end_hour'] = pd.to_datetime(df['End date']).dt.time
    df['End date'] = pd.to_datetime(df['End date']).dt.date
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
    with open(r'../gcs_dataset_list.txt', 'a') as fp:
        fp.write("%s\n" % start)
    print('Done')



@task(name='write_to_GCS', description='Upload the file onto GCS Data Lake')
def write_gcs(file):
    gcs_block = GcsBucket.load("tfldez-bucket")
    print(f'Uploading {file} onto GCS Bucket')
    gcs_block.upload_from_path(from_path=f'../data/{file}', to_path=file)


@flow(name='etl_web_to_local', description='ETL the TfL files')
def etl_ingest():
    start = date(2022, 1, 5) # starting from 2022-01-05
    print(start)
    while start < date(2023, 4, 3):
        dates, end_date = start_end_dates(start)
        print(f'Getting dataset with start date {dates}...')
        dataset_id = calc_id(start)
        filename = download_file(dataset_id, dates)
        df = data_clean(filename)
        save_to_parquet(df, start)
        if filename.endswith('.csv'): os.system(f'rm ../data/{filename}')
        write_gcs(f'{start}.parquet')
        start = end_date + timedelta(days=1)


if __name__ == "__main__":
    etl_ingest()
