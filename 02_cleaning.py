import pandas as pd
import os

def old_cleaning(file_path):
    df = pd.read_csv(file_path)
    df['EndStation Id'] = df['EndStation Id'].fillna(9999).astype('int64')
    df['EndStation Name'] = df['EndStation Name'].fillna('unknown')
    df['EndStation Name'] = df['EndStation Name'].replace(" ,", ",")
    df['StartStation Id'] = df['EndStation Id'].fillna(9999).astype('int64')
    df['StartStation Name'] = df['StartStation Name'].fillna('unknown')
    df['StartStation Name'] = df['StartStation Name'].replace(" ,", ",")
    df['Bike Id'] = df['Bike Id'].fillna(0)
    df['End Date'] = pd.to_datetime(df['End Date'])
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['Bike Model'] = 'CLASSIC'
    df.to_parquet(file_path[:-4], compression='gzip')

def newstyle_cleaning(file_path):
    df = pd.read_csv(file_path)
    df.drop(columns='Total duration', inplace=True)
    df.rename(columns={
        'Number': 'Rental Id',
        'Start date':'Start Date',
        'Start station number': 'StartStation Id',
        'Start station': 'StartStation Name',
        'End date': 'End Date',
        'End station number':'EndStation Id',
        'End station': 'EndStation Name',
        'Bike number': 'Bike Id',
        'Bike model': 'Bike Model',
        'Total duration (ms)': 'Duration'
    }, inplace=True)
    df['Duration'] = round(df['Duration']/60)
    df['StartStation Id'] = df['StartStation Id'].replace('300006-1', '300006').fillna(999999).astype('int64')
    df['EndStation Id'] = df['EndStation Id'].replace('300006-1', '300006').fillna(999999).astype('int64')
    df['EndStation Name'] = df['EndStation Name'].fillna('unknown')
    df['EndStation Name'] = df['EndStation Name'].replace(" ,", ",")
    df['StartStation Name'] = df['StartStation Name'].fillna('unknown')
    df['StartStation Name'] = df['StartStation Name'].replace(" ,", ",")
    df['Bike Id'] = df['Bike Id'].fillna(0)
    df['End Date'] = pd.to_datetime(df['End Date'])
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df.to_parquet(file_path[:-4], compression='gzip')


if __name__ == '__main__':
    for file in os.listdir('data'):
        file_path = os.path.join('data',file)
        print(file_path)
        if file.endswith('csv'):
            df = pd.read_csv(file_path)
            old_cleaning(file_path) if len(df.columns) == 9 else newstyle_cleaning(file_path)
