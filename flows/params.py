from datetime import date,datetime


params = {
    'url': 'https://cycling.data.tfl.gov.uk/usage-stats/',
    'start_date': '2022-01-01',
    'end_date': '2023-12-31',
    'start_week_number': 299
}



def get_url():
    return params['url']

def get_start_date():
    return datetime.strptime(params['start_date'], '%Y-%m-%d')

def get_end_date():
    return datetime.strptime(params['end_date'], '%Y-%m-%d')

def get_start_wk_number():
    return params['start_week_number']
