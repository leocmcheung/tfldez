<<<<<<< HEAD
![](images/tflbikes.jpg)

# TfL Bikes Data Engineering Project
This is a Data Engineering Project which uses the publicly available [cycling data from Transport for London](https://cycling.data.tfl.gov.uk/).
TfL Bikes (formally known as Santander Cycles) is the bicycle sharing system in central London since 2010. The operation of the scheme is contracted by Transport for London to Serco. In Sep 2022 TfL introduced electric bikes which makes cycling in London more comfortable and convenient.


## Problem Statement
TfL have made available a [unified API](https://tfl.gov.uk/info-for/open-data-users/unified-api) for open data sharing. [Detailed information]((https://cycling.data.tfl.gov.uk/)) for each journey can also be retrieved for data analysis.

While TfL kindly provides a full list of data for journeys on the cycling scheme since 2012, a lack of a live and interactive dashboard means the general public are finding it difficult to understand the vast amount of data.
The purpose of this project is to make an flowing data pipeline which extracts the data from TfL portal, loads into a cloud storage platform, apply data transformation and visualise the findings.

## Technology Stack
The following services are used in this project:
- Terraform - as Infrastructure-as-Code (IaC) tool
- Prefect - as Data Orchestration tool
- Google Cloud Storage (GCS) - as Data Lake
- Google BigQuery (BQ) - as Data Warehouse for queries
- dbt - as Data Transformation and Modelling tool
- Google Looker Studio - as Data Visualisation tool

The Data Pipeline Archiecture is as followed: PIC

## Data Description
There are two sets of raw data format available on TfL portal. Since the introduction of electric bikes on 12 Sep 2022 the dataset format was also renewed. Please find the following table for reference.
| Column(after data transformation) | Raw data column (before 12 Sep 2022)| Raw data column (after 12 Sep 2022)| Description |
|--------|--------|--------|-------------|
| rental_id | Rental Id | Number | Unique identifier for each journey |
| start_date | Start Date | Start date | The date and time for start of journey |
| startstation_id | StartStation Id | Start station number | Unique ID for start location (a bike station) |
| startstation_name | StartSation Name | Start station | Name of the start location |
| end_date | End Date | End date | The date and time for end of journey |
| endstation_id | EndStation Id | End station number | Unique ID for end location (a bike station) |
| endstation_name | EndSation Name | End station | Name of the end location |
| bike_id | Bike Id | Bike number | Unique ID for the bike hired |
| bike_model | <N/A> | Bike model | The type of bike hired (Classic Manual Bike or PBSC Electric Bike)
| duration | Duration | Total duration (ms) | The total duration of the journey (in minutes after data transformation) |
| <N/A> | <N/A> | Total duration | The total duration of the jounrey in written form, removed after transformation |

## Dashboard
The interactive dashboard can be found [here](https://lookerstudio.google.com/u/0/reporting/aa0e9e98-d067-4763-b156-26f495f00bd7)

## Reproducibility
To reproduce the project in your working space, please follow the instructions
1. Fork this repo and clone it to your local machine
For HTTPS:
`git clone https://github.com/leocmcheung/tfldez.git`
FOR SSH:
`git clone git@github.com:leocmcheung/tfldez.git`

2. Setup your Google Cloud
- Create a Google account and signup for [Google Cloud Platform](https://console.cloud.google.com/)
- Create a New Project and take note of the project-id
- Create a Service Account and configure its Identity and Access Management (IAM) policy
  - Viewer
  - Storage Admin
  - Storage Object Admin
  - BigQuery Admin
- Create a new key for the service account, and download the key as JSON credentials. Store the key in a secure location.
- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
- Replace the GCP key location in the following codes and execute the codes in terminal:
```export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login
```
- Close and restart your terminal

3. (Optional) Create a new virtual environment. The following codes are for creating a new virtualenv using pyenv, although other environment applications are available (e.g. Anaconda). In the project directory run:
```
pyenv install 3.10.6
pyenv virtualenv 3.10.6 tfldez
pyenv local tfldez
```

4. Setup Terraform
- Follow the [instructions](https://developer.hashicorp.com/terraform/downloads) on the website for your particular operating system and install Terraform.
- Once installed go to `terraform/` folder and update your GCP project's region and zone (default as europe-west6 Zurich)
- Run the following codes in terminal to initiate, plan and apply the infrastructure.
```bash
cd terraform/
terraform init
terraform plan -var="project=<your-gcp-project-id>"
terraform apply -var="project=<your-gcp-project-id>"
```