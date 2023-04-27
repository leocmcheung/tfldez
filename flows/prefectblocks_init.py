from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


'''
A script to initialise GCP Credentials block and GCS Bucket block on Prefect Cloud
'''

your_GCS_bucket_name = "tfl_data_lake_tfldez"  # Please insert your own bucket name on GCS
gcs_credentials_block_name = "tfldez-gcp-creds"

credentials_block = GcpCredentials(
    service_account_info= {
        } # Please insert the contents of your Service Account json
   # REMEMBER please don't disclose this in any public repostiories!!!!
)

credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)
print('GCP Credentials block done')

bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(gcs_credentials_block_name),
    bucket=your_GCS_bucket_name,
)

bucket_block.save(f"tfldez-bucket", overwrite=True)
print('GCS Bucket block done')
