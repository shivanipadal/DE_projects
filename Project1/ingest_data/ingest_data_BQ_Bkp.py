from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(file: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{file}"
    print("-------------------------------------------")
    print(f"gcs_path is {gcs_path}")
    gcs_block = GcsBucket.load("de-project-gcs")
    
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def write_bq(path: Path,file: str) -> int:
    """Write DataFrame to BiqQuery"""

    df = pd.read_parquet(path)
    print("PAth is--------------------")
    print(path)
    gcp_credentials_block = GcpCredentials.load("gcp-creds-project")

    if file == 'IPL_Ball_by_Ball_2008_2020.csv.gz.parquet' :
        df.to_gbq(
            destination_table="raw_ipl_match_data.ball_by_ball",
            project_id="de-project-2023",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
        )
    elif file == 'IPL_Matches_2008_2020.csv.gz.parquet' :
        df.to_gbq(
            destination_table="raw_ipl_match_data.match_data",
            project_id="de-project-2023",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
        )       
 
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq(file):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(file)
    #df = transform(path)
    records=write_bq(path,file)

    return records

@flow(log_prints=True)
def ingest_data_bq(
    files: list[str] = ['IPL_Ball_by_Ball_2008_2020.csv.gz.parquet','IPL_Matches_2008_2020.csv.gz.parquet']
    ):
    rec_count = 0
    for file in files:
        print(f'-----------FILE is {file}----------------')
        records=etl_gcs_to_bq(file)
        rec_count = rec_count + records

    print("Total no of records processed", rec_count)

if __name__ == "__main__":
    files=['IPL_Ball_by_Ball_2008_2020.csv.gz.parquet','IPL_Matches_2008_2020.csv.gz.parquet']
    ingest_data_bq(files)
