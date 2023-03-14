from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import gzip
import os


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    # df = pd.read_csv(dataset_url)

    # with gzip.open(dataset_url, 'rb') as fio:
    df = pd.read_csv(dataset_url)
        
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    if not os.path.exists('data'): 
        os.makedirs('data')

    path = Path(f"data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_block = GcsBucket.load("de-project-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def ingest_data() -> None:
    """The main ETL function"""
 
    for i in range(0,2):  #To load two files
        if i == 0 :
            # dataset_url = "https://github.com/shivanipadal/DE_projects/blob/main/input_data/IPL_Matches_2008_2020.csv.gz?raw=true" 
            dataset_url = "https://raw.githubusercontent.com/shivanipadal/DE_projects/main/input_data/IPL_Matches_2008_2020.csv.gz"
            dataset_file = "IPL_Matches_2008_2020.csv.gz"
        else :
            dataset_url = "https://raw.githubusercontent.com/shivanipadal/DE_projects/main/input_data/IPL_Ball_by-Ball_2008_2020.csv.gz"
            dataset_file = "IPL_Ball_by_Ball_2008_2020.csv.gz"
            

        df = fetch(dataset_url)
        path = write_local(df, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    ingest_data()

