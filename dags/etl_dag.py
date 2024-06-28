import csv
from  datetime import datetime
from  airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.models import Variable


with DAG(
    "etl_dag",
    description="ETL DAG",
    schedule_interval='*/3 * * * *',
    start_date=datetime(2024, 6, 27),
    catchup=False,
    default_view="graph",
    tags=["brach", "tag", "pipeline"],
) as dag:
    dataset = pd.read_csv("/opt/airflow/data/spotify_songs_2024.csv", sep=",")
    dataset.columns = [
       "Track Name","Album Name","Artist","Release Date","ISRC","All Time Rank","Track Score","Spotify Streams","Spotify Playlist Count","Spotify Playlist Reach","Spotify Popularity","YouTube Views","YouTube Likes","TikTok Posts","TikTok Likes","TikTok Views","YouTube Playlist Reach","Apple Music Playlist Count","AirPlay Spins","SiriusXM Spins","Deezer Playlist Count","Deezer Playlist Reach","Amazon Playlist Count","Pandora Streams","Pandora Track Stations","Soundcloud Streams","Shazam Counts","TIDAL Popularity","Explicit Track"
    ]
    def process_file(**kwarg):
        with open(Variable.get("file_path_json", "r"))  as file:
            data = csv.load(file)
            kwarg["ti"].xcom_push(key="Track Name", value=data["Track Name"])
            kwarg["ti"].xcom_push(key="Album Name", value=data["Album Name"])
            kwarg["ti"].xcom_push(key="Artist", value=data["Artist"])

    def extract(dataset):
        dataset.to_csv("/opt/airflow/data/extracted_spotify.csv")

    def transform():
        dataset = pd.read_csv("/opt/airflow/data/extracted_spotify.csv")
        dataset["Spotify Streams"] = dataset["Spotify Streams"].apply(lambda x: x.replace(",", ""))
        dataset["Spotify Streams"] = dataset["Spotify Streams"].astype(int)
        dataset.to_csv("/opt/airflow/data/transformed_spotify.csv") 
    def load():
        dataset = pd.read_csv("/opt/airflow/data/transformed_spotify.csv")
        dataset.to_csv("/opt/airflow/data/transformed_spotify.csv") 

    get_data = PythonOperator(task_id="get_data", python_callable=process_file, provide_context=True)
    extract = PythonOperator(task_id='extract', python_callable=extract, op_args=[dataset])
    transform = PythonOperator(task_id='transform', python_callable=transform)
    load = PythonOperator(task_id='load', python_callable=load)

    extract >> transform >> load


       