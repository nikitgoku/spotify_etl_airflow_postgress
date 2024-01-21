# Import dependencies
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

from dotenv import load_dotenv, dotenv_values

from datetime import date
from datetime import datetime
from datetime import timedelta

import pandas as pd
import requests
import boto3
import psycopg2
import logging
import os

# Get the directory path for reference
dir_path = os.getcwd()

# Load the environment variables
load_dotenv()

# Create a function to download recently played songs
# using the Spotify Web API
def download_spotify_data():
    # USER_ID   = ""
    # Make sure that the token refreshes
    WEB_TOKEN = os.getenv("SPOTIFY_API_TOKEN")

    # **EXTRACT**
    # This section extracts the recently played songs using the spotify web API
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=WEB_TOKEN)
    }

    # Convert time to Unix timestamp in miliseconds      
    today             = datetime.now()
    yesterday         = today - timedelta(days=1)
    yesterday_unix_ts = int(yesterday.timestamp()) * 1000
    played_date       = today.strftime("%d-%m-%y")

    # Download all song's data in the last 24 hours window
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_ts), 
        headers = headers
    )

    data = r.json()

    # Create list of valid parameters to extract the aspects of the data
    song_names     = []
    artist_names   = []
    played_at_list = []
    timestamps     = []

    # Extract only the relevant bits of data from the json object      
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
            
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    songs_dict = {
        "song_name"  : song_names,
        "artist_name": artist_names,
        "played_at"  : played_at_list,
        "timestamp"  : timestamps
    }

    # Transform the json file into a dataframe
    songs_df = pd.DataFrame(songs_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    
    # Convert the dataframe into a .csv file and store it in the /data/ directory/
    songs_df.to_csv(dir_path+"/data/"+played_date+"-recently-played.csv", header=False)


# Create a function to load the downloaded data from Spotify to S3
def load_data_into_s3():
    # Get today's time to rename the files with timestamp
    today     = date.today()
    coll_date = today.strftime("%d-%m-%y")
    
    # Create a session by using boto3's session API by passing the access key and the secret access key.
    session = boto3.Session(
        aws_access_key_id     = os.getenv("AWS_S3_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_KEY"),
    )

    # Create the S3 resource
    s3 = session.resource('s3')
    # Upload the file from the /data/ directory and store it in the s3 bucket with a specified name
    s3.meta.client.upload_file(dir_path+"/data/"+coll_date+"-recently-played.csv", "spotify-web-api-data", coll_date+"-recently_played.csv")


# Create a function to download the data from S3
def download_data_from_s3():
    # Create a client to get the filename of the latest file uploaded on S3
    s3 = boto3.client(
        's3',
        aws_access_key_id     = os.getenv("AWS_S3_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_KEY"),
    )

    get_latest  = lambda obj: int(obj['LastModified'].strftime("%H%M"))
    objects     = s3.list_objects_v2(Bucket="spotify-web-api-data")["Contents"]
    latest_file = [obj["Key"] for obj in sorted(objects, key=get_latest)][-1]

    # Create a session to download the latest file uploaded on S3
    session = boto3.Session(
        aws_access_key_id     = os.getenv("AWS_S3_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_KEY"),
    )

    s3 = session.resource("s3")
    s3.Bucket("spotify-web-api-data").download_file(latest_file, dir_path+"/s3_data/"+latest_file)

# Create a function to load the data into PostgreSQL
def load_data_into_rds():
    # Establish the connection to the Postgres database
    connection = psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST"),
        database = os.getenv("POSTGRES_DATABASE"),
        user     = os.getenv("POSTGRES_USER"),
        password = os.getenv("POSTGRES_PASSWORD"),
        port     = os.getenv("POSTGRES_PORT")
    )  
    # Create a cursor object to interact with the database
    cursor = connection.cursor()
    logging.info('Postgres server connection is successful')

    # Execute a MySQL function to check if the connection is being made
    cursor.execute("SELECT VERSION()")

    # Check if the connection is established to the PostgreSQL
    data = cursor.fetchone()
    print("INFO: Connection establised to: ", data)

    # Create a client to get the filename of the latest file uploaded on S3
    s3 = boto3.client(
        's3',
        aws_access_key_id     = os.getenv("AWS_S3_ACCESS_KEY"),
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_KEY"),
    )

    get_latest  = lambda obj: int(obj['LastModified'].strftime("%H%M"))
    objects     = s3.list_objects_v2(Bucket="spotify-web-api-data")["Contents"]
    latest_file = [obj["Key"] for obj in sorted(objects, key=get_latest)][-1]

    # Create a command to createa table in the database with the features in the csv file
    command = (
        """
        CREATE TABLE IF NOT EXISTS recently_played_songs (
            id SERIAL PRIMARY KEY NOT NULL,
            song_name TEXT,
            artist_name TEXT,
            played_at TIMESTAMP,
            timestamp TIMESTAMP
        );
        """
    )

    # Print and execute the command
    print(command)
    cursor.execute(command)
    connection.commit()

    # Insert the data into the table created in the database's table created on the RDS
    print(latest_file)
    f = open(dir_path+"/s3_data/"+latest_file, 'r')
    print(f)

    # Using try and catch to generate any exception if the data is not present
    try:
        cursor.copy_from(f, "recently_played_songs", sep=",")
        print("INFO: Data inserted in the recently_played_songs sucessfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # Pass exception to a function
        print("ERROR: "+psycopg2.DatabaseError)
        print(Exception)
        # show_psycopg2_exception(err)
        cursor.close()
    
    connection.commit()
    cursor.close()
    connection.close()
    

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2024, 1, 19)    # Don't forget to change the date
}

spotify_dag = DAG(
    "recently_played_dag",
    default_args = default_args,
    description = "Collecting songs played on Spotify within the 24 hour window for data analysis",
    schedule_interval = "@daily",
    catchup = False
)

download_data = PythonOperator(
    task_id = "download_spotify_data",
    python_callable = download_spotify_data,
    dag = spotify_dag
)

load_data_s3 = PythonOperator(
    task_id = "load_data_into_s3",
    python_callable = load_data_into_s3,
    dag = spotify_dag
)

download_s3_data = PythonOperator(
    task_id = "download_data_from_s3",
    python_callable = download_data_from_s3,
    dag = spotify_dag
)

load_data_rds = PythonOperator(
    task_id = "load_data_into_rds",
    python_callable = load_data_into_rds,
    dag = spotify_dag
)

download_data >> load_data_s3 >> download_s3_data >> load_data_rds