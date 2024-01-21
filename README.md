# End-End Data Pipeline using Airflow, Docker, Amazon S3, PostgreSQL

![image](https://github.com/nikitgoku/spotify_etl_airflow_postgress/assets/114753615/acf3fefb-cc7c-4b35-a88e-80d60ea33c03)

This project consists of a data pipeline which extracts Spotify data using the Spotify Web API to store it as .csv file which is then uploaded to Amazon S3. The most recent file is then extracted from S3 and ingested into and RDS, and for that PostgreSQL is ran locally. Airflow with Docker is used to orchestrate the data pipeling where DAGs are used. This project is to be extended to include PowerBI to have a visual dashboard of the data.

## **Extraction**
[Spotify Web API](https://developer.spotify.com/documentation/web-api) for the extraction of data, which enables the creation of applications that can interact with Spotify's streaming service, such as retrieving content metadata, getting recommendations, creating and managing playlists, or controlling playback, for our case getting recently played songs.

Extraction of the data required `Access Token` which is a total different process but [How to Authenticate and use Spotify Web API](https://www.youtube.com/watch?v=1vR3m0HupGI) this can be helpful to extract the access token. Make sure that the `Access Token` has a time period and it has to be refreshed 3600s.

## **Transform**
A `.json` file is extracted which is then transformed into a `Pandas` DataFrame and then saved as a `.csv` file into the local machine. This file is then loaded into `S3` where all the file based on their timestamp are saved. This whole process requires S3 access and secret key, the process for which this [post](https://support.promax.com/knowledge/amazon-s3) can be helpful.

## **Load**
After this most recent file from S3 is downloaded which is then loaded into PostgreSQL database within a table.

## **Airflow and Docker**
This whole ETL pipeline is orchestrated with Airflow launched using Docker. Each of the ETL task and defined as operators (object) in a DAG encapsulating the ETL jobs as seen below:
![Screenshot 2024-01-21 143212](https://github.com/nikitgoku/spotify_etl_airflow_postgress/assets/114753615/3f3619e4-327f-4f98-a24f-ad89a7dc128e)

## **Future Works**
This project will be extended to include a visual dashboad, probably PowerBI just to visually see the data what the data entails visually.
