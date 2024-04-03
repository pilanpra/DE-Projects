import six
from google.cloud import bigquery

def main():
    # gcloud auth application-default login
    client = bigquery.Client(project='dm-project-407810')
    schema = [
        bigquery.SchemaField("Full_Date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("Hour_Col", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Origin_Station", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Destination_Station", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Trip_Count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("Year_Col", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Month_Col", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("DateOfTheMonth", "INTEGER", mode="REQUIRED")
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        max_bad_records=1000,
    )
        
    for year in range(2020, 2024):
        year_str = str(year)
        table_id = f'dm-project-407810.BART_DailyRiders.BART_{year}DailyRiders'
        for month in range(1, 13):
            month_str = str(month)
            uri = f"gs://zoomcamp-api-to-gcs/BART_DailyRiders/{year_str}/month_{month_str}.csv"      
            print(uri)
            table_id = f'dm-project-407810.BART_DailyRiders.BART_{year}DailyRiders'
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)  
            load_job.result()   
            destination_table = client.get_table(table_id)  
            print("Successfully loaded {} rows!".format(destination_table.num_rows))

if __name__ == '__main__':
    main()
