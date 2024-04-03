from google.cloud import bigquery

def remove_all_rows():
    client = bigquery.Client(project='dm-project-407810')

    # Define the list of tables to remove rows from
    tables = [
        'BART_DailyRiders.BART_2020DailyRiders',
        'BART_DailyRiders.BART_2021DailyRiders',
        'BART_DailyRiders.BART_2022DailyRiders',
        'BART_DailyRiders.BART_2023DailyRiders'
    ]

    for table_id in tables:
        try:
            query = f'DELETE FROM `{table_id}` WHERE true'  
            query_job = client.query(query)
            rows_deleted = query_job.result().total_rows
            print(f"{rows_deleted} rows deleted from table {table_id}.")
        except Exception as e:
            print(f"Error deleting rows from table {table_id}: {e}")

if __name__ == '__main__':
    remove_all_rows()
