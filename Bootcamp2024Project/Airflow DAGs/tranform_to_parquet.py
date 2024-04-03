import os
import pandas as pd

def process_parquet_files(input_folder, output_folder):
    """
    Process Parquet files from input_folder and save them as a concatenated Parquet file in output_folder.

    Args:
        input_folder (str): Path to the directory containing Parquet files.
        output_folder (str): Path to the directory where the concatenated Parquet file will be saved.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    _columns = ['Day', 'Hour', 'Origin Station', 'Destination Station', 'Trip Count']
    
    for year_dir in os.listdir(input_folder):
        year_path = os.path.join(input_folder, year_dir)
        print(f"Processing year: {year_dir}")
        
        if not os.path.isdir(year_path):
            continue
        
        # Initialize an empty list to store DataFrames
        dfs = []
        for file_name in os.listdir(year_path):
            if file_name.endswith('.parquet'):
                file_path = os.path.join(year_path, file_name)
                df = pd.read_parquet(file_path, columns=_columns)
                num_rows_before = df.shape[0]
                df = df.dropna()
                num_rows_deleted = num_rows_before - df.shape[0]
                df['Month_Col'] = df['Day'].dt.month
                month = df['Month_Col'].mode()[0]
                month = str(month).replace(' ', '').strip()
                df = df.rename(columns={'Day': 'Full_Date', 'Origin Station': 'Origin_Station', 'Destination Station': 'Destination_Station', 'Trip Count': 'Trip_Count'})
                df['Year_Col'] = df['Full_Date'].dt.year
                df['DateOfTheMonth'] = df['Full_Date'].dt.day
                print(month)
                print(df.head(2))
                print(df.columns)
                print(df.shape)
                output_file_name = os.path.join(output_folder, year_dir)
                os.makedirs(output_file_name, exist_ok=True)
                output_file = os.path.join(output_file_name, f"month_{month}.csv")
                df.to_csv(output_file, index=False)
            print(f"Processed and saved {output_file_name}")
            
        # for file_name in os.listdir(year_path):
        #     if file_name.endswith('.parquet'):
        #         file_path = os.path.join(year_path, file_name)
        #         df = pd.read_parquet(file_path, columns=_columns)
        #         month = df['Day'].dt.month.mode()[0]
        #         month = str(month).replace(' ', '').strip()
        #         print(month)
        #         df['DateOfTheMonth'] = df['Day'].dt.day
        #         df['Year'] = df['Day'].dt.year
        #         df['Month'] = df['Day'].dt.month
        #         print(df.head(2))
        #         print(df.columns)
        #         print(df.shape)
        #         output_file_name = os.path.join(output_folder, year_dir)
        #         os.makedirs(output_file_name, exist_ok=True)
        #         output_file = os.path.join(output_file_name, f"month_{month}.parquet")
        #         df.to_parquet(output_file, engine='pyarrow')
        # print(f"Processed and saved {output_file_name}")

# Example usage
if __name__ == "__main__":
    extract_folder = '/home/prasadpilankar/extracted/RidersMonthly'  
    transform_folder = '/home/prasadpilankar/transform/RidersMonthly'  

    process_parquet_files(extract_folder, transform_folder)
