import sys
import os
import pydicom
import pandas as pd
import boto3
from io import BytesIO, StringIO
import logging
import io
import time
import threading
import csv

try:
    current_directory = os.getcwd()
    utils_directory = os.path.abspath(os.path.join(current_directory, '../utils'))
    sys.path.append(utils_directory)
    import config
except Exception as e:
    print(f"Error importing source folder path or utils path {e}")


def logger(func):
    def wrapper(*args, **kwargs):
        print(f"Logger: {func.__name__} was called")
        return func(*args, **kwargs)
    return wrapper


@logger
def extract_dicom_metadata(dicom_data):
    """
    Extracts specified DICOM metadata from a DICOM file and stores it in a dictionary.
    
    args: 
        dicom_data (pydicom.dataset.FileDataset): DICOM data.
        
    returns:
        dict: A dictionary containing the extracted DICOM metadata.
    """
    try:
        data = {}
        for attr in config.dicom_attributes.get('attributes', []):
            data[attr] = getattr(dicom_data, attr, None)
        return data
    except Exception as e:
        print(f"Error extracting DICOM metadata: {e}")
        return None


@logger
def parse_dicom_file(obj):
    """
    Reads the DICOM file from the S3 object and converts it into a pandas DataFrame.
    
    args: 
        obj (boto3.s3.Object): The S3 object containing the DICOM file.
        
    returns:
        pd.DataFrame: DataFrame containing the DICOM metadata.
    """
    try:
        file_content = obj.get()['Body'].read()
        dicom_data = pydicom.dcmread(BytesIO(file_content))
        print(f"Successfully read DICOM file: {obj.key}")

        metadata = extract_dicom_metadata(dicom_data)
        if metadata:
            metadata["FilePath"] = obj.key
            return pd.DataFrame([metadata])  # Directly returning a DataFrame
        return None

    except Exception as e:
        print(f"Failed to read file {obj.key} as DICOM: {e}")
        return None


def save_df_to_local(df, local_dir, obj_key):
    """
    Saves the DICOM metadata DataFrame to a local CSV file. If the file already exists,
    appends the new data.
    
    args: 
        df (pd.DataFrame): The DataFrame containing the DICOM metadata.
        local_dir (str): The local directory where the data should be saved.
        obj_key (str): The key (name) of the S3 object.
        
    returns:
        pd.DataFrame: The DataFrame that was saved (or appended).
    """
    
    if df.shape[0] == 0:
        print(f"Empty metadata list for {obj_key}")
        return f"Empty metadata list for {obj_key}"
    
    try:
        PatientID = str(df['PatientID'][0])
        StudyInstanceUID = str(df['StudyInstanceUID'][0])

        patient_folder = os.path.join(local_dir, PatientID)
        os.makedirs(patient_folder, exist_ok=True)
        
        local_file_path = os.path.join(patient_folder, f"{StudyInstanceUID}.csv")

        if os.path.exists(local_file_path):
            existing_df = pd.read_csv(local_file_path)
            updated_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            updated_df = df

        updated_df.to_csv(local_file_path, index=False)
        print(f"Saved DataFrame to {local_file_path}")

        return updated_df 
    
    except Exception as e:
        print(f"Failed to save DataFrame locally for {obj_key}: {e}")
        return pd.DataFrame()


def threaded_save_df_to_local(df, local_dir, obj_key):
    """Wrapper function to run save_df_to_local in a new thread."""
    thread = threading.Thread(target=save_df_to_local, args=(df, local_dir, obj_key))
    thread.start()
    return thread

def save_to_datamart_layer(df, datamart_attributes):
    """
    Saves DataFrame to respective folders for analysis and transformation.
    
    args:
        df (pd.DataFrame): The DataFrame containing the data.
        datamart_attributes (dict): Dictionary containing the datamart categories as keys and the corresponding list of columns as values.
    """
    for key, value in datamart_attributes.items():
        output_folder = f'../Datamarts/{key}/'
        os.makedirs(output_folder, exist_ok=True)  # Ensure the folder exists
        output_csv_path = os.path.join(output_folder, f'{key}.csv')

        datamart_to_save = df[value]

        if os.path.exists(output_csv_path):
            existing_df = pd.read_csv(output_csv_path)
            # This logic is for storing data only if it is new info
            if key in config.datamart_primary_keys:
                new_rows = datamart_to_save[~datamart_to_save[config.datamart_primary_keys[key]].isin([config.datamart_primary_keys[key]])]
            else:
                new_rows = datamart_to_save
                
            if not new_rows.empty:
                new_rows.to_csv(output_csv_path, mode='a', header=False, index=False)
                print(f"Appended new data to {output_csv_path}")
            else:
                print(f"No new {config.datamart_primary_keys[key]} data to append for {key}")
                
            # Dropping duplicates   
            existing_df = pd.read_csv(output_csv_path)
            existing_df = existing_df.drop_duplicates()
            
        else:
            datamart_to_save.to_csv(output_csv_path, index=False)
            print(f"Data has been saved to {output_csv_path}")

def threaded_save_to_datamart_layer(transformed_df, datamart_attributes):
    """Wrapper function to run threaded_save_to_datamart_layer in a new thread."""
    thread = threading.Thread(target=save_to_datamart_layer, args=(transformed_df, datamart_attributes))
    thread.start()
    return thread
            

def main():

    os.environ['ACCESS_KEY'] = 'your-access-key'
    os.environ['SECRET_KEY'] = 'your-secret-key'

    aws_access_key_id = os.getenv('ACCESS_KEY')
    aws_secret_access_key = os.getenv('SECRET_KEY')

    print(aws_access_key_id, aws_secret_access_key)
    s3 = boto3.resource(
        service_name='s3',
        region_name='eu-north-1',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    df = pd.DataFrame()
    raw_bucket_name = config.aws_bucket_name.get('raw', '')
    raw_transformed_name = config.aws_bucket_name.get('transformed', '')
    root_folder_name = config.root_folder
    transformed_folder_name = config.transformed_file_location
    raw_bucket = s3.Bucket(raw_bucket_name)
    start_time = time.time()
    
    threads = []
    
    for obj in raw_bucket.objects.filter(Prefix=root_folder_name):
        if not obj.key.endswith('/'):
            print(f'Processing {obj.key} file')
            metadata = parse_dicom_file(obj)
            if metadata is not None:
                # Run save_df_to_local in a separate thread
                thread = threaded_save_df_to_local(metadata, transformed_folder_name, obj.key)
                threads.append(thread)
                print('**********************************************')

    for thread in threads:
        thread.join()
        
    datamart_threads = []
    
    for root, dirs, files in os.walk(transformed_folder_name):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")
                transformed_df = pd.read_csv(csv_file_path)
                datamart_thread = threaded_save_to_datamart_layer(transformed_df, config.datamart_attributes)
                datamart_threads.append(thread)
                
    for datamart_thread in datamart_threads:
        datamart_thread.join()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f'Total execution time {execution_time}')


if __name__ == '__main__':    
    main()
