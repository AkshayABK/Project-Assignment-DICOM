import sys
import os
import pydicom
import pandas as pd
import boto3
from io import BytesIO, StringIO
import logging
import io
import time

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
    '''
    summary : This function takes s3 DICOM object body metadata and adds into dictionary file
    args : dicom_date --> s3 object information
    '''
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
    '''
    Summary :  This function reads the s3 DICOM files and convert this into Dataframe  
    args: obj --> s3 object information
    '''
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
    '''
    Summary : This function saves converted dataframe into logical orgnized folder if same patient ID and studyInstanceUID 
    appends the data into single file
    args: 
        df --> DICOM dataframe
        local_dir --> loca path to save csv
        obj_key --> file name
            
    '''
    
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

        return f"Successfully saved {local_file_path}"
    
    except Exception as e:
        print(f"Failed to save DataFrame locally for {obj_key}: {e}")
        return f"Error saving file: {e}"


def main():
    s3 = boto3.resource(
        service_name='s3',
        region_name='eu-north-1',
        aws_access_key_id=config.aws_credential.get('aws_access_key_id', ''),
        aws_secret_access_key=config.aws_credential.get('aws_secret_access_key', ''),
    )

    df = pd.DataFrame()
    raw_bucket_name = config.aws_bucket_name.get('raw', '')
    raw_transformed_name = config.aws_bucket_name.get('transformed', '')
    root_folder_name = config.root_folder
    transformed_folder_name = config.transformed_file_location
    raw_bucket = s3.Bucket(raw_bucket_name)
    transformed_bucket = s3.Bucket(raw_transformed_name)

    for obj in raw_bucket.objects.filter(Prefix=root_folder_name):
        if not obj.key.endswith('/'):
            start_time = time.time()
            print(f'Processing {obj.key} file')
            metadata = parse_dicom_file(obj)
            if metadata is not None:
                res = save_df_to_local(metadata, transformed_folder_name, obj.key)
                end_time = time.time()
                execution_time = end_time - start_time
                print(f'Total process time for {obj.key} is {execution_time}')
                print('**********************************************')

if __name__ == '__main__':
    main()
