import sys
import os
import pydicom
import pandas as pd
import boto3
from io import BytesIO
import logging
import time
import threading
from pathlib import Path

try:
    current_directory = os.getcwd()
    utils_directory = os.path.abspath(os.path.join(current_directory, '../utils'))
    sys.path.append(utils_directory)
    import config
except Exception as e:
    print(f"Error importing source folder path or utils path {e}")


def setup_logging():
    """
    Sets up logging for the script with separate files for success and error logs.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Success log file
    success_handler = logging.FileHandler("../logs/success.log")
    success_handler.setLevel(logging.INFO)
    success_handler.setFormatter(formatter)

    # Error log file
    error_handler = logging.FileHandler("../logs/error.log")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    # Adding handlers to the logger
    logger.addHandler(success_handler)
    logger.addHandler(error_handler)

    return logger

def generate_summary_from_folders(base_dir):
    total_studies = 0
    total_slices = 0
    slices_per_study = []
    slice_thickness_dist = {}

    for patient_folder in Path(base_dir).iterdir():
        if patient_folder.is_dir():  
            patient_id = patient_folder.name  

            for study_file in patient_folder.glob("*.csv"):
                study_instance_uid = study_file.stem
                
                table = pd.read_csv(study_file)

                if "SliceThickness" in table.columns:
                    total_slices += table["SliceThickness"].sum()
                    slices_per_study.append(table["SliceThickness"].count())
                    slice_thickness_dist = table["SliceThickness"].describe().to_dict()

                total_studies += 1
    
    data = {
    "Total Studies": [total_studies],
    "Total Slices Across All Scans": [total_slices],
    "Average Number of Slices Per Study": [sum(slices_per_study) / len(slices_per_study) if slices_per_study else 0],
    "Slice Thickness Distribution": [slice_thickness_dist]
    }

    df = pd.DataFrame(data)
    
    df.to_csv('Summary.csv', index=False)

    return df


def extract_dicom_metadata(dicom_data):
    """
    Extracts specified DICOM metadata from a DICOM file and stores it in a dictionary.
    """
    try:
        data = {}
        for attr in config.dicom_attributes.get('attributes', []):
            data[attr] = getattr(dicom_data, attr, None)
        return data
    except Exception as e:
        logger.error(f"Error extracting DICOM metadata: {e}")
        return None


def parse_dicom_file(obj):
    """
    Reads the DICOM file from the S3 object and converts it into a pandas DataFrame.
    """
    try:
        file_content = obj.get()['Body'].read()
        dicom_data = pydicom.dcmread(BytesIO(file_content))
        logger.info(f"Successfully read DICOM file: {obj.key}")

        metadata = extract_dicom_metadata(dicom_data)
        if metadata:
            return pd.DataFrame([metadata])  # Return DataFrame

    except Exception as e:
        logger.error(f"Failed to read file {obj.key} as DICOM: {e}")
        return pd.DataFrame()


def save_df_to_local(df, local_dir, obj_key):
    """
    Saves the DICOM metadata DataFrame to a local CSV file. If the file already exists,
    appends the new data after removing duplicates.
    """
    if df.shape[0] == 0:
        logger.error(f"Empty metadata list for {obj_key}")
        return

    try:
        PatientID = str(df['PatientID'][0])
        StudyInstanceUID = str(df['StudyInstanceUID'][0])

        patient_folder = os.path.join(local_dir, PatientID)
        os.makedirs(patient_folder, exist_ok=True)

        local_file_path = os.path.join(patient_folder, f"{StudyInstanceUID}.csv")

        if os.path.exists(local_file_path):
            existing_df = pd.read_csv(local_file_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.applymap(str)
            combined_df.drop_duplicates(keep='last', inplace=True)
            combined_df.to_csv(local_file_path, index=False)
            logger.info(f"Appended data and removed duplicates in {local_file_path}")
        else:
            # If the file does not exist, just save the new data
            df.to_csv(local_file_path, index=False)
            logger.info(f"Created and saved new file to {local_file_path}")
        
    except Exception as e:
        logger.error(f"Failed to save DataFrame locally for {obj_key}: {e}")


def threaded_save_df_to_local(df, local_dir, obj_key):
    """Wrapper function to run save_df_to_local in a new thread."""
    thread = threading.Thread(target=save_df_to_local, args=(df, local_dir, obj_key))
    thread.start()
    return thread


def save_to_datamart_layer(df, datamart_attributes):
    """
    Saves DataFrame to respective folders for analysis and transformation.
    """
    for key, value in datamart_attributes.items():
        output_folder = f'../Datamarts/{key}/'
        os.makedirs(output_folder, exist_ok=True)
        output_csv_path = os.path.join(output_folder, f'{key}.csv')

        datamart_to_save = df[value]

        try:
            if os.path.exists(output_csv_path):
                existing_df = pd.read_csv(output_csv_path)
                combined_df = pd.concat([existing_df, datamart_to_save], ignore_index=True).drop_duplicates()
                combined_df.to_csv(output_csv_path, index=False)
                logger.info(f"Appended data to {output_csv_path}")
            else:
                datamart_to_save.to_csv(output_csv_path, index=False)
                logger.info(f"Created and saved data to {output_csv_path}")
        except Exception as e:
            logger.error(f"Failed to save to datamart layer {key}: {e}")


def threaded_save_to_datamart_layer(df, datamart_attributes):
    """Wrapper function to run save_to_datamart_layer in a new thread."""
    thread = threading.Thread(target=save_to_datamart_layer, args=(df, datamart_attributes))
    thread.start()
    return thread


def main():
    os.environ['ACCESS_KEY'] = 'your-access-key'
    os.environ['SECRET_KEY'] = 'your-secret-key'

    aws_access_key_id = os.getenv('ACCESS_KEY')
    aws_secret_access_key = os.getenv('SECRET_KEY')

    s3 = boto3.resource(
        service_name='s3',
        region_name='eu-north-1',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    df = pd.DataFrame()
    raw_bucket_name = config.aws_bucket_name.get('raw', '')
    root_folder_name = config.root_folder
    transformed_folder_name = config.transformed_file_location
    raw_bucket = s3.Bucket(raw_bucket_name)

    start_time = time.time()

    threads = []

    for obj in raw_bucket.objects.filter(Prefix=root_folder_name):
        if not obj.key.endswith('/'):
            print(f'Processing file: {obj.key}')
            logger.info(f'Processing file: {obj.key}')
            metadata = parse_dicom_file(obj)
            if metadata is not None:
                thread = threaded_save_df_to_local(metadata, transformed_folder_name, obj.key)
                threads.append(thread)

    for thread in threads:
        thread.join()

    logger.info(f"Data successfully processed and saved to the transformed folder: {transformed_folder_name}")
    
    datamart_threads = []
    
    for root, dirs, files in os.walk(transformed_folder_name):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")
                transformed_df = pd.read_csv(csv_file_path)
                datamart_thread = threaded_save_to_datamart_layer(transformed_df, config.datamart_attributes)
                datamart_threads.append(datamart_thread)
                
    for datamart_thread in datamart_threads:
        datamart_thread.join()
     
    #Generating summary
    generate_summary_from_folders('../summary')

    end_time = time.time()
    logger.info(f'Total execution time: {end_time - start_time} seconds')


if __name__ == '__main__':
    logger = setup_logging()
    logger.info("Starting the script...")
    try:
        main()
        logger.info("Script completed successfully.")
    except Exception as e:
        logger.error(f"Script failed: {e}")
