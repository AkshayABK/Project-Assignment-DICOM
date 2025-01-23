import os
import sys
import io
import pandas as pd

try:
    current_directory = os.getcwd()
    
    utils_directory = os.path.abspath(os.path.join(current_directory, '../utils'))
    sys.path.append(utils_directory)
    import config
    
except Exception as e:
    print(f"Error importing source folder path or utils path {e}")
    

def process_csv_folders(root_folder):
    '''
    Summary :  Iterate through local folders if CSV files appends the data into single file and returns dataframe
    args : root folder
    '''

    metadata_list = []
    for subdir, _, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(subdir, file)
                try:
                    df = pd.read_csv(file_path)
                    metadata_list.append(df)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    if metadata_list:
        consolidated_df = pd.concat(metadata_list, ignore_index=True)
        return consolidated_df
    else:
        print("No CSV files found.")
        return pd.DataFrame()

def generate_summary_from_dataframe(df):
    '''
    
    Summary : This fucntion reads dataframe and we can generate summary returns dataframe
    args : DataFrame
    '''

    try:
        required_columns = [
            "StudyInstanceUID", "InstanceNumber", "SliceThickness", "PatientID"
        ]
        for col in required_columns:
            if col not in df.columns:
                print(f"Missing required column: {col}")
                return None

        total_studies = df["StudyInstanceUID"].nunique()

        total_slices = len(df)

        slices_per_study = df.groupby("StudyInstanceUID")["InstanceNumber"].count()
        avg_slices_per_study = slices_per_study.mean()

        slice_thickness_distribution = df["SliceThickness"].dropna().value_counts()

        summary_data = {
            "Metric": [
                "Total Studies",
                "Total Slices",
                "Average Slices per Study",
                "Slice Thickness Distribution"
            ],
            "Value": [
                total_studies,
                total_slices,
                avg_slices_per_study,
                slice_thickness_distribution.to_dict()
            ]
        }

        summary_df = pd.DataFrame(summary_data)
        return summary_df

    except Exception as e:
        print(f"Error generating summary: {e}")
        return pd.DataFrame()