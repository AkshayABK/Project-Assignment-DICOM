# Project Summary

This project involves parsing DICOM files from S3 to local storage, applying basic transformations, and organizing them for analysis.

# Project Overview

This project contains three main modules:

1. **script**
2. **summary**
3. **utils**
4. **logs**

## 1. **script**

This module contains two Python files:

- **`main.py`**: Handles reading from an S3 bucket and creates local logical folders based on patient IDs at the specified path.
- **`summary_script.py`**: Contains functions that generate summaries from DICOM files.

## 2. **summary**

This folder includes a summary csv file.

## 3. **utils**

This folder includes a configuration file that stores:

- Root folder, transformed folder, S3 bucket name, and Datamart table columns.
- Columns to be parsed from DICOM files.

## 4. **logs**

This folder includes a logs of success and error:

## Steps to Run the Project

1. Open **`main.py`** in the **`script`** folder and insert your AWS access key and secret key in the `os.env` in `main.py` file.
   
2. Go to the path of main.py in terminal and run the following command to install dependecy and execute **`main.py`**:
   ```bash
   pip install -r requirements.txt
   python main.py

> **Note:**  
> Ensure you are using the correct version of Python for this project. The current recommended version is Python 3.9.13.

3. Output Folders and Files:
   After running the script, the following structure will be created:
   
   - `transformed/` folder will contain subfolders named after each `patientID`, and within each patient folder, CSV files will be created, with the filenames corresponding to the `studyInstanceUID` values.
     
4. Additionally, Datamart folders will be created, containing CSV files that categorize DICOM data for analysis.
   
5. Once the folders and files are generated, you may see some basic summary csv file in summary folder. 
