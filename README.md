# Project Overview

This project contains three main modules:

1. **script**
2. **summary**
3. **utils**

## 1. **script**

This module contains two Python files:

- **`main.py`**: Handles reading from an S3 bucket and creates local logical folders based on patient IDs at the specified path.
- **`summary_script.py`**: Contains functions that generate summaries from DICOM files.

## 2. **summary**

This folder includes a Jupyter notebook for analyzing DICOM file summaries using data from Datamarts.

## 3. **utils**

This folder includes a configuration file that stores:

- Root folder, transformed folder, S3 bucket name, and Datamart table columns.
- Columns to be parsed from DICOM files.

## Steps to Run the Project

1. Open **`main.py`** in the **`script`** folder and insert your AWS access key and secret key in the `os.env`.
   
2. Run the following command to execute **`main.py`**:
   ```bash
   python main.py

    [!NOTE] Python version I used 3.9.8

3. After running the script, a "transformed" folder will be created with subfolders named after patient IDs. Each subfolder will contain a CSV file named after the studyInstanceUID.
4. Additionally, Datamart folders will be created, containing CSV files that categorize DICOM data for analysis.
5. Once the folders and files are generated, open the Jupyter notebook in the summary folder to analyze the data in detail. 
