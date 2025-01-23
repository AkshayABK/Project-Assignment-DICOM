# Summary

This package has 3 modules as follows:

1. **script**
2. **summary**
3. **utils**

### 1. **script**
In this folder, there are two `.py` files:
- `main.py`: Contains logic to read an S3 bucket and create local logical folders for each new patient ID at the specified path.
- `summary_script.py`: Contains functions that are useful for generating summaries from DICOM files.

### 2. **summary**
This folder contains a Jupyter notebook that allows you to analyze the summary by utilizing the modified functions from `summary_script.py`.

### 3. **utils**
The `utils` folder contains a configuration file with the following details:
- AWS Access Key and Secret Key.
- Columns to be parsed from the DICOM files.

### Steps to Run the Function

1. Run `main.py` located in the `script` folder.
2. After running the script, you should see a transformed folder created, which contains subfolders named by patient ID. Each subfolder will have a CSV file containing the `studyInstanceUID`.
3. Once the folders and files are created, you can proceed to analyze the data using the Jupyter notebook in the `summary` folder for high-level analysis.
