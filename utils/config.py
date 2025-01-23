dicom_attributes = {'attributes':["PatientName","PatientID","PatientBirthDate","PatientSex","StudyInstanceUID","StudyID","StudyDate","StudyTime","AccessionNumber","SeriesInstanceUID","SeriesNumber","Modality","SeriesDate","SeriesTime","SOPInstanceUID","InstanceNumber","ImagePositionPatient","ImageOrientationPatient","PixelSpacing","SliceThickness","SliceLocation","Manufacturer","ManufacturerModelName","DeviceSerialNumber"]
                   }

aws_credential = {
    'aws_access_key_id':'your-accesss-key',
    'aws_secret_access_key':'your-secrete-key'
}


aws_bucket_name = {
    'raw':'source-files-raw',
    'transformed':'transformed-file-curated'
}

root_folder = 'lidc_small_dset/'

transformed_file_location = '../transformed'