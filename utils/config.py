dicom_attributes = {'attributes':["PatientID", "PatientName", "PatientBirthDate", "PatientSex", "PatientAge", "EthnicGroup","PatientWeight", "PatientSize", "StudyInstanceUID", "StudyDate", "StudyTime", "AccessionNumber", "ReferringPhysicianName", "StudyID", "StudyDescription", "PatientID", "SeriesInstanceUID", "SeriesNumber", "Modality", "SeriesDescription", "BodyPartExamined", "StudyInstanceUID", "SOPInstanceUID", "InstanceNumber", "ImageType", "AcquisitionDate", "AcquisitionTime", "PixelSpacing", "SliceThickness","SliceLocation","FrameOfReferenceUID", "SeriesInstanceUID", "Manufacturer", "ManufacturerModelName", "StationName", "DeviceSerialNumber", "SoftwareVersions", "ProtocolName", "ContrastBolusAgent", "ScanningSequence", "SequenceVariant", "ScanOptions", "Rows", "Columns", "BitsAllocated", "BitsStored", "HighBit", "PixelRepresentation", "PhotometricInterpretationInstitutionName", "InstitutionName", "InstitutionalDepartmentName", "FrameOfReferenceUID"]}


aws_bucket_name = {
    'raw':'source-files-raw',
    'transformed':'transformed-file-curated'
}

root_folder = 'lidc_small_dset/'

transformed_file_location = '../transformed'

datamart_attributes = {
'patientInfo' : [
    "PatientID",
    "PatientName",
    "PatientBirthDate",
    "PatientSex",
    "PatientAge",
    "EthnicGroup",
    "PatientWeight",
    "PatientSize"
],
'studyInfo' : [
    "StudyInstanceUID",
    "PatientID",
    "StudyDate",
    "StudyTime",
    "AccessionNumber",
    "ReferringPhysicianName",
    "StudyID",
    "StudyDescription"
],
'seriesInfo' : [
    "SeriesInstanceUID",
    "StudyInstanceUID",
    "SeriesNumber",
    "Modality",
    "SeriesDescription",
    "BodyPartExamined"
],
'imageInfo' : [
    "SOPInstanceUID",
    "SeriesInstanceUID",
    "InstanceNumber",
    "ImageType",
    "AcquisitionDate",
    "AcquisitionTime",
    "PixelSpacing",
    "SliceThickness",
    "SliceLocation",
    "FrameOfReferenceUID"
],
'equipmentInfo' : [
    "Manufacturer",
    "ManufacturerModelName",
    "StationName",
    "DeviceSerialNumber",
    "SoftwareVersions"
],
'procedureInfo' : [
    "ProtocolName",
    "ContrastBolusAgent",
    "ScanningSequence",
    "SequenceVariant",
    "ScanOptions"
],
'pixelDataInfo' : [
    "Rows",
    "Columns",
    "BitsAllocated",
    "BitsStored",
    "HighBit",
    "PixelRepresentation",
    "PhotometricInterpretationInstitutionName"
],
'miscInfo' : [
    "InstitutionName",
    "InstitutionalDepartmentName",
    "FrameOfReferenceUID"
]
}

datamart_primary_keys = {
    'patientInfo' : "PatientID",
    'studyInfo' : "StudyInstanceUID",
    'seriesInfo' : "SeriesInstanceUID",
    'imageInfo' : "SOPInstanceUID"
}