import os
import pydicom
from collections import defaultdict

def process_dicom_files(extracted_folder):
    # Initialize hierarchy
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    # Iterate through extracted files
    print(f"Looking for DICOM files in: {extracted_folder}")
    for root, dirs, files in os.walk(extracted_folder):
        print(f"Found directory: {root} with files: {files}")
        for file in files:
            file_path = os.path.join(root, file)
            print(f"Attempting to read file: {file_path}")  # Debugging line
            try:
                dicom = pydicom.dcmread(file_path, stop_before_pixels=True)
                patient_id = dicom.get("PatientID", "Unknown_Patient")
                study_uid = dicom.get("StudyInstanceUID", "Unknown_Study")
                series_uid = dicom.get("SeriesInstanceUID", "Unknown_Series")
                modality = dicom.get("Modality", "Unknown")
                hierarchy[patient_id][study_uid][series_uid].append(modality)
                print(f"Processed file: {file_path}")
                print(f"  Patient ID: {patient_id}")
                print(f"  Study UID: {study_uid}")
                print(f"  Series UID: {series_uid}")
                print(f"  Modality: {modality}")
            except Exception as e:
                print(f"Failed to process file: {file_path}, error: {e}")
                continue  # Skip unreadable or non-DICOM files
    
    print("Final hierarchy:")
    for patient_id, studies in hierarchy.items():
        print(f"Patient ID: {patient_id}")
        for study_uid, series in studies.items():
            print(f"  Study UID: {study_uid}")
            for series_uid, modalities in series.items():
                print(f"    Series UID: {series_uid}")
                print(f"      Modalities: {modalities}")
    
    return hierarchy

def cleanup_extracted_files(extracted_folder):
    # Cleanup extracted files
    for root, dirs, files in os.walk(extracted_folder, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        os.rmdir(root)
