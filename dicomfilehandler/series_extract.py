import os
import pydicom
from collections import defaultdict

def process_dicom_files(extracted_folder):
    # Initialize hierarchy
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    
    # Iterate through extracted files
    print(f"Looking for DICOM files in: {extracted_folder}")
    for root, dirs, files in os.walk(extracted_folder):
        print(f"Found directory: {root} with files: {files}")
        for file in files:
            file_path = os.path.join(root, file)
            try:
                dicom = pydicom.dcmread(file_path, stop_before_pixels=True)
                patient_id = dicom.get("PatientID", "None")#"Unknown_Patient")
                study_uid = dicom.get("StudyInstanceUID", "None")#"Unknown_Study")
                series_uid = dicom.get("SeriesInstanceUID","None")# "Unknown_Series")
                modality = dicom.get("Modality","None")# "Unknown")
                hierarchy[patient_id][study_uid][series_uid].add(modality)
            except Exception as e:
                print(f"Failed to process file: {file_path}, error: {e}")
                continue  # Skip unreadable or non-DICOM files
    print("hierarchy",hierarchy.items())
    return hierarchy

def cleanup_extracted_files(extracted_folder):
    # Cleanup extracted files
    for root, dirs, files in os.walk(extracted_folder, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        os.rmdir(root)
