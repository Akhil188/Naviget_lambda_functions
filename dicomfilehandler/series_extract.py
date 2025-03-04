import os
import pydicom
import uuid
from collections import defaultdict
from datetime import datetime

def safe_json_serialize(value):
        if isinstance(value, pydicom.multival.MultiValue):
            return list(value)  # Convert MultiValue to a standard list
        return value

def process_dicom_files(extracted_folder, user_id, upload_id):
    # Initialize hierarchy
    hierarchy = []
    
    # Iterate through extracted files
    print(f"Looking for DICOM files in: {extracted_folder}")
    for root, dirs, files in os.walk(extracted_folder):
        print(f"Found directory: {root} with files: {files}")
        for file in files:
            file_path = os.path.join(root, file)
            try:
                dicom = pydicom.dcmread(file_path, stop_before_pixels=True)
                
                # Extract required metadata
                study_id= dicom.get("StudyInstanceUID", "None")#"Unknown_Study")
                series_id = dicom.get("SeriesInstanceUID","None")# "Unknown_Series")
                dicom_series_id = dicom.get("SeriesInstanceUID", "None")
                modality = dicom.get("Modality", "None")
                series_description = dicom.get("SeriesDescription", "None")
                series_number = dicom.get("SeriesNumber", 0)
                slice_thickness = dicom.get("SliceThickness", 0.0)
                number_of_slices = dicom.get("ImagesInAcquisition", 0)  # Alternative: dicom.get("NumberOfSlices", 0)

                image_orientation = safe_json_serialize(dicom.get("ImageOrientationPatient", "None"))
                pixel_spacing = safe_json_serialize(dicom.get("PixelSpacing", "None"))

                manufacturer = dicom.get("Manufacturer", "None")
                manufacturer_model_name = dicom.get("ManufacturerModelName", "None")
                included_in_conversion = True  # Assuming all processed files are included
                patient_name=str(dicom.get("PatientName", "None"))

                created_at = datetime.utcnow().isoformat()
                if not study_id or study_id.lower() == "none":
                    print(f"Skipping invalid patient ID: {patient_id}")
                    continue
                # Append extracted metadata to hierarchy
                existing_entry = next((entry for entry in hierarchy if entry["series_id"] == series_id), None)
                if existing_entry is None:
                    hierarchy.append({
                        "series_upload_id":series_id + "_" + upload_id, 
                        "series_id": series_id,
                        "study_id": study_id,
                        "dicom_series_id": dicom_series_id,
                        "modality": modality,
                        "series_description": series_description,
                        "series_number": int(series_number) if series_number else None,
                        "slice_thickness": float(slice_thickness) if slice_thickness else None,
                        "number_of_slices": int(number_of_slices) if number_of_slices else None,
                        "image_orientation": image_orientation,
                        "pixel_spacing": pixel_spacing,
                        "manufacturer": manufacturer,
                        "manufacturer_model_name": manufacturer_model_name,
                        # "included_in_conversion": included_in_conversion,
                        "created_at": created_at,
                        "user_id": user_id,
                        "file_id": upload_id,
                        "patient_name":patient_name
                    })
                #else:
                
            except Exception as e:
                print(f"Failed to process file: {file_path}, error: {e}")
                continue  # Skip unreadable or non-DICOM files
    
    return hierarchy

def extract_patient_data_from_dicom(extracted_folder, user_id, upload_id):
    """Extracts patient metadata from DICOM files while linking them to their series."""
    
    patients = []
    seen_patients = set()  # Avoid duplicate patient entries

    print(f"Scanning DICOM files in: {extracted_folder}")
    for root, _, files in os.walk(extracted_folder):
        print(f"Processing directory: {root} with {len(files)} files")
        
        for file in files:
            file_path = os.path.join(root, file)
            try:
                dicom = pydicom.dcmread(file_path, stop_before_pixels=True)

                # Get series metadata correctly
                series_id = dicom.get("SeriesInstanceUID", None)  # Fetch series_id from DICOM
                if not series_id or series_id.lower() == "none":
                    print(f"Skipping patient record - Missing series_id in file: {file_path}")
                    continue  # Skip patients with no series_id

                series_upload_id = f"{series_id}_{upload_id}"  # Link with upload_id

                # Extract and anonymize missing fields
                dicom_patient_id = dicom.get("PatientID", f"ANON_{uuid.uuid4().hex[:8]}")
                first_name = str(dicom.get("PatientName", "ANONYMOUS^PATIENT")).split("^")[0] or "ANONYMOUS"
                last_name = str(dicom.get("PatientName", "ANONYMOUS^PATIENT")).split("^")[-1] or "PATIENT"
                birth_date = dicom.get("PatientBirthDate", None)  # Keep None if not available
                sex = dicom.get("PatientSex", "A")  # Default to 'A' for anonymous users
                
                # Ensure only allowed values for sex
                allowed_sex_values = {"M", "F", "O", "A"}
                if sex not in allowed_sex_values:
                    sex = "A"  # Assign 'A' if invalid value found

                # Ensure unique patient ID per patient data
                patient_id = str(uuid.uuid4())  
                patient_user_id = user_id 
                created_at = datetime.utcnow().isoformat()
                updated_at = datetime.utcnow().isoformat()

                if dicom_patient_id in seen_patients:
                    continue  # Skip duplicate patient entries
                
                seen_patients.add(dicom_patient_id)

                # Append patient metadata to the list
                patients.append({
                    "patient_id": patient_id,
                    "dicom_patient_id": dicom_patient_id,
                    "first_name": first_name,
                    "last_name": last_name,
                    "birth_date": birth_date if birth_date and birth_date != "" else None,
                    "sex": sex,
                    "contact_info": {"email": None, "phone": None},
                    "patient_user_id": patient_user_id,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "series_upload_id": series_upload_id,  # Keeping this for reference
                })

            except Exception as e:
                print(f"Error processing DICOM file: {file_path}, error: {e}")
                continue  # Skip unreadable or non-DICOM files

    return patients


    
def cleanup_extracted_files(extracted_folder):
    for root, dirs, files in os.walk(extracted_folder, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))  # Remove subdirectories
    os.rmdir(extracted_folder)  # Finally, remove the extracted_folder itself
