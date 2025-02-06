from datetime import datetime
from clients import supabase

def update_file_record(upload_id: str, status: str) -> bool:
    """Update the file status in Supabase"""
    try:
        data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        supabase.table('files').update(data).eq('file_id', upload_id).execute()
        return True
            
    except Exception as e:
        print(f"Failed to update file record (ID: {upload_id}) with status: {status}. Error: {str(e)}")
        return False

def insert_status_history(file_id: str, user_id: str, status: str, status_id: str, details: str = None) -> bool:
    """Insert status history record"""
    try:
        data = {
            'status_id': status_id,
            'file_id': file_id,
            'status': status,
            'details': details,
            'uploaded_by': user_id,
            'created_at': datetime.utcnow().isoformat()
        }
        
        supabase.table('status_history').insert(data).execute()
        return True
            
    except Exception as e:
        print(f"Failed to insert status history for file {file_id}. Error: {str(e)}")
        return False

def insert_hierarchy_data(hierarchy: dict) -> bool:
    """Insert hierarchy data into the database"""
    try:
        for patient_id, studies in hierarchy.items():
            for study_uid, series in studies.items():
                for series_uid, modalities in series.items():
                    # Validate modalities
                    if not isinstance(modalities, list) or not all(isinstance(mod, str) for mod in modalities):
                        print(f"Invalid modalities for patient_id: {patient_id}, study_uid: {study_uid}, series_uid: {series_uid}. Modalities: {modalities}")
                        continue  # Skip invalid entries

                    data = {
                        'patient_id': patient_id,
                        'study_uid': study_uid,
                        'series_uid': series_uid,
                        'modalities': ', '.join(modalities) if modalities else 'None'  # Handle empty modalities
                    }
                    print(f"Inserting data: {data}")  # Log the data being inserted
                    response = supabase.table('hierarchy').insert(data).execute()
                    if response.status_code != 201:  # Check for successful insertion
                        print(f"Failed to insert data: {data}, Response: {response.data}, Error: {response.error}")
                        # Log the entire response for better debugging
                        print(f"Response details: {response}")
        return True
            
    except Exception as e:
        print(f"Failed to insert hierarchy data. Error: {str(e)}")
        return False 