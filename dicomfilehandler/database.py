from datetime import datetime
from supabase import create_client, Client
import os
import traceback
from postgrest.exceptions import APIError
import random

# Replace these with your actual Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_file_record(upload_id: str, status: str) -> bool:
    """Update the file status in Supabase"""
    try:
        data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        print(data)
        
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
            'updated_by': user_id,
            'created_at': datetime.utcnow().isoformat()
        }
        print("Inserting data:", data) 
        supabase.table('status_history').insert(data).execute()
        return True
            
    except Exception as e:
        print(f"Failed to insert status history for file {file_id}. Error: {str(e)}")
        return False

def insert_hierarchy_data(file_id: str, hierarchy: dict) -> bool:
    """
    Insert hierarchy data into the database.
    """
    try:
        if not isinstance(hierarchy, dict) or not hierarchy:
            print("Invalid or empty hierarchy data provided.")
            return False

        for patient_id, studies in hierarchy.items():
            if not patient_id or patient_id.lower() == "none":
                print(f"Skipping invalid patient ID: {patient_id}")
                continue

            for study_uid, series in studies.items():
                for series_uid, modalities in series.items():
                    if isinstance(modalities, set):
                        modalities = list(modalities)

                    data = {
                        'file_id': file_id,
                        'patient_id': str(patient_id),   
                        'study_id': str(study_uid),        
                        'series_id': str(series_uid),      
                        'modality': ', '.join(modalities) if modalities else 'None'  
                    }

                    try:
                        print(f"Attempting to insert data: {data}")

                        response = supabase.table("patient_ids").insert([data]).execute()
                        print(f" Supabase Raw Response: {response}")
                    except Exception as e:
                        print(f" Unexpected error while inserting: {repr(e)}")
                        return False
        return True

    except Exception as main_error:
        print(f" Critical error in insert_hierarchy_data: {repr(main_error)}")
        return False

