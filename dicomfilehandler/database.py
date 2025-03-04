from datetime import datetime
from supabase import create_client, Client
import os
import traceback
from postgrest.exceptions import APIError
import random
import uuid

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

def insert_hierarchy_data(hierarchy: list) -> bool:
    try:
        if not hierarchy:
            print("No data to insert. Skipping...")
            return False
        
        print(f"Inserting batch data: {len(hierarchy)} records")
        response = supabase.table("series").insert(hierarchy).execute()

        if response.error:
            print(f"Supabase Insert Error: {response.error}")
            return False
        
        print(f"Supabase Response: {response.data}")
        return True
    except Exception as e:
        print(f"Unexpected error while inserting: {repr(e)}")
        return False
        
def insert_patient_data(patients: list) -> bool:
    """Insert patient records into the 'patients' table."""
    try:
        if not patients:
            print("No patient data to insert. Skipping...")
            return False
        
        print(f"Inserting batch patient data: {len(patients)} records")
        response = supabase.table("patients").insert(patients).execute()

        if response.error:
            print(f"Supabase Insert Error: {response.error}")
            return False
        
        print(f"Supabase Response: {response.data}")
        return True
    except Exception as e:
        print(f"Unexpected error while inserting patient data: {repr(e)}")
        return False
