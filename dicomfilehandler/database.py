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