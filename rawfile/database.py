from datetime import datetime
from supabase import Client

class DatabaseHandler:
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client

    def update_files_status(self, file_id: str, status: str) -> None:
        """Update the status of a file in the files table."""
        data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        self.supabase.table('files').update(data).eq('file_id', file_id).execute()

    def update_status_history(self, status_id: str, status: str, details: str) -> None:
        """Update the status history table."""
        data = {
            'status': status,
            'details': details,
        }
        self.supabase.table('status_history').update(data).eq('status_id', status_id).execute()

    def insert_file_conversion(self, conversion_id: str, file_id: str, status: str, 
                             raw_file_path: str, presigned_raw_url: str, presigned_json_url: str, image_url: str) -> None:
        """Insert a new entry in the file_conversion table."""
        data = {
            'conversion_id': conversion_id,
            'file_id': file_id,
            'raw_file_path': raw_file_path,
            'status': status,
            'created_at': datetime.utcnow().isoformat(),
            'pre_signed_raw': presigned_raw_url,
            'pre_signed_json': presigned_json_url,
            'image_url': image_url
        }
        self.supabase.table('file_conversion').insert(data).execute() 