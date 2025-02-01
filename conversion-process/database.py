from datetime import datetime
from supabase import create_client, Client
import os

class Database:
    def __init__(self):
        self.supabase: Client = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_KEY')
        )

    def update_files_status(self, file_id: str, status: str) -> None:
        """Update the status of a file in the files table."""
        data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        self.supabase.table('files').update(data).eq('file_id', file_id).execute()

    def update_status_history(self, status_id: str, status: str, details: str) -> None:
        """Update status history record."""
        data = {
            'status': status,
            'details': details,
        }
        self.supabase.table('status_history').update(data).eq('status_id', status_id).execute()

    def update_file_conversion(self, conversion_id: str, output_file_key: str, status: str) -> None:
        """Update the conversion status in the file_conversion table."""
        data = {
            'status': status,
            'output_folder': output_file_key,
            'completed_at': datetime.utcnow().isoformat()
        }
        self.supabase.table('file_conversion').update(data).eq('conversion_id', conversion_id).execute()

    def update_user_status(self, user_id: str) -> None:
        """Update user status to completed."""
        data = {
            'status': 'completed',
        }
        self.supabase.table('users').update(data).eq('user_id', user_id).execute() 