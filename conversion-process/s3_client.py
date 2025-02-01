import boto3
import tempfile
import os

class S3Client:
    def __init__(self):
        self.client = boto3.client('s3')
        self.bucket_name = 'naviget-chandu-test'

    def generate_output_files(self, company_id: str, user_id: str, upload_id: str) -> list:
        """Generate and upload output files to S3."""
        base_output_path = f"{company_id}/{user_id}/uploads/{upload_id}/output/"
        output_files = []

        for idx in range(1, 4):
            output_file_key = f"{base_output_path}{upload_id}-output{idx}"
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(f"Output file {idx} content for upload ID: {upload_id}".encode())
                tmp_file.flush()

                self.client.upload_file(tmp_file.name, self.bucket_name, output_file_key)
                print(f"Uploaded output file: {output_file_key}")
                output_files.append(output_file_key)
            
            os.remove(tmp_file.name)

        return output_files 