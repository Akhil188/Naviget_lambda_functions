import boto3
from typing import Optional

class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def generate_presigned_url(self, bucket_name: str, object_key: str, expiration: int = 3600) -> Optional[str]:
        """Generate a presigned URL for accessing an S3 object."""
        try:
            return self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration
            )
        except Exception as e:
            print(f"Error generating presigned URL for {object_key}: {e}")
            return None

    def list_objects(self, bucket: str, prefix: str):
        """List objects in S3 bucket with given prefix."""
        return self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    def download_file(self, bucket: str, key: str, local_path: str):
        """Download file from S3 to local path."""
        self.s3_client.download_file(bucket, key, local_path)

    def upload_file(self, local_path: str, bucket: str, key: str):
        """Upload file from local path to S3."""
        self.s3_client.upload_file(local_path, bucket, key) 