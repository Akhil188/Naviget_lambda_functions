import io
import zipfile
from clients import s3_client
from notifications import send_error_notification

CHUNK_SIZE = 50 * 1024 * 1024  # 50MB chunks

def process_zip_file_streaming(bucket_name: str, object_key: str, extracted_prefix: str, user_id: str) -> bool:
    """Process the ZIP file using streaming to minimize memory usage"""
    try:
        print(f"Starting ZIP processing for object: {object_key}")
        
        # Get object size
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = response['ContentLength']
            print(f"File size: {file_size} bytes")
        except Exception as e:
            raise Exception(f"Failed to get object size: {str(e)}")
        
        # Stream the zip file in chunks
        zip_buffer = io.BytesIO()
        position = 0
        
        print("Starting chunk download")
        while position < file_size:
            chunk_end = min(position + CHUNK_SIZE, file_size)
            try:
                chunk = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Range=f'bytes={position}-{chunk_end-1}'
                )['Body'].read()
                
                zip_buffer.write(chunk)
                position = chunk_end
                print(f"Downloaded bytes: {position}/{file_size}")
                
            except Exception as e:
                raise Exception(f"Failed to download chunk at position {position}: {str(e)}")

        zip_buffer.seek(0)
        print("Download complete, starting extraction")
        
        # Process ZIP contents
        try:
            with zipfile.ZipFile(zip_buffer) as zip_file:
                file_count = 0
                for file_info in zip_file.infolist():
                    if file_info.file_size > 0:  # Skip directories
                        extracted_key = f"{extracted_prefix}/{file_info.filename}"
                        print(f"Processing file: {file_info.filename}")
                        
                        # Stream each file from zip to S3
                        with zip_file.open(file_info) as source_file:
                            s3_client.upload_fileobj(
                                source_file,
                                bucket_name,
                                extracted_key,
                                ExtraArgs={'ContentType': 'application/octet-stream'}
                            )
                            file_count += 1
                            print(f"Extracted: {extracted_key}")
                
                print(f"Successfully extracted {file_count} files")
                return True
                
        except Exception as e:
            raise Exception(f"Failed during ZIP extraction: {str(e)}")
        
    except Exception as e:
        error_description = f"Failed to process ZIP file {object_key} in bucket {bucket_name}. Error: {str(e)}"
        print(f"Error in process_zip_file_streaming: {error_description}")
        send_error_notification(
            error_type="ZIP_PROCESSING_ERROR",
            error_title="ZIP File Processing Failed",
            error_description=error_description,
            user_id=user_id
        )
        return False 