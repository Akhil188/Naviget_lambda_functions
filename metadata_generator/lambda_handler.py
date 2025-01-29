import openai
import json
import boto3
import os
from datetime import datetime
from uuid import uuid4
from supabase import create_client, Client
import re

# Environment variables (assumed to be set in Lambda)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
# SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

# Load the JSON file
def load_json(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Generate a prompt based on JSON data and a user query
def generate_prompt(dicom_metadata):
    """Generate prompt for OpenAI to extract important DICOM metadata"""
    prompt = f"""
    You are an expert radiologist analyzing DICOM metadata. From the following DICOM metadata, extract ALL clinically significant attributes that would be important for:
    1. Patient identification and demographics
    2. Study and series information
    3. Image acquisition parameters
    4. Technical details needed for image viewing/processing
    5. Equipment and protocol information

    Required fields to ALWAYS include if present:
    - Patient Information:
        * PatientName
        * PatientID
        * PatientBirthDate
        * PatientSex
        * PatientAge
        * PatientWeight
    
    - Study Information:
        * StudyDescription
        * StudyDate
        * StudyTime
        * AccessionNumber
        * ReferringPhysicianName
        * StudyInstanceUID
    
    - Series Information:
        * SeriesDescription
        * SeriesNumber
        * SeriesInstanceUID
        * Modality
        * BodyPartExamined
    
    - Image Technical Details:
        * SliceThickness
        * SliceLocation
        * ImageType
        * PixelSpacing
        * ImageOrientationPatient
        * ImagePositionPatient
        * WindowCenter
        * WindowWidth
        * PhotometricInterpretation
        * SamplesPerPixel
        * Rows
        * Columns
    
    - Acquisition Parameters:
        * KVP
        * ExposureTime
        * XRayTubeCurrent
        * ExposureIndex
        * AcquisitionDate
        * AcquisitionTime
        * RepetitionTime
        * EchoTime
        * ImagingFrequency
        * MagneticFieldStrength
        * FlipAngle
    
    - Equipment Information:
        * Manufacturer
        * ManufacturerModelName
        * SoftwareVersions
        * DeviceSerialNumber
        * StationName
        * InstitutionName

    Return a JSON object containing ALL available attributes from these categories, maintaining their exact values.
    Do not omit any fields that match these categories.
    Do not include fields that are empty or null.

    DICOM Metadata:
    {json.dumps(dicom_metadata, indent=2)}
    """
    return prompt

# Query OpenAI API
def query_openai_api(prompt, api_key):
    """Query OpenAI API for metadata extraction"""
    try:
        openai.api_key = api_key
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",  # Using 3.5 to avoid token limits
            messages=[
                {"role": "system", "content": "You are a DICOM metadata expert. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1000,
            temperature=0.1
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"OpenAI API Error: {str(e)}")
        return {}

def insert_dicom_image(conversion_id, dicom_data):
    """
    Insert DICOM image information into dicom_images table
    """
    try:
        print("=== Inserting DICOM Image ===")
        print(f"Conversion ID: {conversion_id}")
        
        # Clean and validate data
        data = {
            'conversion_id': conversion_id,
            'sop_instance_uid': str(dicom_data.get('sop_instance_uid', '')),
            'file_path': str(dicom_data.get('file_path', '')),
            'instance_number': str(dicom_data.get('instance_number', '')),
            'slice_location': str(dicom_data.get('slice_location', '')),
            'image_position_patient': str(dicom_data.get('image_position_patient', '')),
            'image_orientation_patient': str(dicom_data.get('image_orientation_patient', '')),
            'pixel_spacing': str(dicom_data.get('pixel_spacing', ''))
        }
        
        print("Data to insert:", json_safe_dumps(data))
        
        # Validate required fields
        if not data['sop_instance_uid'] or not data['file_path']:
            raise ValueError("Missing required fields: sop_instance_uid and file_path must not be empty")
        
        # Insert into database
        try:
            response = supabase.table('dicom_images').insert(data).execute()
            print("Database response:", json_safe_dumps(response.data))
            
            if not response.data or len(response.data) == 0:
                raise Exception("No data returned from insert operation")
            
            image_id = response.data[0].get('image_id')
            if not image_id:
                raise Exception("No image_id returned from insert operation")
                
            print(f"Successfully inserted DICOM image with ID: {image_id}")
            return image_id
            
        except Exception as e:
            print("Database Error Details:")
            print(f"Error Type: {type(e).__name__}")
            print(f"Error Message: {str(e)}")
            if hasattr(e, 'response'):
                print(f"Response: {e.response}")
            raise
            
    except Exception as e:
        print("!!! DICOM Image Insert Error !!!")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise

def insert_dicom_metadata(image_id, metadata):
    """
    Insert DICOM metadata into dicom_metadata table
    """
    try:
        print("=== Inserting DICOM Metadata ===")
        print(f"Image ID: {image_id}")
        print(f"Number of metadata fields to insert: {len(metadata)}")
        
        metadata_records = []
        for key, value in metadata.items():
            if value is not None:
                try:
                    # Ensure value is a string and not too long
                    str_value = str(value)
                    if len(str_value) > 1000:  # Add length limit if needed
                        str_value = str_value[:1000] + "..."
                    
                    metadata_records.append({
                        'image_id': image_id,
                        'attribute_key': key,
                        'attribute_value': str_value
                    })
                except Exception as e:
                    print(f"Error processing metadata field {key}: {str(e)}")
        
        print(f"Prepared {len(metadata_records)} records for insertion")
        
        # Insert in batches to avoid request size limits
        batch_size = 100
        for i in range(0, len(metadata_records), batch_size):
            batch = metadata_records[i:i + batch_size]
            try:
                print(f"Inserting batch {i//batch_size + 1} of {(len(metadata_records)-1)//batch_size + 1}")
                response = supabase.table('dicom_metadata').insert(batch).execute()
                print(f"Successfully inserted batch with {len(batch)} records")
            except Exception as e:
                print(f"Error inserting batch: {str(e)}")
                print("First record in failed batch:", batch[0] if batch else "No records")
                raise
        
        return True
    except Exception as e:
        print("!!! DICOM Metadata Insert Error !!!")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise

def clean_dicom_value(value):
    """Clean DICOM values for JSON formatting"""
    if not value:
        return None
    
    # Convert to string
    value_str = str(value)
    
    # Remove DICOM tags and type indicators
    value_str = re.sub(r'\([0-9A-Fa-f]{4},\s*[0-9A-Fa-f]{4}\)', '', value_str)
    value_str = re.sub(r'[A-Z]{2}:', '', value_str)
    
    # Clean up arrays and quotes
    value_str = value_str.strip('[]').strip("'").strip('"')
    
    # Remove newlines and extra spaces
    value_str = ' '.join(value_str.split())
    
    return value_str

def parse_dicom_data(json_data):
    try:
        print("=== Parsing DICOM Data ===")
        
        # Get DICOM metadata
        dicom_metadata = json_data.get('dicom_metadata', {})
        print("Available DICOM attributes:", list(dicom_metadata.keys()))

        # Extract required fields for dicom_images table
        dicom_data = {
            'sop_instance_uid': dicom_metadata.get('SOPInstanceUID'),
            'file_path': dicom_metadata.get('FileSetID'),
            'instance_number': dicom_metadata.get('InstanceNumber'),
            'slice_location': dicom_metadata.get('SliceLocation'),
            'image_position_patient': dicom_metadata.get('ImagePositionPatient'),
            'image_orientation_patient': dicom_metadata.get('ImageOrientationPatient'),
            'pixel_spacing': dicom_metadata.get('PixelSpacing')
        }
        
        print("Extracted DICOM Data:", json_safe_dumps(dicom_data))

        # Use OpenAI to extract important metadata
        print("Requesting OpenAI to analyze DICOM metadata...")
        prompt = generate_prompt(dicom_metadata)
        important_metadata = query_openai_api(prompt, OPENAI_API_KEY)
        print("OpenAI extracted metadata fields:", list(important_metadata.keys()))
        
        # Add DirectoryRecordSequence if present
        if 'DirectoryRecordSequence' in dicom_metadata:
            sequence_value = dicom_metadata['DirectoryRecordSequence']
            if sequence_value:
                # Process DirectoryRecordSequence
                patient_count = str(sequence_value).count("'PATIENT'")
                study_count = str(sequence_value).count("'STUDY'")
                series_count = str(sequence_value).count("'SERIES'")
                image_count = str(sequence_value).count("'IMAGE'")
                
                important_metadata['DirectoryRecordSequence'] = {
                    'count': patient_count + study_count + series_count + image_count,
                    'types': {
                        'PATIENT': patient_count,
                        'STUDY': study_count,
                        'SERIES': series_count,
                        'IMAGE': image_count
                    }
                }
        
        print(f"Total metadata fields selected: {len(important_metadata)}")
        print("Categories of metadata:", [k for k in important_metadata.keys() if not k.startswith('_')])
        
        return dicom_data, important_metadata
        
    except Exception as e:
        print("!!! DICOM Data Parsing Error !!!")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        raise

def json_safe_dumps(obj, indent=2):
    """Helper function to handle JSON serialization of datetime objects"""
    def default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    return json.dumps(obj, default=default, indent=indent)

# Lambda Handler
def lambda_handler(event, context):     
    print("### Lambda Invoked ###")
    print("Event:", json_safe_dumps(event))

    try:
        # Extract SNS message details
        print("Extracting SNS message...")
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        print("SNS Message:", json_safe_dumps(sns_message))
        
        company_id = sns_message.get('company_id')
        user_id = sns_message.get('user_id')
        upload_id = sns_message.get('upload_id')
        conversion_id = sns_message.get('conversion_id')
        status = sns_message.get('status')

        if status != 'Success':
            raise Exception("Invalid status received, expected 'Success'.")

        # Load JSON data from S3
        json_file_path = f"{company_id}/{user_id}/uploads/{upload_id}/temp/"
        print(f"Looking for JSON files in: s3://naviget-chandu-test/{json_file_path}")
        
        try:
            response = s3_client.list_objects_v2(
                Bucket='naviget-chandu-test',
                Prefix=json_file_path
            )
            print("S3 list response:", json_safe_dumps(response))
            
            json_files = [obj['Key'] for obj in response.get('Contents', []) 
                         if obj['Key'].endswith('.json')]
            print("Found JSON files:", json_files)
            
            if not json_files:
                raise Exception(f"No JSON files found in s3://naviget-chandu-test/{json_file_path}")
                
            json_file = json_files[0]
            print(f"Reading file: {json_file}")
            
            s3_response = s3_client.get_object(
                Bucket='naviget-chandu-test',
                Key=json_file
            )
            print("Got S3 object, reading content...")
            
            content = s3_response['Body'].read().decode('utf-8')
            json_data = json.loads(content)
            print("Loaded JSON data:", json_safe_dumps(json_data))
            
        except Exception as e:
            print(f"S3 Error Details: {str(e)}")
            print(f"Error Type: {type(e).__name__}")
            raise Exception(f"Error accessing S3: {str(e)}")

        try:
            # Parse DICOM data directly from settings.json
            dicom_data, metadata = parse_dicom_data(json_data)
            print("Parsed DICOM data:", json_safe_dumps(dicom_data))
            print("Parsed metadata count:", len(metadata))

            # Insert DICOM image and get image_id
            print("Inserting DICOM image...")
            image_id = insert_dicom_image(conversion_id, dicom_data)
            print(f"Got image_id: {image_id}")

            # Insert metadata using the returned image_id
            print("Inserting DICOM metadata...")
            insert_dicom_metadata(image_id, metadata)
            print("Database insertions completed successfully")

            # Publish SNS completion message
            completion_message = {
                "company_id": company_id,
                "user_id": user_id,
                "conversion_id": conversion_id,
                "status": "Completed",
                "timestamp": datetime.utcnow().isoformat(),
            }

            # sns_response = sns_client.publish(
            #     TopicArn=SNS_TOPIC_ARN,
            #     Message=json.dumps(completion_message),
            #     Subject='File Processing Completed'
            # )

            return {
                "statusCode": 200,
                "body": json.dumps({
                    'message': 'File processing completed successfully.',
                    'upload_id': upload_id,
                })
            }

        except Exception as e:
            print("!!! Database Operation Error !!!")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            raise

    except Exception as e:
        error_message = f"Error occurred: {str(e)}"
        print(error_message)
        print("Full error details:", e.__class__.__name__)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': error_message
            })
        }
