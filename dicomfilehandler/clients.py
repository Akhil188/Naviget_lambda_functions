import os
from supabase import create_client, Client
import boto3

# Environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_API_KEY = os.getenv('SUPABASE_KEY')
SNS_TOPIC_ARN = os.getenv('PENDING_TOPIC_ARN')
ERROR_TOPIC_ARN = os.getenv('ERROR_TOPIC_ARN')

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)
s3_client = boto3.client('s3')
sns_client = boto3.client('sns') 