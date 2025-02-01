import json
from database import Database
from sns_messages import SNSMessages
from s3_client import S3Client

def lambda_handler(event, context):
    print("### Lambda 3 Invoked ###")
    print("Event: ", json.dumps(event, indent=4))

    try:
        # Initialize services
        db = Database()
        sns = SNSMessages()
        s3 = S3Client()

        # Validate event structure
        if not event.get('Records') or not event['Records'][0].get('Sns'):
            raise ValueError("Invalid event structure: Missing SNS records")

        # Parse SNS message
        try:
            sns_message = json.loads(event['Records'][0]['Sns']['Message'])
            print("Parsed SNS Message: ", sns_message)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid SNS message format: {str(e)}")

        # Validate required fields
        required_fields = ['company_id', 'user_id', 'upload_id', 'status_id', 'conversion_id']
        missing_fields = [field for field in required_fields if not sns_message.get(field)]
        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            sns.send_error_notification(
                error_type="ValidationError",
                error_title="Missing Required Fields",
                error_description=error_msg,
                user_id=sns_message.get('user_id', 'unknown')
            )
            raise ValueError(error_msg)

        # Extract message data
        company_id = sns_message['company_id']
        user_id = sns_message['user_id']
        upload_id = sns_message['upload_id']
        status_id = sns_message['status_id']
        conversion_id = sns_message['conversion_id']

        # Generate and upload output files
        output_files = s3.generate_output_files(company_id, user_id, upload_id)

        # Update database records
        try:
            db.update_files_status(upload_id, 'converted')
            db.update_status_history(status_id, 'converted', 'Output files generated.')
            db.update_file_conversion(conversion_id, output_files[-1], 'completed')
            db.update_user_status(user_id)
        except Exception as db_error:
            sns.send_error_notification(
                error_type="DatabaseError",
                error_title="Database Update Failed",
                error_description=str(db_error),
                user_id=user_id
            )
            raise Exception(f"Database update failed: {str(db_error)}")

        # Send completion notification
        sns.send_completion_notification(company_id, user_id, upload_id, conversion_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                'message': 'Output files generated successfully.',
                'upload_id': upload_id,
                'output_files': output_files
            })
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"Error occurred: {str(e)}"
            })
        }
