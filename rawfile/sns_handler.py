import json
import boto3
from datetime import datetime
from typing import Dict, Any

class SNSHandler:
    def __init__(self, error_topic_arn: str, pending_topic_arn: str):
        self.sns_client = boto3.client('sns')
        self.error_topic_arn = error_topic_arn
        self.pending_topic_arn = pending_topic_arn

    def send_error_notification(self, error_type: str, error_title: str, 
                              error_description: str, user_id: str) -> None:
        """Send error notification via SNS"""
        try:
            message = {
                'error_type': error_type,
                'error_title': error_title,
                'error_description': error_description,
                'user_id': user_id
            }
            
            self.sns_client.publish(
                TopicArn=self.error_topic_arn,
                Message=json.dumps(message)
            )
            print(f"Error notification sent: {error_type} - {error_title}")
            
        except Exception as e:
            print(f"Failed to send error notification: {str(e)}")

    def send_success_notification(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Send success notification via SNS"""
        response = self.sns_client.publish(
            TopicArn=self.pending_topic_arn,
            Message=json.dumps(data),
            Subject="File Extraction Status"
        )
        return response 