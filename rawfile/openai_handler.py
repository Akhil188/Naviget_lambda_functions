import os
import requests
from openai import OpenAI

class OpenAIHandler:
    def __init__(self):
        self.client = OpenAI()  # This automatically uses OPENAI_API_KEY from environment

    def generate_medical_image(self, description):
        """
        Generate a medical image using OpenAI's API based on the description.
        Returns the image data if successful, None otherwise.
        """
        try:
            # Enhance the prompt for better medical visualization
            enhanced_prompt = (
                f"Generate a professional medical illustration of a {description}. "
                "The image should be clear, detailed, and suitable for medical context. "
                "Use medical visualization style with anatomical accuracy."
            )

            response = self.client.images.generate(
                model="dall-e-3",  # Using the latest DALL-E model
                prompt=enhanced_prompt,
                size="1024x1024",
                quality="standard",
                n=1,
            )
            
            image_url = response.data[0].url
            
            # Download the image
            img_response = requests.get(image_url)
            if img_response.status_code == 200:
                return img_response.content
            return None
        except Exception as e:
            print(f"Error generating image: {str(e)}")
            return None 