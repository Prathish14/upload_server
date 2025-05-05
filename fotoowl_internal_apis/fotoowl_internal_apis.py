import aiohttp
import base64
import json
import os

fotoowl_internal_api_key = os.environ.get("FOTOOWL_INTERNAL_API_KEY")
fotoowl_event_picture_process_api = os.environ.get("FOTOOWL_EVENT_PICTURE_PROCESS_API")

class FotoowlInternalApis:
    @staticmethod
    async def send_uploded_image_info_to_event_picture_process(
        event_id: int,
        image_name: str,
        mime_type: str,
        b2_id: str,
        path: str,
        user_id: str,
        height: int,
        width: int):

        try:
            url = fotoowl_event_picture_process_api

            headers = {
                'Authorization': f"Basic {fotoowl_internal_api_key}",
                'Content-Type': 'application/json'
            }

            body = {
                "event_id": event_id,
                "image_name": image_name,
                "mime_type": mime_type,
                "b2_id": b2_id,
                "path": path,
                "user_id": user_id,
                "height": height,
                "width": width,
                "collection_ids": [-1]
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=body) as response:
                    text = await response.text()
                    print(f"after sending image info in process api, response code: {response.status}")
                    return response.status

        except Exception as e:
            print(f"Error sending uploaded image info: {e}")
            return None


 
    
    
