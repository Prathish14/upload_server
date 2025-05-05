import aioboto3
import os
from typing import Optional

class BotoB2:
    session = aioboto3.Session(aws_access_key_id=os.environ["B2_BOTO_ACCESS_KEY_ID"],
                               aws_secret_access_key=os.environ["B2_BOTO_ACCESS_KEY"])

    @staticmethod
    async def upload_file_async(content, file_name, content_type):
        try:
            async with BotoB2.session.client('s3', endpoint_url=os.environ["B2_BOTO_ENDPOINT_URL"]) as s3_client:
                response = await s3_client.put_object(Body=content, Bucket=os.environ["B2_BUCKET_NAME"], Key=file_name, ContentType=content_type)
                error_msg = None
                if response is None:
                    error_msg = f'null response from upload content_type: {content_type}, file_name: {file_name}'
                else:
                    response_meta_data = response.get('ResponseMetadata')
                    if response_meta_data is None:
                        error_msg = f'null response from upload content_type: {content_type}, file_name: {file_name} response: {response}'
                    else:
                        status_code = response_meta_data.get('HTTPStatusCode')
                        if status_code != 200:
                            error_msg = f'null response from upload content_type: {content_type}, file_name: {file_name} response: {response}'
                if error_msg:
                    return None
                return response['ResponseMetadata']['HTTPHeaders']['x-amz-version-id']
            
        except Exception as e:
            error_msg = f'error upload_file content_type: {content_type} path:{file_name}, error: {e.__str__()}'
            print(error_msg)
            return None

    @staticmethod
    async def upload_ftp_uploaded_image_to_event_bucket(content, content_type, file_name, event_id, event_user_id):
        file_path_raw = f"events/{event_id}/{event_user_id}/raw/{file_name}"
        raw_id = await BotoB2.upload_file_async(content, file_path_raw, content_type)
        print(f"upload completed with raw_id:{raw_id} and file_path:{file_path_raw}")
        return raw_id, file_path_raw
