import os
import asyncio
import aiofiles
import binascii
import numpy as np
import cv2 as cv
import mimetypes
from pathlib import Path
from b2_upload_service.b2_uploader import BotoB2
from fotoowl_internal_apis.fotoowl_internal_apis import FotoowlInternalApis

#WATCH_FOLDER = r"D:\Foto_Owl_dev\upload_server\temp"
WATCH_FOLDER = "/app/mnt/vfs"

async def process_file(file_path):
    print(f"Processing file: {file_path}")
    filename = Path(file_path).name
    mime_type, encoding = mimetypes.guess_type(file_path)
    async with aiofiles.open(file_path, 'rb') as file:
        binary_data = await file.read()

    
    content = binascii.b2a_base64(binary_data).decode("utf8")
    content = binascii.a2b_base64(content)
    jpg_as_np = np.frombuffer(content, dtype=np.uint8)
    img = cv.imdecode(jpg_as_np, flags=1)
    img_width = img.shape[1]
    img_height = img.shape[0]

    event_id = "1089"
    event_user_id = "j8NCXEn4MSXSqwBIJPrdPFvEfjY2"
 
    raw_id,uploaded_file_path = await BotoB2.upload_ftp_uploaded_image_to_event_bucket(content=binary_data, content_type=mime_type, file_name=filename, 
                                                                              event_id=event_id, event_user_id=event_user_id)
         
    if raw_id and uploaded_file_path:
        print("now informing api about file uploded")
        await FotoowlInternalApis.send_uploded_image_info_to_event_picture_process(event_id=event_id, image_name=filename, mime_type=mime_type,
                                                                                  b2_id=raw_id, path=filename, user_id=event_user_id, 
                                                                                  height=img_height, width=img_width)
 


    try:
        os.remove(file_path)
        print(f"Deleted file: {file_path}")
    except Exception as e:
        print(f"Failed to delete {file_path}: {e}")

async def watch_folder():
    while True:
        try:
            files = os.listdir(WATCH_FOLDER)
            tasks = []

            for filename in files:
                file_path = os.path.join(WATCH_FOLDER, filename)
                if os.path.isfile(file_path):
                    tasks.append(asyncio.create_task(process_file(file_path)))

            if tasks:
                await asyncio.gather(*tasks)

            await asyncio.sleep(5)  # polling interval
        except Exception as e:
            print(f"Error: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(watch_folder())
