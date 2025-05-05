import os
import asyncio
import aiofiles
import binascii
import numpy as np
import cv2 as cv
import mimetypes
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from b2_upload_service.b2_uploader import BotoB2
from fotoowl_internal_apis.fotoowl_internal_apis import FotoowlInternalApis

WATCH_FOLDER = "/app/mnt/vfs"  # Change to your path if needed
#WATCH_FOLDER = r"D:\Foto_Owl_dev\upload_server\temp"
MAX_CONCURRENT_TASKS = 5
queue = asyncio.Queue(100)

# ──────────────────────────────────────────────────────────────────────────────
# Utility: Check file is fully written
def is_file_ready(file_path):
    try:
        initial_size = os.path.getsize(file_path)
        time.sleep(1)
        return os.path.exists(file_path) and os.path.getsize(file_path) == initial_size
    except Exception:
        return False

# ──────────────────────────────────────────────────────────────────────────────
# Utility: Decode image buffer
def decode_image(content):
    try:
        jpg_as_np = np.frombuffer(content, dtype=np.uint8)
        img = cv.imdecode(jpg_as_np, flags=1)
        if img is not None:
            return img.shape[1], img.shape[0]  # width, height
        return None, None
    except Exception as e:
        print(f"[decode_image] Error: {e}")
        return None, None

# ──────────────────────────────────────────────────────────────────────────────
# Worker Task: Process a file from queue
async def process_file(file_path):
    try:
        print(f"[process_file] Start: {file_path}")
        filename = Path(file_path).name
        mime_type, _ = mimetypes.guess_type(file_path)

        async with aiofiles.open(file_path, 'rb') as file:
            binary_data = await file.read()

        content = binascii.b2a_base64(binary_data).decode("utf8")
        content = binascii.a2b_base64(content)

        img_width, img_height = await asyncio.to_thread(decode_image, content)
        if not img_width or not img_height:
            print(f"[process_file] Skipping invalid image: {file_path}")
            return

        # Replace with dynamic event info if needed
        event_id = "1089"
        event_user_id = "j8NCXEn4MSXSqwBIJPrdPFvEfjY2"

        raw_id, uploaded_file_path = await BotoB2.upload_ftp_uploaded_image_to_event_bucket(
            content=binary_data,
            content_type=mime_type,
            file_name=filename,
            event_id=event_id,
            event_user_id=event_user_id
        )

        if raw_id and uploaded_file_path:
            await FotoowlInternalApis.send_uploded_image_info_to_event_picture_process(
                event_id=event_id,
                image_name=filename,
                mime_type=mime_type,
                b2_id=raw_id,
                path=filename,
                user_id=event_user_id,
                height=img_height,
                width=img_width
            )
        print(f"[process_file] Done: {file_path}")

    except Exception as e:
        print(f"[process_file] Error: {e}")
    
    finally:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}", exc_info=True)

# ──────────────────────────────────────────────────────────────────────────────
# Worker loop
async def worker(name):
    while True:
        file_path = await queue.get()
        if is_file_ready(file_path):
            await process_file(file_path)
        queue.task_done()

# ──────────────────────────────────────────────────────────────────────────────
# Watchdog Handler
class FileCreatedHandler(FileSystemEventHandler):
    def __init__(self, loop):
        super().__init__()
        self.loop = loop

    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith((".jpg", ".jpeg", ".png")):
            print(f"[watchdog] New file: {event.src_path}")
            asyncio.run_coroutine_threadsafe(queue.put(event.src_path), self.loop)

# ──────────────────────────────────────────────────────────────────────────────
# Main entrypoint
async def main():
    loop = asyncio.get_running_loop()

    # Start workers
    workers = [asyncio.create_task(worker(f"worker-{i}")) for i in range(MAX_CONCURRENT_TASKS)]

    # Process existing files at startup
    for filename in os.listdir(WATCH_FOLDER):
        file_path = os.path.join(WATCH_FOLDER, filename)
        if os.path.isfile(file_path) and file_path.lower().endswith((".jpg", ".jpeg", ".png")):
            print(f"[startup] Found existing file: {file_path}")
            await queue.put(file_path)

    # Start folder observer
    event_handler = FileCreatedHandler(loop)
    observer = Observer()
    observer.schedule(event_handler, WATCH_FOLDER, recursive=False)
    observer.start()
    print("[main] Watching folder...")

    try:
        await asyncio.Event().wait()  # Infinite sleep to keep app alive
    except KeyboardInterrupt:
        print("[main] Shutting down...")
    finally:
        observer.stop()
        observer.join()
        for w in workers:
            w.cancel()

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())