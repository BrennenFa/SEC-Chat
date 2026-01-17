import os
import boto3
import fireo
import requests
from cdp_backend.database import models as db_models
from google.cloud.firestore import Client
from google.auth.credentials import AnonymousCredentials
from gcsfs import GCSFileSystem
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# --- CONFIG ---
S3_BUCKET = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
CDP_PROJECT_ID = "cdp-charlotte-98a7c348"

s3_client = boto3.client('s3', region_name=AWS_REGION)

# connect to charlotte's CDP Firestore
fireo.connection(client=Client(
    project=CDP_PROJECT_ID,
    credentials=AnonymousCredentials()
))

# Connect to the google cloud
fs = GCSFileSystem(project=CDP_PROJECT_ID, token="anon")

def sync_with_uri_detection(uri, s3_key):
    """detects GCS vs external URIs, error tests both"""

    # GCS file - use existing fs
    if uri.startswith('gs://'):
        with fs.open(uri, 'rb') as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)
        
    # external url
    elif uri.startswith(('http://', 'https://')):
        response = requests.get(uri, stream=True, timeout=30)
        response.raise_for_status()
        with response.raw as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)
    else:
        raise ValueError(f"Unknown URI scheme: {uri}")



# --- SYNC WORKER FUNCTIONS ---
def sync_transcript(json):
    """Worker: Downloads from GCS and uploads to S3."""
    try:
        # Get the file reference
        file_ref = json.file_ref.get()
        gcs_uri = file_ref.uri 
        
        # filename clean
        file_name = gcs_uri.split("/")[-1]
        s3_key = f"charlotte/transcripts/json/{file_name}"
        
        
        # Stream from GCS to S3
        with fs.open(gcs_uri, 'rb') as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)
            
    except Exception as e:
        print(f"Error syncing {json.id}: {e}")

# caption files --- not needed currently
# def sync_session_captions(session_model):
#     """Sync session VTT caption files"""
#     try:
#         # Check if caption URI exists
#         if not hasattr(session_model, 'caption_uri') or not session_model.caption_uri:
#             return  # Skip if no caption URI

#         file_name = session_model.caption_uri.split("/")[-1].split("?")[0]  # Clean query params
#         s3_key = f"charlotte/sessions/captions/{file_name}"

#         sync_with_uri_detection(session_model.caption_uri, s3_key)
#     except Exception as e:
#         print(f"Error syncing caption for {session_model.id}: {e}")

def sync_event_agenda(event_model):
    """Sync event agenda PDFs"""
    try:
        # Check if agenda URI exists
        if not hasattr(event_model, 'agenda_uri') or not event_model.agenda_uri:
            return  # Skip if no agenda URI

        file_name = event_model.agenda_uri.split("/")[-1].split("?")[0]
        s3_key = f"charlotte/events/agendas/{file_name}"

        sync_with_uri_detection(event_model.agenda_uri, s3_key)
    except Exception as e:
        print(f"Error syncing agenda for {event_model.id}: {e}")

def sync_event_minutes(event_model):
    """Sync event minutes PDFs"""
    try:
        # Check if minutes URI exists
        if not hasattr(event_model, 'minutes_uri') or not event_model.minutes_uri:
            return 

        file_name = event_model.minutes_uri.split("/")[-1].split("?")[0]
        s3_key = f"charlotte/events/minutes/{file_name}"

        sync_with_uri_detection(event_model.minutes_uri, s3_key)
    except Exception as e:
        print(f"Error syncing minutes for {event_model.id}: {e}")

def sync_event_thumbnails(event_model):
    """Sync event thumbnail images (PNG/GIF)"""
    try:
        # Sync static thumbnail (PNG)
        if hasattr(event_model, 'static_thumbnail_ref') and event_model.static_thumbnail_ref:
            thumb_ref = event_model.static_thumbnail_ref.get()
            file_name = thumb_ref.uri.split("/")[-1]
            s3_key = f"charlotte/events/thumbnails/static/{file_name}"
            sync_with_uri_detection(thumb_ref.uri, s3_key)

        # Sync hover thumbnail (GIF)
        if hasattr(event_model, 'hover_thumbnail_ref') and event_model.hover_thumbnail_ref:
            thumb_ref = event_model.hover_thumbnail_ref.get()
            file_name = thumb_ref.uri.split("/")[-1]
            s3_key = f"charlotte/events/thumbnails/hover/{file_name}"
            sync_with_uri_detection(thumb_ref.uri, s3_key)
    except Exception as e:
        print(f"Error syncing thumbnails for {event_model.id}: {e}")

def sync_matter_files(matter_model):
    """Sync matter supporting documents"""
    try:
        if not hasattr(matter_model, 'uri') or not matter_model.uri:
            return

        file_name = matter_model.uri.split("/")[-1].split("?")[0]
        s3_key = f"charlotte/matters/files/{file_name}"

        sync_with_uri_detection(matter_model.uri, s3_key)
    except Exception as e:
        print(f"Error syncing matter file for {matter_model.id}: {e}")

def sync_person_pictures(person_model):
    """Sync council member photos"""
    try:
        # Check if picture ref exists
        if not hasattr(person_model, 'picture_ref') or not person_model.picture_ref:
            return

        pic_ref = person_model.picture_ref.get()
        file_name = pic_ref.uri.split("/")[-1]
        s3_key = f"charlotte/people/pictures/{file_name}"

        sync_with_uri_detection(pic_ref.uri, s3_key)
    except Exception as e:
        print(f"Error syncing picture for {person_model.id}: {e}")

def main():
    # Sync Transcript json
    print(" Syncing Transcripts...")
    transcripts = list(db_models.Transcript.collection.fetch())
    print(f"  Found {len(transcripts)} transcripts")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_transcript, transcripts),
            total=len(transcripts),
            desc="  Progress"
        ))

    # Sync Event Agendas
    print("\ Syncing Event Agendas...")
    events = list(db_models.Event.collection.fetch())
    print(f"  Found {len(events)} events")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_event_agenda, events),
            total=len(events),
            desc="  Progress"
        ))

    # Sync Event Minutes
    print("\n Syncing Event Minutes...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_event_minutes, events),
            total=len(events),
            desc="  Progress"
        ))

    # Sync Event Thumbnails
    print("\n Syncing Event Thumbnails...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_event_thumbnails, events),
            total=len(events),
            desc="  Progress"
        ))

    # Sync Matter Files
    print("\n Syncing Matter Files...")
    matters = list(db_models.MatterFile.collection.fetch())
    print(f"  Found {len(matters)} matter files")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_matter_files, matters),
            total=len(matters),
            desc="  Progress"
        ))

    # Sync People Pictures
    print("\n Syncing People Pictures...")
    people = list(db_models.Person.collection.fetch())
    print(f"  Found {len(people)} people")
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(tqdm(
            executor.map(sync_person_pictures, people),
            total=len(people),
            desc="  Progress"
        ))

    print()
    print("=" * 60)
    print("[✓] Complete CDP data sync finished!")
    print("=" * 60)

if __name__ == "__main__":
    main()