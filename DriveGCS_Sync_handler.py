import os
import io
import json
import time
from google.oauth2 import service_account
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask, request, jsonify
import logging
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - Update these values
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# GCS_KEY_FILE = 'gcs-service-account-key.json'
GCS_KEY_FILE = 'temp_service_account.json'
encoded_key = os.environ.get('GCS_KEY_B64')

if encoded_key:
    with open(GCS_KEY_FILE, 'wb') as f:
        f.write(base64.b64decode(encoded_key))
else:
    raise RuntimeError("❌ GCS_KEY_B64 not found in environment variables")

BUCKET_NAME = 'testbucket1233455'  # Update with your actual bucket name
GCS_BASE_PATH = 'cogninest.ai/1 ANG Data Aggregator'  # Update with your preferred path
SHARED_FOLDER_ID = '1-dFuPfkdYLlVS9VLgIehIGfh6ZcQLXF3'  # Update with your actual folder ID

app = Flask(__name__)

def authenticate_drive():
    """Authenticate with Google Drive using service account."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCS_KEY_FILE, scopes=DRIVE_SCOPES
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        logger.info("✅ Drive authentication successful!")
        return drive_service
    except Exception as e:
        logger.error(f"❌ Error authenticating with Drive: {e}")
        return None

def authenticate_gcs():
    """Authenticate with Google Cloud Storage."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCS_KEY_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = storage.Client(credentials=credentials)
        logger.info("✅ GCS authentication successful!")
        return client
    except Exception as e:
        logger.error(f"❌ Error authenticating with GCS: {e}")
        return None

def get_file_metadata(drive_service, file_id):
    """Get metadata for a file or folder."""
    try:
        return drive_service.files().get(
            fileId=file_id,
            fields='id, name, parents, mimeType, owners',
            supportsAllDrives=True
        ).execute()
    except HttpError as error:
        logger.error(f"❌ Error getting metadata for file {file_id}: {error}")
        return None

def get_drive_path(drive_service, file_id):
    """Get the full path of a file in Drive, including all parent folders."""
    path_parts = []
    current_id = file_id

    try:
        metadata = get_file_metadata(drive_service, current_id)
        if not metadata:
            return None
            
        path_parts.append(metadata['name'])
        parents = metadata.get('parents', [])
        
        if not parents:
            return metadata['name']
        
        current_id = parents[0]
        depth = 0
        max_depth = 20
        
        while current_id and depth < max_depth:
            parent_metadata = get_file_metadata(drive_service, current_id)
            if not parent_metadata:
                break
                
            path_parts.append(parent_metadata['name'])
            parents = parent_metadata.get('parents', [])
            
            if not parents:
                break
                
            current_id = parents[0]
            depth += 1
            
        path_parts.reverse()
        return '/'.join(path_parts)
        
    except Exception as e:
        logger.error(f"❌ Error building path: {e}")
        return None

def get_relative_path_from_shared_folder(drive_service, file_id, shared_folder_id):
    """Get the relative path from the shared folder root."""
    try:
        full_path = get_drive_path(drive_service, file_id)
        if not full_path:
            return None, None
        
        shared_folder_metadata = get_file_metadata(drive_service, shared_folder_id)
        if not shared_folder_metadata:
            return None, None
        
        shared_folder_name = shared_folder_metadata['name']
        path_parts = full_path.split('/')
        
        try:
            shared_folder_index = path_parts.index(shared_folder_name)
            relative_parts = path_parts[shared_folder_index + 1:]
            
            if not relative_parts:
                return None, ""
            
            file_name = relative_parts[-1]
            folder_path = '/'.join(relative_parts[:-1]) if len(relative_parts) > 1 else ""
            
            return file_name, folder_path
            
        except ValueError:
            logger.warning(f"⚠️ Shared folder '{shared_folder_name}' not found in path: {full_path}")
            file_name = path_parts[-1]
            folder_path = '/'.join(path_parts[:-1]) if len(path_parts) > 1 else ""
            return file_name, folder_path
            
    except Exception as e:
        logger.error(f"❌ Error getting relative path: {e}")
        return None, None

def download_drive_file(drive_service, file_id, mime_type):
    """Download a file from Google Drive."""
    try:
        if mime_type and mime_type.startswith('application/vnd.google-apps'):
            export_formats = {
                'application/vnd.google-apps.document': ('application/pdf', '.pdf'),
                'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
                'application/vnd.google-apps.presentation': ('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx'),
            }
            
            export_mime_type, extension = export_formats.get(mime_type, ('application/pdf', '.pdf'))
            
            request = drive_service.files().export_media(
                fileId=file_id, 
                mimeType=export_mime_type
            )
        else:
            request = drive_service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True
            )
            extension = ''
        
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            if status:
                logger.info(f"📥 Download progress: {int(status.progress() * 100)}%")
        
        file_content.seek(0)
        return file_content, extension
        
    except HttpError as error:
        logger.error(f"❌ Error downloading file {file_id}: {error}")
        return None, None

def upload_to_gcs(gcs_client, bucket_name, blob_name, file_content):
    """Upload a file to Google Cloud Storage."""
    try:
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        file_content.seek(0)
        blob.upload_from_file(file_content)
        
        logger.info(f"📤 Uploaded to GCS: {blob_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Error uploading to GCS: {e}")
        return False

def create_gcs_folder_structure(gcs_client, bucket_name, gcs_path):
    """Create folder structure in GCS."""
    try:
        if not gcs_path or gcs_path == "/":
            return True
            
        bucket = gcs_client.bucket(bucket_name)
        folder_blob = bucket.blob(gcs_path + "/")
        
        if not folder_blob.exists():
            folder_blob.upload_from_string("")
            logger.info(f"📁 Created GCS folder: {gcs_path}/")
        
        return True
    except Exception as e:
        logger.error(f"❌ Error creating GCS folder structure: {e}")
        return False

def validate_file_path(file_path):
    """Validate if the file is in a path we want to sync."""
    if not file_path:
        return True
    
    path_parts = file_path.split('/')
    for part in path_parts:
        if part in ["2025"]:  # Based on your original logic
            return True
    
    return True

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    """Handle webhook from Zapier when a new file is uploaded to Drive."""
    try:
        data = request.get_json()
        logger.info(f"🔔 Received webhook data: {data}")
        
        file_id = data.get('file_id') or data.get('id')
        file_name = data.get('file_name') or data.get('name')
        
        if not file_id:
            return jsonify({'error': 'File ID not provided'}), 400
        
        logger.info(f"🔄 Processing new file: {file_name} (ID: {file_id})")
        
        # Authenticate services
        drive_service = authenticate_drive()
        gcs_client = authenticate_gcs()
        
        if not drive_service or not gcs_client:
            return jsonify({'error': 'Authentication failed'}), 500
        
        # Get file metadata
        metadata = get_file_metadata(drive_service, file_id)
        if not metadata:
            return jsonify({'error': 'Could not retrieve file metadata'}), 404
        
        # Skip folders
        if metadata.get('mimeType') == 'application/vnd.google-apps.folder':
            logger.info(f"📁 Skipping folder: {metadata['name']}")
            return jsonify({
                'success': True,
                'message': 'Skipped folder - only processing files',
                'file_id': file_id
            }), 200
        
        # Get relative path from shared folder
        file_name, relative_folder_path = get_relative_path_from_shared_folder(
            drive_service, file_id, SHARED_FOLDER_ID
        )
        
        if not file_name:
            return jsonify({'error': 'Could not determine file path'}), 500
        
        logger.info(f"📄 File: {file_name}")
        logger.info(f"📂 Relative path: {relative_folder_path}")
        
        # Validate if we should process this file
        if not validate_file_path(relative_folder_path):
            logger.info(f"⏭️ Skipping file - not in target folder: {relative_folder_path}")
            return jsonify({
                'success': True,
                'message': f'Skipped file - not in target folder',
                'file_name': file_name,
                'path': relative_folder_path
            }), 200
        
        # Build GCS paths
        if relative_folder_path:
            gcs_folder_path = f"{GCS_BASE_PATH}/{relative_folder_path}"
        else:
            gcs_folder_path = GCS_BASE_PATH
        
        # Create folder structure in GCS
        if relative_folder_path:
            create_gcs_folder_structure(gcs_client, BUCKET_NAME, gcs_folder_path)
        
        # Download file from Drive
        logger.info(f"📥 Downloading file from Drive...")
        file_content, extension = download_drive_file(
            drive_service, 
            file_id, 
            metadata.get('mimeType')
        )
        
        if not file_content:
            return jsonify({'error': 'Could not download file from Drive'}), 500
        
        # Determine final filename for GCS
        if extension and not file_name.endswith(extension):
            gcs_filename = file_name + extension
        else:
            gcs_filename = file_name
        
        # Calculate the full GCS path
        gcs_object_name = f"{gcs_folder_path}/{gcs_filename}"
        
        # Upload to GCS
        logger.info(f"📤 Uploading to GCS: {gcs_object_name}")
        upload_success = upload_to_gcs(gcs_client, BUCKET_NAME, gcs_object_name, file_content)
        
        if upload_success:
            response_data = {
                'success': True,
                'message': f'Successfully replicated: {file_name} to {gcs_object_name}',
                'file_name': file_name,
                'original_name': metadata['name'],
                'drive_path': relative_folder_path,
                'gcs_path': gcs_object_name,
                'file_id': file_id,
                'mime_type': metadata.get('mimeType')
            }
            logger.info(f"✅ Successfully replicated: {file_name} to {gcs_object_name}")
            
            # Small delay to avoid rate limiting
            time.sleep(0.5)
            
            return jsonify(response_data), 200
        else:
            return jsonify({'error': f'Failed to replicate: {file_name}'}), 500
            
    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/test', methods=['GET'])
def test_endpoint():
    """Test endpoint to verify the service is running."""
    try:
        logger.info("🧪 Running health check...")
        
        # Test authentication
        drive_service = authenticate_drive()
        gcs_client = authenticate_gcs()
        
        if not drive_service or not gcs_client:
            return jsonify({'error': 'Authentication failed'}), 500
        
        # Test shared folder access
        shared_folder_metadata = get_file_metadata(drive_service, SHARED_FOLDER_ID)
        
        # Get service account email
        with open(GCS_KEY_FILE, 'r') as f:
            key_data = json.load(f)
            service_email = key_data['client_email']
        
        result = {
            'status': 'Service is running locally! 🚀',
            'bucket': BUCKET_NAME,
            'gcs_base_path': GCS_BASE_PATH,
            'shared_folder_id': SHARED_FOLDER_ID,
            'shared_folder_name': shared_folder_metadata['name'] if shared_folder_metadata else 'Not accessible',
            'shared_folder_access': shared_folder_metadata is not None,
            'service_account_email': service_email,
            'environment': 'localhost'
        }
        
        logger.info("✅ Health check passed!")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/test-file/<file_id>', methods=['GET'])
def test_file_processing(file_id):
    """Test processing a specific file ID to debug path resolution."""
    try:
        logger.info(f"🧪 Testing file processing for ID: {file_id}")
        
        drive_service = authenticate_drive()
        if not drive_service:
            return jsonify({'error': 'Drive authentication failed'}), 500
        
        metadata = get_file_metadata(drive_service, file_id)
        if not metadata:
            return jsonify({'error': 'File not found or not accessible'}), 404
        
        full_path = get_drive_path(drive_service, file_id)
        file_name, relative_path = get_relative_path_from_shared_folder(
            drive_service, file_id, SHARED_FOLDER_ID
        )
        
        should_process = validate_file_path(relative_path)
        
        if relative_path:
            gcs_path = f"{GCS_BASE_PATH}/{relative_path}/{file_name}"
        else:
            gcs_path = f"{GCS_BASE_PATH}/{file_name}"
        
        result = {
            'file_id': file_id,
            'file_name': file_name,
            'full_drive_path': full_path,
            'relative_path': relative_path,
            'should_process': should_process,
            'gcs_path': gcs_path,
            'metadata': {
                'name': metadata['name'],
                'mimeType': metadata['mimeType'],
                'parents': metadata.get('parents', [])
            }
        }
        
        logger.info(f"✅ File test completed for: {file_name}")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ File test failed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/list-shared-folder', methods=['GET'])
def list_shared_folder_contents():
    """List contents of the shared folder for debugging."""
    try:
        logger.info("📋 Listing shared folder contents...")
        
        drive_service = authenticate_drive()
        if not drive_service:
            return jsonify({'error': 'Drive authentication failed'}), 500
        
        query = f"'{SHARED_FOLDER_ID}' in parents and trashed = false"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType, parents)",
            spaces='drive',
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        items = results.get('files', [])
        
        logger.info(f"📂 Found {len(items)} items in shared folder")
        
        return jsonify({
            'shared_folder_id': SHARED_FOLDER_ID,
            'items_count': len(items),
            'items': items
        })
        
    except Exception as e:
        logger.error(f"❌ Error listing folder: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    """Root endpoint with helpful information."""
    return jsonify({
        'service': 'Drive to GCS Sync Webhook Handler',
        'status': 'Running locally 🏠',
        'endpoints': {
            '/test': 'GET - Health check',
            '/webhook': 'POST - Main webhook endpoint',
            '/test-file/<file_id>': 'GET - Test file processing',
            '/list-shared-folder': 'GET - List shared folder contents'
        },
        'tip': 'Use ngrok to expose this to the internet for Zapier integration'
    })

if __name__ == '__main__':
    print("🚀 Starting Drive to GCS Sync Webhook Handler...")
    print("📋 Configuration:")
    print(f"   Bucket: {BUCKET_NAME}")
    print(f"   Base Path: {GCS_BASE_PATH}")
    print(f"   Shared Folder: {SHARED_FOLDER_ID}")
    print("🌐 Server starting on http://localhost:5000")
    print("🧪 Test endpoint: http://localhost:5000/test")
    print("🔗 Webhook endpoint: http://localhost:5000/webhook")
    print("")
    print("💡 To expose to internet for Zapier:")
    print("   1. Install ngrok: https://ngrok.com/download")
    print("   2. Run: ngrok http 5000")
    print("   3. Use the ngrok URL in Zapier")
    print("")
    
    app.run(debug=True, host='0.0.0.0', port=5000)