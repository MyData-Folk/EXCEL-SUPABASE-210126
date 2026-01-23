import urllib.request
import urllib.parse
import json
import os
import mimetypes
import uuid

# Configuration
BASE_URL = "http://localhost:5000"
TEST_FILE = "test_import.xlsx"

def create_dummy_excel():
    try:
        import pandas as pd
        df = pd.DataFrame({
            'Date': ['2023-01-01', '2023-01-02'],
            'Revenue': [1000, 2000],
            'Hotel': ['H1', 'H1']
        })
        df.to_excel(TEST_FILE, index=False)
        print(f"Created dummy file {TEST_FILE}")
    except ImportError:
        print("Pandas not found, creating a dummy text file instead for upload test (might fail validation if strict)")
        with open(TEST_FILE, "w") as f:
            f.write("dummy content")

def post_json(endpoint, data):
    url = f"{BASE_URL}{endpoint}"
    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.read().decode()}")
        return e.code, None
    except Exception as e:
        print(f"Error: {e}")
        return 0, None

def upload_file():
    url = f"{BASE_URL}/api/upload"
    boundary = uuid.uuid4().hex
    params = {}
    
    with open(TEST_FILE, 'rb') as f:
        file_content = f.read()

    # Build multipart/form-data
    data = []
    data.append(f'--{boundary}')
    data.append(f'Content-Disposition: form-data; name="file"; filename="{TEST_FILE}"')
    data.append('Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    data.append('')
    data.append(file_content) # This needs to be bytes, so mixing str/bytes is tricky
    data.append(f'--{boundary}--')
    data.append('')
    
    # Proper bytes construction
    body = b''
    body += f'--{boundary}\r\n'.encode()
    body += f'Content-Disposition: form-data; name="file"; filename="{TEST_FILE}"\r\n'.encode()
    body += b'Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n'
    body += file_content
    body += f'\r\n--{boundary}--\r\n'.encode()

    req = urllib.request.Request(
        url,
        data=body,
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
         print(f"Upload Failed: {e.code} {e.read().decode()}")
         return e.code, None
    except Exception as e:
         print(f"Upload Failed: {e}")
         return 0, None

def steps():
    print(f"Targeting {BASE_URL}")
    
    # 1. Upload
    print("\n--- Testing Upload ---")
    status, data = upload_file()
    print(f"Status: {status}")
    print(f"Data: {data}")
    
    if status != 200 or not data:
        print("Stopping due to upload failure")
        return

    # Use the CORRECT filepath (UUID)
    filename = data['filepath']
    print(f"Using filepath (UUID): {filename}")
    
    # 2. Preview
    print("\n--- Testing Preview ---")
    status, p_data = post_json('/api/preview', {'filename': filename})
    print(f"Status: {status}")
    if status == 200:
        print("Preview OK")
    
    # 3. Auto Process
    print("\n--- Testing Auto Process ---")
    payload = {
        'filename': filename,
        'category': 'RAPPORT RÉSERVATIONS EN COURS D-EDGE', # Example category
        'hotel_id': 'TEST_HOTEL'
    }
    status, a_data = post_json('/api/auto-process', payload)
    print(f"Status: {status}")
    print(f"Response: {a_data}")

if __name__ == "__main__":
    if not os.path.exists(TEST_FILE):
        create_dummy_excel()
    steps()
