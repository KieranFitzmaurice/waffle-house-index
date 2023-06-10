from __future__ import print_function

import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import os

import sys

def upload_to_folder(filepath,folder_id):
    """Upload a file to the specified folder and prints file ID, folder ID
    Args: path to file, id of the folder
    Returns: id of the file uploaded

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    filename = os.path.split(filepath)[-1]

    if os.path.exists('token.json'):
        # If modifying these scopes, delete the file token.json.
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        raise ValueError('No tokens found; please generate a token.')

    # If credentials are no longer valid, prompt user to re-generate them
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise ValueError('Credentials are no longer valid; please generate a new, valid token.')

    try:
        # create drive api client
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        media = MediaFileUpload(filepath,
                                mimetype='application/gzip', resumable=True)
        # pylint: disable=maybe-no-member
        file = service.files().create(body=file_metadata, media_body=media,
                                      fields='id').execute()
        print(F'File ID: "{file.get("id")}".')
        return file.get('id')

    except HttpError as error:
        print(F'An error occurred: {error}')
        return None


folder_id = '1W5J8ezWFU-ZTC7Gdtmfdp_lPoJ3-8nCc'
filepath = sys.argv[1]
file_id = upload_to_folder(filepath,folder_id)

if file_id is not None:
    sys.exit(0)
else:
    sys.exit(1)
