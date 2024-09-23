import datetime
import logging
import os
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from dateutil.relativedelta import relativedelta


PROJECT_PATH = os.getenv("FOLDER_PATH")

logger2 = logging.getLogger("google")
logger2.setLevel(logging.INFO)

handler2 = logging.FileHandler(os.path.join(PROJECT_PATH, "google.log"), mode='a', encoding='utf-8')
formatter2 = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

handler2.setFormatter(formatter2)
logger2.addHandler(handler2)


start_folder = os.getenv("START_FOLDER")

def log_actions(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger2.error(f"An error accured in func: '{func.__name__}' : {e}")
            return None
    return wrapper

@log_actions
def start_core():
    """
    Creates a connection to GoogleDrive services using the 'credentials.json' file

    return: connection object
    """
    credentials_file = 'credentials.json'
    credentials = service_account.Credentials.from_service_account_file(
        os.path.join(PROJECT_PATH, credentials_file),
        scopes=['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=credentials)
    return service

@log_actions
def delete_days_from_google_drive(folder_id: str) -> None:
    service = start_core()
    response = service.files().list(q=f"'{folder_id}' in parents").execute()

    files = response.get('files', [])

    for file in files:
        file_id = file.get('id')
        service.files().delete(fileId=file_id).execute()



def parsing_from_name_to_datetime(name: str) -> datetime.datetime:
    file_name = name[:-5] # remove the .json at the end
    splited_data = re.split(r'[-_]', file_name)
    return datetime.datetime(int(splited_data[-2]),int(splited_data[-3]),int(splited_data[-4]))


@log_actions
def delete_weeks_from_google_drive(folder_id: str) -> None:
    service = start_core()
    response = service.files().list(q=f"'{folder_id}' in parents").execute()

    files = response.get('files', [])

    for file in files:
        file_id = file.get('id')
        created_time = parsing_from_name_to_datetime(file.get('name'))

        today = datetime.datetime.now()
        target_date = today - relativedelta(months=1) - relativedelta(days=(today.day-1))

        if created_time < target_date:
            service.files().delete(fileId=file_id).execute()

@log_actions
def add_data_to_google_drive(target_folder_id: str, file_path: str) -> None:
    service = start_core()
    file_metadata = {
    'name': os.path.basename(file_path),
    'parents': [target_folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='text/plain')

    file = service.files().create(body=file_metadata,media_body=media,fields='id').execute()


@log_actions 
def find_tree(config_data: dict, tree={}) -> dict:
    """
    Find all id-keys and build an 'tree.json'
    """

    service = start_core()
    allowed_dbs = [conf["db_name"] for conf in config_data]

    # ask google for content in root folder
    response = service.files().list(q=f"'{start_folder}' in parents").execute() 

    for db_folder in response.get('files', []): 
        
        if db_folder.get('name') not in allowed_dbs: 
            continue

        # get the folder id of certain db
        tree[db_folder.get('name')] = {"id":db_folder.get('id')} 
        
        # ask google for content in db folder
        response_backup = service.files().list(q=f"'{db_folder.get('id')}' in parents").execute() 
        
        for backup_folder in response_backup.get('files', []): 

            if backup_folder.get('name') != "backups": 
                continue
            
            tree[db_folder.get('name')][backup_folder.get('name')] = {"id":backup_folder.get('id')} 
            
            # ask google for content in backup folder
            response_time_segments = service.files().list(q=f"'{backup_folder.get('id')}' in parents").execute() 

            for segments_folder in response_time_segments.get('files', []): 

                if segments_folder.get('name') not in ["day", "week", "month"]: 
                    continue
                
                tree[db_folder.get('name')][backup_folder.get('name')][segments_folder.get('name')] = {"id":segments_folder.get('id')} 
            
    return tree

        

@log_actions
def build_tree(config: dict, tree={}) -> None:
    """
    Build right structure in start folder
    """
    service = start_core()
    response = service.files().list(q=f"'{start_folder}' in parents").execute()
    g_drive_folders = [name.get("name")  for name in response.get('files', [])]
    
    if config["db_name"] in g_drive_folders:
        return 

    folder_metadata = {
    'name': config["db_name"],
    'mimeType': 'application/vnd.google-apps.folder',
    'parents': [start_folder]
    }
    folder = service.files().create(body=folder_metadata,fields='id').execute()
    tree[config["db_name"]] = {"id":folder.get('id')}

    folder_metadata = {
    'name': "backups",
    'mimeType': 'application/vnd.google-apps.folder',
    'parents':[tree[config["db_name"]]["id"]]
    }
    folder = service.files().create(body=folder_metadata,fields='id').execute()
    tree[config["db_name"]]["backups"] = {"id":folder.get('id')}

    folder_metadata = {
    'name': "day",
    'mimeType': 'application/vnd.google-apps.folder',
    'parents':[tree[config["db_name"]]["backups"]["id"]]
    }
    folder = service.files().create(body=folder_metadata,fields='id').execute()
    tree[config["db_name"]]["backups"]["day"] = {"id":folder.get('id')}

    folder_metadata = {
    'name': "week",
    'mimeType': 'application/vnd.google-apps.folder',
    'parents':[tree[config["db_name"]]["backups"]["id"]]
    }
    folder = service.files().create(body=folder_metadata,fields='id').execute()
    tree[config["db_name"]]["backups"]["week"] = {"id":folder.get('id')}

    folder_metadata = {
    'name': "month",
    'mimeType': 'application/vnd.google-apps.folder',
    'parents':[tree[config["db_name"]]["backups"]["id"]]
    }
    folder = service.files().create(body=folder_metadata,fields='id').execute()
    tree[config["db_name"]]["backups"]["month"] = {"id":folder.get('id')}
