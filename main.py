import json
import logging
import os
import shutil
import time
from bson import json_util
from pymongo import MongoClient
import datetime
import schedule
from dateutil.relativedelta import relativedelta
from google_interaction import add_data_to_google_drive, build_tree, delete_days_from_google_drive, delete_weeks_from_google_drive, find_tree



PROJECT_PATH = os.getenv("FOLDER_PATH")
BACKUPS_FOLDER = os.getenv("BACKUPS_PATH")


logger1 = logging.getLogger("main")
logger1.setLevel(logging.INFO)


handler1 = logging.FileHandler(os.path.join(PROJECT_PATH, "main.log"), mode='a', encoding='utf-8')
formatter1 = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")


handler1.setFormatter(formatter1)

logger1.addHandler(handler1)

hierarchy = ["day", "week", "month"]

client = MongoClient(os.getenv("DB_URL"))

import functools
def catch_exceptions(cancel_on_failure=False):
    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                result = job_func(*args, **kwargs)
                logger1.info(f"{args}, {kwargs} done.")
                return result
            except:
                import traceback
                logger1.exception(f"Exception in  func: '{job_func.__name__}' : {traceback.format_exc()}")
                if cancel_on_failure:
                    return schedule.CancelJob
                
        return wrapper
    return catch_exceptions_decorator

def log_actions(func):
    """
    Log all function executions or errors
    """
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger1.info(f"Func '{func.__name__}' done.")
            return result
        except Exception as e:
            logger1.error(f"An error accured in func: '{func.__name__}': {e}")
            return None
    return wrapper

@log_actions
def delete_days_backups(db_config: dict) -> None:
    shutil.rmtree(os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", "day"))
    os.makedirs(os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", "day"))
    delete_days_from_google_drive(tree[db_config["db_name"]]["backups"]["day"]["id"])


@log_actions
def delete_weeks_backups(db_config: dict) -> None:
    directory = os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", "week")
    today = datetime.datetime.now()
    target_date = today - relativedelta(months=1) - relativedelta(days=(today.day-1))
    
    files = os.listdir(directory)
    for file in files:
        file_path = os.path.join(BACKUPS_FOLDER, directory, file)
        file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
        if file_creation_time < target_date:
            os.remove(file_path)
    delete_weeks_from_google_drive(tree[db_config["db_name"]]["backups"]["week"]["id"])


@catch_exceptions(cancel_on_failure=True)
def backup_data(db_config: dict, folder: str) -> None:
    """
    Save right backup in BACKUPS_PATH and send them to drive
    """
    if folder == "week":
        delete_days_backups(db_config)
    if folder == "month":
        current_date = datetime.date.today()
        if current_date.day == 1:
            delete_weeks_backups(db_config)
        else:
            return
    
    db = client.get_database(db_config["db_name"])

    for collection_name in db_config["collections"]:
        if collection_name not in db.list_collection_names():
            logging.error(f"In database '{db_config['db_name']}' not found collection '{collection_name}'")

        collection = db.get_collection(collection_name) 
        data = list(collection.find({}))

        if data == []:
            continue
        file_name = "backup_"+collection_name+"_"+datetime.datetime.now().strftime("%d-%m-%Y-%H")+".json"
        file_path = os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", folder, file_name)
        with open(file_path, 'w') as json_file: 
            json.dump(data, json_file, indent=2, default=json_util.default)
        add_data_to_google_drive(tree[db_config["db_name"]]["backups"][folder]["id"],file_path)


@log_actions
def main():
    global tree
    tree = {}
    schedule_list = {}

    database_names = client.list_database_names()
    with open(os.path.join(PROJECT_PATH,'config.json'), 'r') as file:

        data_file = json.load(file)
        for db_config in data_file:
            if db_config["db_name"] not in database_names:
                raise Exception(f"Database '{db_config['db_name']}' not found!")
            
            schedule_list[db_config["db_name"]] = schedule.Scheduler()
            schedule_list[db_config["db_name"]].every().hour.at(":00").do(backup_data, db_config, "day")
            schedule_list[db_config["db_name"]].every().day.at("00:00").do(backup_data, db_config, "week")
            schedule_list[db_config["db_name"]].every().day.at("00:00").do(backup_data, db_config, "month")

            if not os.path.exists(os.path.join(BACKUPS_FOLDER,db_config["db_name"])):
                
                db = client.get_database(db_config["db_name"])

                for collection_name in db_config["collections"]:
                    if collection_name not in db.list_collection_names():
                        raise Exception(f"In database '{db_config['db_name']}' not found collection '{collection_name}'")
                os.makedirs(os.path.join(BACKUPS_FOLDER,db_config["db_name"]))
                
                for directory_name in hierarchy:
                    if not os.path.exists(os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", directory_name)):
                        os.makedirs(os.path.join(BACKUPS_FOLDER, db_config["db_name"], "backups", directory_name))
                build_tree(db_config, tree)
        
        tree = find_tree(data_file)

        
    with open(os.path.join(PROJECT_PATH,"tree.json"), 'w') as json_file: 
            json.dump(tree, json_file, indent=2, default=json_util.default)

    
    print("Script started!")

    while True:
        for task in schedule_list.values():
            task.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
    

    
        

                
                
        