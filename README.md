
# Description

A simple script to create backups on Google Drive.

The script creates backups every hour, then every 24 hours a daily one is made (hourly ones are deleted), then every month (daily ones are deleted), monthly ones are stored forever. 



## Installation

1) GoogleDrive setup:
- On the GoogleCloud Console site, you need to create a new project, add the GoogleDrive API for it, and create a service account.

- Once you've created it, don't forget to create and save credentials. 
    
    Rename file as ```'credentials.json'``` and put in root of project.
- When you have already created a service account, you can go to Google Drive where you will store backups (your personal or corporate), create a folder with the name you want and share access with the created service account.

2) Script config:


2.1 First of all you need venv and libraries:
```bash
  python -m venv venv
  venv\Scripts\activate
  pip install -r requirements.txt
```

2.2 After this you need to find your 'root' folder ```id``` in GoogleDrive:

All you need to do is go to your google drive, select the folder you want and copy the id from the address bar.

Example: 

    https://drive.google.com/drive/u/0/folders/Your_folder_id


2.3 Then you need to make 'config.json' file, exaple:
```bash
[
  {
      "db_name": "your_db_name_1",
      "collections": ["your_collection_name"]
  },
  {
      "db_name": "your_db_name_2",
      "collections": ["your_collection_name"]
  }
]
```

Finally, you can configure your ```'run_main.sh'``` and start the project.
