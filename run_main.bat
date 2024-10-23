@echo off

call .\venv\Scripts\activate

SET BACKUPS_PATH=%~dp0
SET FOLDER_PATH=%~dp0
SET START_FOLDER=your_start_folder_id

python main.py
pause