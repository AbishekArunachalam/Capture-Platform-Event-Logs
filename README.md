# Capture-Platform-Event-Logs
The script establishes connection and ingests platform event logs in a JSON file to a MySQL database

## Overview

I have set up a MySQL server instance on the AWS cloud. The aim is to inject the platform event logs to the database on a daily basis using AWS Lambda service and CRON expression. 

The connection details of the server are passed through a config.ini file. For security purposes, the file is not added to the GIT repository.

## Config.ini File Pattern

```
[mysqldb]
hostname = ""
user_name = Abishek
password = ""
db_name = db_name
table_name = platform_events
port = 3306
```

## Getting Started

**Create a virtual environment for the project:**


```
python3 -m venv env_name

source env_name/bin/activate

deactivate - to exit virtual environment  
```

**Install package dependencies in the virtual environment:**
```
pip install -r requirements.txt
```

## Running Script

**Execute the script from project root directory:**
```
python3 src/record_events.py
```

