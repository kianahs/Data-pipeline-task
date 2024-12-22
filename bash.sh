#!/bin/bash

CONFIG_FILE="config.json"
# extracting information needed for connecting to the sql server from the config file
HOST=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['host'])")
USER=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['user'])")
PASSWORD=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['password'])")
PORT=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['port'])")
DATABASE=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['database_name'])")
SQL_SOURCE=$(python -c "import json; print(json.load(open('$CONFIG_FILE'))['sql_source'])")

# connect to the mysql server and source .sql code
echo "Connecting attempt to SQL server $HOST on port $PORT..."
mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASSWORD" "$DATABASE" < "$SQL_SOURCE"

# run the python script if the connection is successfull
if [ $? -eq 0 ]; then
    echo "Connected to Database!, Running Python script ..."
    python connect_database.py
    # jupyter nbconvert --to python --execute connect_database.ipynb
else
    echo "Failed to connect to MySQL server"
    exit 1
fi
