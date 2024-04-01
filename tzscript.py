#!/usr/bin/env python
# coding: utf-8

# In[151]:


import requests
import logging
from datetime import datetime
import mysql.connector

mysql_db = 'timezones'
base_url = 'http://api.timezonedb.com/v2.1/'


# In[142]:


# Set up logging
logging.basicConfig(filename='error.log', level=logging.ERROR)


# In[143]:


def mysql_connection():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="timezones"
    )

    return conn


# In[144]:


def db_exec(query, params=None):
    conn = mysql_connection()
    cursor = conn.cursor()

    cursor.execute(query, params)

    conn.commit()
    conn.close()


# In[145]:


# Function to create tables if they don't exist
def create_tables():

    # Create TZDB_TIMEZONES table
    db_exec('''CREATE TABLE IF NOT EXISTS TZDB_TIMEZONES (
                    COUNTRYCODE VARCHAR(10),
                    COUNTRYNAME VARCHAR(100),
                    ZONENAME VARCHAR(100) PRIMARY KEY,
                    GMTOFFSET INTEGER,
                    IMPORT_DATE DATE
                )''')

    # Create TZDB_ZONE_DETAILS table
    db_exec('''CREATE TABLE IF NOT EXISTS TZDB_ZONE_DETAILS (
                    COUNTRYCODE VARCHAR(10),
                    COUNTRYNAME VARCHAR(100),
                    ZONENAME VARCHAR(100),
                    GMTOFFSET INTEGER,
                    DST INTEGER,
                    ZONESTART INTEGER,
                    ZONEEND INTEGER,
                    PRIMARY KEY (ZONENAME, ZONESTART, ZONEEND),
                    IMPORT_DATE DATE
                )''')

    # Create TZDB_ERROR_LOG table
    db_exec('''CREATE TABLE IF NOT EXISTS TZDB_ERROR_LOG (
                    error_id INT AUTO_INCREMENT PRIMARY KEY,
                    error_message TEXT,
                    timestamp DATETIME
                )''')


# In[165]:


# Function to log errors into TZDB_ERROR_LOG table
def log_error(error_message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    error_data = (error_message, timestamp)

    db_exec('''INSERT INTO TZDB_ERROR_LOG (error_message, timestamp)
                VALUES (%s, %s)''', error_data)


# In[211]:


def insert_tz_details(zone_name):
    zones_url = "{base_url}/get-time-zone?key={api_key}&format=json&by=zone&zone={zone_name}".format(
        base_url=base_url, api_key=api_key, zone_name=zone_name)
    response = requests.get(zones_url)

    return response.json()


# In[214]:


# Function to query TimezoneDB API and populate tables
def populate_tables(api_key):
    zones_url = base_url + 'list-time-zone'

    try:
        # Query API to get list of timezones
        params = {'key': api_key, 'format': 'json'}
        response = requests.get(zones_url, params=params)
        data = response.json()

        # Delete existing data in TZDB_TIMEZONES table
        db_exec('''DELETE FROM TZDB_TIMEZONES''')

        for zone in data['zones']:
            # Insert into TZDB_TIMEZONES table
            tz_data = (
                zone['countryCode'],
                zone['countryName'],
                zone['zoneName'],
                zone['gmtOffset'],
                zone['timestamp']
            )

            # Populate TZDB_TIMEZONES
            db_exec('''INSERT INTO tzdb_timezones
                        (COUNTRYCODE, COUNTRYNAME, ZONENAME, GMTOFFSET, IMPORT_DATE)
                        VALUES (%s, %s, %s, %s, %s)''', tz_data)

            conn = mysql_connection()
            cursor = conn.cursor()
            tz_details_in_record = cursor.execute('''
                SELECT * FROM tzdb_zone_details WHERE ZONENAME = %s
            ''', [zone['zoneName']])

            tz_record = cursor.fetchall()

            if len(tz_record) == 0:
                tz_details = insert_tz_details(zone['zoneName'])
                if (tz_details["status"] == 'OK'):
                    tz_details_data = (
                        zone['countryCode'],
                        zone['countryName'],
                        zone['zoneName'],
                        zone['gmtOffset'],
                        tz_details["dst"],
                        tz_details["zoneStart"],
                        0 if tz_details["zoneEnd"] is None else tz_details["zoneEnd"],
                        tz_details["formatted"]
                    )

                # Populate TZDB_ZONE_DETAILS tables
                db_exec('''INSERT INTO TZDB_ZONE_DETAILS
                        (COUNTRYCODE, COUNTRYNAME, ZONENAME, GMTOFFSET, DST, ZONESTART, ZONEEND, IMPORT_DATE)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', tz_details_data)
    except Exception as e:
        error_message = f"Error retrieving data from API: {str(e)}"
        log_error(error_message)
        print(e)


if __name__ == "__main__":
    api_key = 'PY34NL871CBX'

    # Create tables if they don't exist
    create_tables()

    # Populate tables with data from TimezoneDB API
    populate_tables(api_key)


# In[ ]:


# In[ ]:
