import sqlite3
import json
import pandas as pd


class DbHandler:

    def __init__(self):
        self.conn = None

    def init_db(self):
        self.conn = sqlite3.connect('iplocation.db')
        cursor = self.conn.cursor()

        # Create a table
        cursor.execute('''CREATE TABLE IF NOT EXISTS ip_geolocation (
                            id INTEGER PRIMARY KEY,
                            ip TEXT,
                            continent_code TEXT,
                            continent_name TEXT,
                            country_code2 TEXT,
                            country_code3 TEXT,
                            country_name TEXT,
                            country_name_official TEXT,
                            country_capital TEXT,
                            state_prov TEXT,
                            state_code TEXT,
                            district TEXT,
                            city TEXT,
                            zipcode TEXT,
                            latitude TEXT,
                            longitude TEXT,
                            is_eu INTEGER,
                            calling_code TEXT,
                            country_tld TEXT,
                            languages TEXT,
                            country_flag TEXT,
                            geoname_id TEXT,
                            isp TEXT,
                            connection_type TEXT,
                            organization TEXT,
                            currency_code TEXT,
                            currency_name TEXT,
                            currency_symbol TEXT,
                            time_zone_name TEXT,
                            time_zone_offset INTEGER,
                            time_zone_offset_with_dst INTEGER,
                            time_zone_current_time TEXT,
                            time_zone_current_time_unix REAL,
                            time_zone_is_dst INTEGER,
                            time_zone_dst_savings INTEGER
                        )''')
        self.conn.commit()

    # Function to insert JSON data into SQLite database
    def insert_geolocation_data(self, data):
        cursor = self.conn.cursor()

        # Connect to the SQLite database

        # Insert data into the table
        cursor.execute('''INSERT INTO ip_geolocation (
                            ip, continent_code, continent_name, country_code2, country_code3,
                            country_name, country_name_official, country_capital, state_prov,
                            state_code, district, city, zipcode, latitude, longitude,
                            is_eu, calling_code, country_tld, languages, country_flag,
                            geoname_id, isp, connection_type, organization, currency_code,
                            currency_name, currency_symbol, time_zone_name, time_zone_offset,
                            time_zone_offset_with_dst, time_zone_current_time,
                            time_zone_current_time_unix, time_zone_is_dst,
                            time_zone_dst_savings
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (data['ip'], data['continent_code'], data['continent_name'], data['country_code2'],
                        data['country_code3'],
                        data['country_name'], data['country_name_official'], data['country_capital'],
                        data['state_prov'],
                        data['state_code'], data['district'], data['city'], data['zipcode'], data['latitude'],
                        data['longitude'],
                        data['is_eu'], data['calling_code'], data['country_tld'], data['languages'],
                        data['country_flag'],
                        data['geoname_id'], data['isp'], data['connection_type'], data['organization'],
                        data['currency']['code'], data['currency']['name'], data['currency']['symbol'],
                        data['time_zone']['name'], data['time_zone']['offset'], data['time_zone']['offset_with_dst'],
                        data['time_zone']['current_time'], data['time_zone']['current_time_unix'],
                        data['time_zone']['is_dst'], data['time_zone']['dst_savings']))

        # Commit changes and close the connection
        self.conn.commit()

    # Function to retrieve a row by IP from the SQLite database
    def retrieve_geolocation_by_ip(self, ip_address):
        cursor = self.conn.cursor()

        # Retrieve row by IP address
        cursor.execute("SELECT * FROM ip_geolocation WHERE ip=?", (ip_address,))  # Get column names
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        if not row:
            return None

        # Create a dictionary using column names and fetched row values
        result = dict(zip(columns, row))

        # Convert the dictionary to JSON
        return json.dumps(result)  # Return the fetched row (as a tuple)

    # Function to retrieve a row by IP from the SQLite database
    def retrieve_all_geolocations(self):
        cursor = self.conn.cursor()

        # Retrieve row by IP address
        cursor.execute("SELECT * FROM ip_geolocation")  # Get column names
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        if not rows:
            return None

        df = pd.DataFrame(rows, columns=columns)

        return df

    def close_db(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
