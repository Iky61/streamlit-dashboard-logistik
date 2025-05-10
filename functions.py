# import library and settings
import pandas as pd
import numpy as np
import ssl
import pytz
import requests
from datetime import datetime, timedelta
import xmlrpc.client
from streamlit_gsheets import GSheetsConnection
import streamlit as st
import altair as alt
import warnings
warnings.filterwarnings('ignore')

# create class for data joiner
class SuportFunction:
    # tansform sekumpulan data kedalam tipe data string
    @staticmethod
    def join_to_text(data):
        data = np.unique(data)
        return ','.join(data)

    # transform sekumpulan data kedalam tipe data aray
    @staticmethod
    def join_to_array(data):
        data = np.unique(data)
        return list(data)
    
    # transform stardar digit for time data type
    @staticmethod
    def transform_time_digit(x):
        con = len(x)
        if con < 2:
            msg = '0' + x
        else:
            msg = x
        return msg
    
    # buat fungsi untuk transformasi data time
    @staticmethod
    def transform_actual_hours(point):
        try:
            if point == '0.0':
                msg = '00:00'
            else:
                hour = str(point).split('.')[0]
                hour = SuportFunction.transform_time_digit(hour)
                
                minute = '0.' + str(point).split('.')[-1]
                minute = float(minute)
                minute = str(int(np.ceil(minute * 60)))
                minute = SuportFunction.transform_time_digit(minute)
                
                msg = hour + ':' + minute
        except:
            msg = point
        return msg
    
    # create convertion local time
    @staticmethod
    def convert_to_local_time(utc_time_str, target_timezone='Asia/Makassar'):
        try:
            # Mengonversi string waktu UTC ke objek datetime
            utc_time = datetime.strptime(utc_time_str, "%Y-%m-%d %H:%M:%S")
            
            # Menentukan waktu sebagai UTC
            utc_zone = pytz.utc
            utc_time = utc_zone.localize(utc_time)
            
            # Mengonversi ke zona waktu lokal
            local_zone = pytz.timezone(target_timezone)
            local_time = utc_time.astimezone(local_zone)
            msg = local_time.strftime("%Y-%m-%d %H:%M:%S")
        except:
            msg = pd.NaT
        return msg
    
    # create function to transform Oprator columns
    @staticmethod
    def transform_last_data_from_list(x):
        try:
            msg = x[-1]
        except:
            msg = x
        return msg
    
    # create function to transform datetime
    @staticmethod
    def transform_datetime(x):
        try:
            msg = pd.to_datetime(x)
        except:
            msg = pd.NaT
        return msg
    
    # create function to split data per-n units in list
    @staticmethod
    def spliting_data(data):

        startIter = 0
        ids_list = data.split(';')

        msg = []
        for n in range(7, len(ids_list) + 1, 7):
            batch = ids_list[startIter:n]  # Mengambil 5 elemen dari list
            msg.append(';'.join(batch))
            startIter += 7
            
        # Menangani batch terakhir jika ada sisa elemen
        if startIter < len(ids_list):
            batch = ids_list[startIter:len(ids_list)]
            msg.append(';'.join(batch))
        return msg

# create class to GET data from APi
class GetDataApi:
    # Methods untuk menarik data dari GSheet
    @staticmethod
    def ConnectionGSheet(url, index_cols):
        connection = st.connection("gsheets", type=GSheetsConnection)
        data = connection.read(spreadsheet=url, usecols=index_cols)
        return data

    # Methods untuk menarik data dari Odoo Api
    @staticmethod
    def ApiOdoo(path, fields=None, batch_size=50000, ids=None):
        # Bypass SSL certificate verification
        context = ssl._create_unverified_context()

        # Correct the URL to include the proper protocol (http/https)
        url = "https://node3.solusienergiutama.com"
        db = "cvsa"
        username = 'dicky.gps105@gmail.com'
        password = "@Lifeislearning0210"

        # Attempting authentication with bypassed SSL verification
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url), context=context) 

        # get uid 
        uid = common.authenticate(db, username, password, {})

        # List to store all fetched data
        all_data = []  
        try:
            if uid:
                # Initialize XMLRPC connection
                models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', context=context)
                
                # If IDs are provided, fetch data only for those IDs
                if ids:
                    # Chunk IDs into manageable sizes if necessary
                    for i in range(0, len(ids), batch_size):
                        id_chunk = ids[i:i + batch_size]
                        
                        # Read methods for the specific IDs
                        if fields is None:
                            data = models.execute_kw(db, uid, password, path, 'read', [id_chunk])
                        else:
                            data = models.execute_kw(db, uid, password, path, 'read', [id_chunk], {'fields': fields})
                        
                        all_data.extend(data)  # Add fetched data to the list
                
                else:
                    # Fetch all data in batches if no IDs are provided
                    start = 0
                    while True:
                        # Search methods with limit and offset for batching
                        get_fields = models.execute_kw(db, uid, password, path, 'search', [[]], {'limit': batch_size, 'offset': start})

                        if not get_fields:  # Stop if no more data
                            break

                        # Read methods
                        if fields is None:
                            data = models.execute_kw(db, uid, password, path, 'read', [get_fields])
                        else:
                            data = models.execute_kw(db, uid, password, path, 'read', [get_fields], {'fields': fields})

                        all_data.extend(data)  # Add fetched data to the list

                        # Update start for the next batch
                        start += batch_size
                
                # Convert to DataFrame
                df = pd.DataFrame(all_data)
                return df

            else:
                # No UID, return empty DataFrame
                return pd.DataFrame(all_data)

        except Exception as e:
            print(f"Error: {e}")
            # Return whatever data has been collected so far
            return pd.DataFrame(all_data)
