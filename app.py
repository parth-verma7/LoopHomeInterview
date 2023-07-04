from flask import Flask, render_template, redirect, url_for, request, session
import pandas as pd
import numpy as np
import pymongo
from pymongo import MongoClient
import sqlite3
import mysql.connector
import pytz
import random
import datetime
import string

app = Flask(__name__)

def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

@app.route('/')
def hello():
    return "Take home interview - Store Monitoring System"

@app.route('/trigger_report')
def trigger_report():
    df_store_status=pd.read_csv('store_status.csv')
    df_timezoned=pd.read_csv('timezoned.csv')
    df_Menu_hours=pd.read_csv('Menu_hours.csv')
    nm=generate_random_string(1)
    conn = sqlite3.connect(f'user7.db')

    df_store_status.to_sql('df_store_status', conn, if_exists='replace', index=False)
    df_timezoned.to_sql('df_timezoned', conn, if_exists='replace', index=False)
    df_Menu_hours.to_sql('df_Menu_hours', conn, if_exists='replace', index=False)

    time_diff=[]
    for i in range(len(df_timezoned)):
        dt_dynamic=datetime.datetime.now(pytz.timezone(df_timezoned['timezone_str'][i]))
        time_diff.append(int(dt_dynamic.strftime('%z')[:3]))
    df_timezoned['time_diff']=time_diff
    print("The df_timezone is ",df_timezoned['time_diff'].head())
    query = ''' 
        SELECT df_store_status.store_id, df_store_status.status, df_store_status.timestamp_utc, COALESCE(df_timezoned.timezone_str, 'America/Chicago') AS timezone_str, 
        substr(df_store_status.timestamp_utc,1,10) as Date,
        CASE (strftime('%w', substr(df_store_status.timestamp_utc,1,10))-1)%7 
                When 0 THEN 'Monday'
                When 1 THEN 'Tuesday'
                When 2 THEN 'Wednesday'
                When 3 THEN 'Thursday'
                When 4 THEN 'Friday'
                When 5 THEN 'Saturday'
                When -1 THEN 'Sunday'
                ELSE 'Unknown'
        END AS Day
        FROM df_store_status
        LEFT JOIN df_timezoned
        ON df_store_status.store_id = df_timezoned.store_id
    ''' 
    df_id = pd.read_sql_query(query, conn)
    print("The df_id is ",df_id.head())

    timestamp_timezone=[]
    rows_to_drop = []
    i=1
    for i in range(len(df_id)):
        try:
            given_datetime = datetime.datetime.strptime(df_id['timestamp_utc'][i], "%Y-%m-%d %H:%M:%S.%f %Z")
            dt_dynamic=datetime.datetime.now(pytz.timezone(df_id['timezone_str'][i]))
            time_difference=int(dt_dynamic.strftime('%z')[:3])
            time_difference=datetime.timedelta(hours=time_difference)
            result_datetime = given_datetime + time_difference
            result_timestamp = result_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            timestamp_timezone.append(result_timestamp)
        except:
            rows_to_drop.append(i)
            print(i)
            i+=1

    df_id = df_id.drop(rows_to_drop)
    df_id['timestamp_timezone']=timestamp_timezone

    day_map={
        'Monday':0,
        'Tuesday':1,
        'Wednesday':2,
        'Thursday':3,
        'Friday':4,
        'Saturday':5,
        'Sunday':6
    }

    reverse_day_map = {v: k for k, v in day_map.items()}
    print(reverse_day_map)

    df_Menu_hours=pd.read_csv('Menu_hours.csv')
    df_Menu_hours['DayOfWeek']=df_Menu_hours['day'].map(reverse_day_map)

    df_Menu_hours.to_sql('df_Menu_hours', conn, if_exists='replace', index=False)

    df_id.to_sql('df_id', conn, if_exists='replace', index=False)

    query = ''' 
        SELECT df_id.store_id, df_id.day, df_id.status, df_id.timestamp_utc, df_id.timezone_str, df_id.Date, df_id.timestamp_timezone,
        df_Menu_hours.start_time_local, df_Menu_hours.end_time_local, df_Menu_hours.DayOfWeek
        FROM df_id
        LEFT JOIN df_Menu_hours
        ON df_id.store_id = df_Menu_hours.store_id
        WHERE df_Menu_hours.DayOfWeek = df_id.day
        AND df_id.status='active'
        AND time(df_id.timestamp_timezone) >= time(df_Menu_hours.start_time_local)
        AND time(df_id.timestamp_timezone) <= time(df_Menu_hours.end_time_local)
    ''' 
    df_filter = pd.read_sql_query(query, conn)
    print("the df_filter is ",df_filter.head())
    df_filter['timestamp_timezone']=pd.to_datetime(df_filter['timestamp_timezone'])
    df_filter['minute']=df_filter['timestamp_timezone'].dt.minute

    df_filter['minute']=df_filter['minute'].apply(lambda x: 60-x)

    df_filter.rename(columns={'minute': 'uptime_last_hour'}, inplace=True)
    df_filter['downtime_last_hour']=60-df_filter['uptime_last_hour']
    df_filter=df_filter.drop(['Day'],axis=1)
    df_filter.to_sql('df_filter', conn, if_exists='replace', index=False)
    query = ''' 
        SELECT df_filter.store_id, df_filter.DayOfWeek,df_filter.Date,df_filter.timestamp_timezone , 
        SUM(df_filter.uptime_last_hour) AS uptime_last_day,
        strftime('%H:%M:%S', df_filter.start_time_local) AS start_time_local,
        strftime('%H:%M:%S', df_filter.end_time_local) AS end_time_local,
        (julianday(df_filter.end_time_local) - julianday(df_filter.start_time_local)) * 1440 AS total_time
        FROM df_filter
        GROUP BY df_filter.store_id, df_filter.DayOfWeek, df_filter.Date
    ''' 
    df_filter2 = pd.read_sql_query(query, conn)
    df_filter2['downtime_last_day']=df_filter2['total_time']-df_filter2['uptime_last_day']
    print("The df_filter2 is ",df_filter2.head())
    df_filter2.to_sql('df_filter2', conn, if_exists='replace', index=False)
    query='''
        SELECT store_id, SUM(uptime_last_day) AS uptime_last_week, 
        CAST(ROUND(SUM(downtime_last_day),0)AS INTEGER) AS downtime_last_week,
        CAST(ROUND(SUM(total_time),0) AS INTEGER) AS total_time_last_week
        FROM df_filter2
        GROUP BY store_id
    '''
    df_filter3 = pd.read_sql_query(query, conn)
    print("The df_filter3 is ",df_filter3.head())
    df_filter3.to_sql('df_filter3', conn, if_exists='replace', index=False)
    query='''
        SELECT df_filter2.store_id,df_filter2.DayOfWeek, df_filter2.timestamp_timezone , df_filter2.uptime_last_day, df_filter2.downtime_last_day, df_filter3.uptime_last_week, df_filter3.downtime_last_week
        FROM df_filter2
        LEFT JOIN df_filter3
        ON df_filter2.store_id = df_filter3.store_id 
    '''
    df_filter4 = pd.read_sql_query(query, conn)

    df_filter4.to_sql('df_filter4', conn, if_exists='replace', index=False)
    print("The df_filter4 is ",df_filter4.head())

    query='''
        SELECT df_filter.store_id,df_filter.DayOfWeek, df_filter.timestamp_timezone, df_filter.uptime_last_hour, df_filter.downtime_last_hour, ROUND((df_filter4.uptime_last_day)/60,2) AS uptime_last_day, ROUND((df_filter4.downtime_last_day)/60,2) AS downtime_last_day, ROUND((df_filter4.uptime_last_week)/60,2) AS uptime_last_week, ROUND((df_filter4.downtime_last_week)/60,2) AS downtime_last_week
        FROM df_filter
        LEFT JOIN df_filter4
        ON df_filter.store_id = df_filter4.store_id 
    '''
    df_filter5 = pd.read_sql_query(query, conn)
    df_filter5.head(7)
    print("The df_filter5 is",df_filter5.head(7))

    df_filter5['downtime_last_day']=abs(df_filter5['downtime_last_day'])
    df_filter5['downtime_last_week']=abs(df_filter5['downtime_last_week'])
    
    df_filter5.to_csv('df_filter5.csv', index=False)
    random_string = generate_random_string(10)
    print("The random string is ",random_string)
    df_filter5.to_csv(f'{random_string}.csv', index=False)
    return f"The random string that is generated is {random_string}"
    

@app.route('/get_report_endpoint',methods=['GET'])
def get_report_endpoint():
    textt = request.args.get('text')
    return "Your .csv file has been saved"

if __name__ == '__main__':
    app.run(debug=True)