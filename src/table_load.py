#!/usr/bin/env python3

import pymysql
import json
import configparser
import pandas as pd


class EventCapture:

    def __init__(self, hostname, user_name, password, db_name, port):
        self.hostname = hostname
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.port = port

    def initialise_db_connection(self):
        # try:
        conn = pymysql.connect(
            host=self.hostname, user=self.user_name, password=self.password, db=self.db_name, port=int(self.port))
        print("MySQL Database connection successful")
        # except Error as err:

        return conn

    def read_json(self, conn):
        path = "../data/event-sample.json"
        with open(path, "r") as f:
            data = json.loads(f.read())
            data_dict = dict()

            for i in range(0, len(data)):
                list_item = data[i]
                payload_id = list_item["Payload"]["ID"]
                event_type = list_item["Type"]
                event_recorded_at = list_item["RecordedAt"]
                data_dict[i] = {
                    "payload_id": payload_id,
                    "event_type": event_type,
                    "event_recorded_at": event_recorded_at,
                }
            df = pd.DataFrame.from_dict(data_dict, orient="index")
            print(df.head())

            df.to_csv("../output/test.csv", index=False)

            # df.to_sql(con=conn, name='platform_events', schema='indebted_db',
            #          index = False, if_exists = 'replace')


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('../config.ini')

    capture = EventCapture(hostname=config['mysqldb']['hostname'],
                           user_name=config['mysqldb']['user_name'], password=config['mysqldb']['password'],
                           db_name=config['mysqldb']['db_name'], port=config['mysqldb']['port'])
    conn = capture.initialise_db_connection()
    capture.read_json(conn)
