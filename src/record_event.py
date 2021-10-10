#!/usr/bin/env python3

import os
import json
import configparser
import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from sqlalchemy_utils import database_exists, create_database


class EventCapture:

    def __init__(self, hostname, user_name, password, db_name, table_name, port):
        self.hostname = hostname
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.port = port

    def initialise_db_connection(self):
        try:
            engine = db.create_engine(
                f'mysql+pymysql://{self.user_name}:{self.password}@{self.hostname}:{self.port}/{self.db_name}')

            if not database_exists(engine.url):
                create_database(engine.url)
            print("MySQL Database connection successful")

        except exc.SQLAlchemyError as err:
            print(err)

        return engine

    def read_json(self):
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
            # df.to_csv("../output/test.csv", index=False)

        return df

    def write_tosql(self, conn, df):
        df.to_sql(con=conn, name=self.table_name, schema=self.db_name,
                  index=False, if_exists='replace')


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('../config.ini')
    capture = EventCapture(hostname=config['mysqldb']['hostname'],
                           user_name=config['mysqldb']['user_name'],
                           password=config['mysqldb']['password'],
                           db_name=config['mysqldb']['db_name'],
                           table_name=config['mysqldb']['table_name'],
                           port=int(config['mysqldb']['port']))
    engine = capture.initialise_db_connection()
    df = capture.read_json()
    capture.write_tosql(engine, df)
