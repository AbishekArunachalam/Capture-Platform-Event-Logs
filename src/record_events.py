#!/usr/bin/env python3

import json
import configparser
import pandas as pd
from sqlalchemy import exc
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database


class EventCapture():
    def __init__(self, hostname, user_name, password, db_name, table_name, port):
        self.hostname = hostname
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.table_name = table_name
        self.port = port

    def initialise_db_connection(self):
        """creates DB connection object

        Returns:
            object: db connection
        """
        try:
            engine = create_engine(
                f"mysql+pymysql://{self.user_name}:{self.password}@{self.hostname}:{self.port}/{self.db_name}")

            if not database_exists(engine.url):
                create_database(engine.url)
            print("MySQL Database connection established successfully")

        except exc.SQLAlchemyError as err:
            print(
                f"Exception encountered while establishing connection to server: {str(err)}")
            raise

        return engine

    def read_json(self, path):
        """parses and converts json to dataframe

        Returns:
            dataframe: json to dataframe format 
        """
        try:
            with open(path, "r") as f:
                json_data = json.loads(f.read())
                print(
                    f"{len(json_data)} records read from events JSON file")

                data_dict = dict()
                for i in range(0, len(json_data)):
                    list_item = json_data[i]
                    try:
                        payload_id = list_item["Payload"]["ID"]
                        event_type = list_item["Type"]
                        event_recorded_at = list_item["RecordedAt"]
                    except KeyError as e:
                        print(f"Key {str(e)} not found in dictionary object")
                        raise

                    data_dict[i] = {
                        "payload_id": payload_id,
                        "event_type": event_type,
                        "event_recorded_at": event_recorded_at,
                    }
                df = pd.DataFrame.from_dict(data_dict, orient="index")
                print(df.head())
                df.to_csv("output/events.csv", index=False)
        except Exception as e:
            print(f"Exception encountered while loading JSON file: {str(e)}")
            raise

        return df

    def write_to_db(self, engine, df):
        """writes dataframe to a table in mysql db

        Args:
            engine (object): db connection 
            df (dataframe): events dataframe
        """
        try:
            df.to_sql(con=engine, name=self.table_name, schema=self.db_name,
                      index=False, if_exists='replace')
            sql = text(
                f"select count(*) as record_count from {self.db_name}.{self.table_name}")
            result = engine.execute(sql)
        except exc.SQLError as e:
            print(f'Database operation failed: {str(e)}')
            raise

        count = [row[0] for row in result][0]
        print(
            f"{count} records successfully inserted to {self.table_name} table")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini')
    capture = EventCapture(hostname=config['mysqldb']['hostname'],
                           user_name=config['mysqldb']['user_name'],
                           password=config['mysqldb']['password'],
                           db_name=config['mysqldb']['db_name'],
                           table_name=config['mysqldb']['table_name'],
                           port=int(config['mysqldb']['port']))
    engine = capture.initialise_db_connection()
    path = "data/event-sample.json"
    df = capture.read_json(path)
    capture.write_to_db(engine, df)
    engine.dispose()
