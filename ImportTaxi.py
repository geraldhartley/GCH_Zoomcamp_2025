#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import pandas as pd
import argparse

def main(params):
    user=params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    csv_name = 'yellow_tripdata_2021-01.parquet'

    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_parquet(csv_name)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest parquet to Postgres')

    #user, password, host, port, database name, table name
    #parquet path

    parser.add_argument('user')           # positional argument
    parser.add_argument('pass')  
    parser.add_argument('host')
    parser.add_argument('port')
    parser.add_argument('db')
    parser.add_argument('table_name')

    args = parser.parse_args()

    main(args)


