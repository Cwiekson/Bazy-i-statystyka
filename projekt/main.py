import os
from datetime import datetime

import pandas as pd
import sqlalchemy


def create_engine(name):

    engine = sqlalchemy.create_engine(f'postgresql://postgres:bartek11@localhost:5432/{name}')
    return engine



def create_archive(df, name):
    if not os.path.exists('archiwum'):
        os.mkdir('archiwum')
    now = datetime.now()
    df.to_csv(f'archiwum/{name}archiwum{now.month}_{now.day}_{now.hour}{now.minute}{now.second}.csv')


def add_df(df, engine, name):
    df.to_sql(name, engine, if_exists='replace', index=True)
    create_archive(df, name)




engine = create_engine('ProjektBazy')

df = pd.read_csv('./data/training_data_with_weather_info_week_1.csv')
df = df.drop(columns=['Id', 'Province/State', 'Lat', 'Long', 'rh', 'ah'])
df = df.rename(columns={'Country/Region': 'country','Date':'date','ConfirmedCases':'confirmedcases',
                    'Fatalities':'fatalities'    })


add_df(df, engine, 'data1')
connection = engine.connect()
connection.execute(f"CREATE TABLE data1_usuniete AS SELECT * FROM data1 WHERE 1=2")
connection.execute("""CREATE TABLE logs (
                            id SERIAL PRIMARY KEY,
                            action VARCHAR(40) NOT NULL,
                            table_name VARCHAR(255) NOT NULL,
                            time TIMESTAMP NOT NULL)
                        """)

print(df.dtypes)

print(df.head(10))
