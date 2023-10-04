import os
import pandas as pd
import sqlalchemy as sql
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
HOST = os.getenv('DB_HOST')
DB = os.getenv('DB_DATABASE')
SCHEMA = os.getenv('DB_SCHEMA')
TABLE = os.getenv('DB_TABLE')
USER = os.getenv('DB_USER')
PASS = os.getenv('DB_PASSWORD')
FILEPATH = os.getenv('FILEPATH')

engine = sql.create_engine(f'mssql+pymssql://{USER}:{PASS}@{HOST}/{DB}')

with engine.connect() as connection:
    # Timestamp check
    rows = connection.execute(f'SELECT MAX(timestamp) FROM {DB}.{SCHEMA}.{TABLE}').fetchone()
    dateFromDB = rows[-len(rows)]
    newDate = datetime.combine(datetime.now().date(), dateFromDB.time())
    dateCheck = (newDate.date() - dateFromDB.date()).days >= 7
    forceInsert = False
    print(f'Last date taken from the DB is {dateFromDB}')
    print(f'New date to use as timestamp is {newDate}')
    if not dateCheck:
        print(f'Less than 7 days have passed since the last import. Do you wish to continue? (Y/N)')
        forceInsert = True if input().lower() == 'y' else False
    
    # DataFrame generation
    dataframe = pd.read_excel(FILEPATH)
    dataframe.insert(0, 'timestamp', newDate)
    dataframe.rename(inplace=True, columns={
        'Antenna_installation_type': 'site_type',
        'place rank': 'place_rank',
        'place rank num': 'place_rank_num',
        'STATE CHECK': 'STATE_CHECK'
    })
    dataframe = dataframe.drop(columns=['siteid', 'check'])
    
    # DB insertion
    if dateCheck or forceInsert:
        print(f'Sample data that will be loaded: \n {dataframe.head()}')
        res = dataframe.to_sql(schema=SCHEMA, name=TABLE, con=connection, if_exists='append', index=False)
        print(f'Inserted {res} rows of data into the DB. Goodbye!') if res else None
    else:
        print('No data was inserted into the DB. Goodbye!')
