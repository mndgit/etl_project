from sqlalchemy import create_engine
import pandas as pd
import pymongo
import config
import transform

def main():
    rankings_country_code_population = transform.main()

    ########################################################
    # load data to postgres
    ########################################################
    #create database connection
    engine = create_engine(f'postgresql://{config.postgres_conn_str}{config.upload_database}')
    #insert df into postgres
    rankings_country_code_population.to_sql(name='country_rankings', con=engine, if_exists='replace', index=False)

    ########################################################
    # load data to mongodb
    ########################################################
    # Initialize PyMongo to work with MongoDBs
    conn = 'mongodb://localhost:27017'
    client = pymongo.MongoClient(conn)
    # Define database and collection
    db = client.etl_project
    collection = db.country_rankings
    doc = rankings_country_code_population.set_index('country').to_dict()

    results = collection.find()
    doc_id = {}

    for i in results:
        doc_id['_id'] = str(i.get('_id'))

    collection.replace_one(doc_id, doc, upsert=True)

if __name__ == '__main__':
    main()