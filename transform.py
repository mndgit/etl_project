import pandas as pd
import config
from sqlalchemy import create_engine
import extract

def transform():
    #extract data
    rankings_df = extract.scrape()

    #reading data from csv file
    file_to_load = "Resources/countries_by_population_2019.csv"
    country_population = pd.read_csv(file_to_load)

    #connection made to connect to Postgres sql to bring the country_code table from country Database(Data#2 from SQL DB)
    engine = create_engine(f'postgresql://{config.postgres_conn_str}{config.csv_database}')
    connection = engine.connect()
    #reading data from sql DB and merging with CSV data
    country_code=pd.read_sql("SELECT * FROM country_code", connection)
    country_code_population = country_population.merge(country_code, on='name')

    #drop NA
    country_code_population=country_code_population.dropna(how="all",axis=0)
    country_code_population=country_code_population.dropna(how="all",axis=1)
    country_code_population=country_code_population

    #formating thevalues
    country_code_population['Density']=country_code_population['Density'].map("{0:.2f}".format)
    country_code_population['pop2019']=country_code_population['pop2019'].map("{0:.0f}".format)
    country_code_population['area']=country_code_population['area'].map("{0:.0f}".format)

    #renaming  and eliminating columns
    country_code_population = country_code_population.rename(columns={"name": "country",
                                                "pop2019": "Population",
                                                "area": "Area","Density":"Density","cca2":"Country Code_1","cca3":"Country Code_2","ccn3":"Country Code_#"
                                                })
    country_code_population=country_code_population[["Rank","country","Population","Area","Density","Country Code_1","Country Code_2","Country Code_#"]]

    #combining the data with the webscraped data
    rankings_country_code_population=country_code_population.merge(rankings_df, on='country')

    #removing duplicates
    rankings_country_code_population.drop_duplicates(subset ="country", keep = False, inplace = True)
    
    return rankings_country_code_population
