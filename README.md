# ETL-Project

![ETL.jpg](https://panoply.io/uploads/versions/diagram4---x----750-328x---.jpg)

## Datasets

1. Website: [Heritage - Country Rankings](https://www.heritage.org/index/ranking)

    * Scraped the tables from this website to get the following fields:
        * Country Name
        * Overall
        * Change

2. CSV: [Countries By Population 2019](population_areacodes_rankings/Resources/countries_by_population_2019.csv)

    * Read csv file using pandas to get the following fields:
        *	Rank
        *	Country Name
        *	Population
        *	Growth Rate
        *	Area
        *	Density

3. CSV/Postgres: [Country Codes 2020](population_areacodes_rankings/Resources/country_codes_2020.csv)

    * Import csv to postgres and query the database to get the following fields:
        * Country Name
        * Country Code 1
        * Country Code 2
        * Country Code 3

## Extract

We began by extracting the data from the datasets above using the following modules:
* Pandas
* Sqlalchemy
* BeautifulSoup
* Requests
* Pymongo
* Splinter

### [Heritage - Country Rankings](https://www.heritage.org/index/ranking) 

Two options for extraction:

* Splinter

```python
#url to be scraped
url = 'https://www.heritage.org/index/ranking'
#use chrome as browser and visit url/parse html from url
executable_path = {'executable_path': 'chromedriver.exe'}
browser = Browser('chrome', **executable_path, headless=True)
browser.visit(url)
html = browser.html
soup = BeautifulSoup(html, 'html.parser')
```

* Request

```python
#url to be scraped
url = 'https://www.heritage.org/index/ranking'
# Retrieve page with the requests module and parse html from url
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
```
    
Using assert, we ensure that the table data extracted are the same

```python
assert (splinter_table_data == request_table_data)
```

### [Countries By Population 2019](population_areacodes_rankings/Resources/countries_by_population_2019.csv)

We read the csv using pandas module.

```python
# File to Load from csv file(Data# 1)
file_to_load = "Resources/countries_by_population_2019.csv"

# Read Purchasing File and store into Pandas data frame
country_population = pd.read_csv(file_to_load)
```

### [Country Codes 2020](population_areacodes_rankings/Resources/country_codes_2020.csv)

We created a database in Postgres called `etl_project` and used the following code to create a table.

```postgres
DROP TABLE IF EXISTS country_code;
CREATE TABLE country_code(
name character varying(200) NOT NULL,
cca2 character varying(5) NOT NULL,
cca2 character varying(5) NOT NULL,
ccn3 character varying(20) NOT NULL,
 PRIMARY KEY (name)
);
```

We import the csv into postgres and then query the database.

```python
#connection made to connect to Postgres sql to bring the country_code table from country Database(Data#2 from SQL DB)
engine = create_engine('postgresql://postgres:postgres@localhost:5432/country')
connection = engine.connect()
country_code=pd.read_sql("SELECT * FROM country_code", connection)
```

## Transform

### [Heritage - Country Rankings](https://www.heritage.org/index/ranking)  

HTML code to be extracted:

```html
<table cellspacing="0" cellpadding="0" class="rankings">    
    <tbody><tr>
      <th scope="col" class="rank"><!--world -->Rank</th>
      <th scope="col" class="country">Country</th>
      <th scope="col" class="overall">Overall<!-- score--></th>
      <th scope="col" class="change">Change<!-- from previous--></th>
      <th class="spacer"></th>
      <th scope="col" class="rank"><!--world -->rank</th>
      <th scope="col" class="country">Country</th>
      <th scope="col" class="overall">Overall<!-- score--></th>
      <th scope="col" class="change">Change<!-- from previous--></th>
    </tr> 
          <tr class="alt">
                  <td class="rank free">1</td>
                  <td class="country"><a href="country/singapore">Singapore</a></td>
                  <td class="overall">89.4</td>
                  <td class="change neutral">0.0</td>
                  
     			  <td class="spacer"></td>
                  <td class="rank free">4</td>
                  <td class="country"><a href="country/australia">Australia</a></td>
                  <td class="overall">82.6</td>
                  <td class="change positive">+1.7</td>
                  </tr>
           <tr>
                  <td class="rank free">2</td>
                  <td class="country"><a href="country/hongkong">Hong Kong</a></td>
                  <td class="overall">89.1</td>
                  <td class="change negative">-1.1</td>
                  
     			  <td class="spacer"></td>
                  <td class="rank free">5</td>
                  <td class="country"><a href="country/switzerland">Switzerland</a></td>
                  <td class="overall">82.0</td>
                  <td class="change positive">+0.1</td>
                  </tr>
           <tr class="alt">
                  <td class="rank free">3</td>
                  <td class="country"><a href="country/newzealand">New Zealand</a></td>
                  <td class="overall">84.1</td>
                  <td class="change negative">-0.3</td>
                  
     			  <td class="spacer"></td>
                  <td class="rank free">6</td>
                  <td class="country"><a href="country/ireland">Ireland</a></td>
                  <td class="overall">80.9</td>
                  <td class="change positive">+0.4</td>
                  </tr>
             
    </tbody></table>
```

Reading through the html below, we use BeautifulSoup method, "find_all()" to extract `<td>` tags and loop through the `string` returned to get the country, overall and change data relying on the class.

```python
#find all table data
results = soup.find_all('td')

#create empty lists
country_list, overall_list, change_list = ([] for i in range(3))

#append result to a list depending on the td class is associated with
for result in results:
    text = result.text.strip()
    if 'country' in result['class']:
        country_list.append(text)
    elif 'overall' in result['class']:
        overall_list.append(text)
    elif 'change' in result['class']:
        change_list.append(text)
```

Once complete, we convert the the list into a dictionary to create a pandas dataframe.

```python
#convert to dataframe
dict = {'country': country_list,'overall': overall_list,'change': change_list}
rankings_df = pd.DataFrame(dict)
```

![enonomic_ranking_df.png](population_areacodes_rankings/Resources/Images/enonomic_ranking_df.PNG)

### [Countries By Population 2019](population_areacodes_rankings/Resources/countries_by_population_2019.csv) / [Country Codes 2020](population_areacodes_rankings/Resources/country_codes_2020.csv)

After extracting the csv and postgress query, we merge them together.

```python
country_code_population = country_population.merge(country_code, on='name')
```

Clean the data

```python
#drop NA
country_code_population=country_code_population.dropna(how="all",axis=0)
country_code_population=country_code_population.dropna(how="all",axis=1)
country_code_population=country_code_population

#formatting the values
country_code_population['Density']=country_code_population['Density'].map("{0:.2f}".format)
country_code_population['pop2019']=country_code_population['pop2019'].map("{0:.0f}".format)
country_code_population['area']=country_code_population['area'].map("{0:.0f}".format)

#rename columns and eliminating columns
country_code_population = country_code_population.rename(columns={"name": "country",
                                               "pop2019": "Population",
                                               "area": "Area","Density":"Density","cca2":"Country Code_1","cca3":"Country Code_2","ccn3":"Country Code_#"
                                               })
country_code_population=country_code_population[["Rank","country","Population","Area","Density","Country Code_1","Country Code_2","Country Code_#"]]
```

![population_merged_df.png](population_areacodes_rankings/Resources/Images/population_merged_df.PNG)

### Final DF

We then merge both the dataframes together.

```python
rankings_country_code_population=country_code_population.merge(rankings_df, on='country')
```

![population_ranking_merged_df.png](population_areacodes_rankings/Resources/Images/population_ranking_merged_df.PNG)

## Load

### Using Postgres

We create our engine. We used a config.py file to hide our secrets. Please do the same to be able to run this code. After creating the enginem we insert the final dataframe into postgres and overwrite any existing data.

```python
#create database connection
engine = create_engine(f'postgresql://{postgres_conn_str}')

#insert df into postgres
rankings_country_code_population.to_sql(name='country_rankings', con=engine, if_exists='replace', index=False)
```

### Using MongoDB

We create out connection and connect using pymongo.

```python
# Initialize PyMongo to work with MongoDBs
conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
```

After it is done, we specify the database and collection to where the data should be inserted. This can be anything the user chooses (rename database "etl_project" or the collection "country_rankings")

```python
# Define database and collection
db = client.etl_project
collection = db.country_rankings
```

Because mongodb cannot have an integer as the key of the dictionary, we used the country name as the key and converted the dataframe to a dictionary. We find all collections within the document and get the `_id`. We then use the `_id` to replace the document if it already exists, or we insert a new document if no document `_id` is found.

```python
doc = rankings_country_code_population.set_index('country').to_dict()

results = collection.find()
doc_id = {}

for i in results:
    doc_id['_id'] = str(i.get('_id'))

collection.replace_one(doc_id, doc, upsert=True)
```