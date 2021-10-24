# Movies: Extract Transform Load

![logo](images/Module8_logo.png)

## Overview
Amazing Prime Video was a platform for streaming movies and TV shows on Amazing Prime, the world's largest online retailer.  The Amazing Prime video team would like to develop an algorithm to predict which low budget movies being released will become popular so that they can buy the streaming rights at a bargain.  To inspire the team, have some fun, and connect with the local coding community, Amazing Prime has decided to sponsor a hackathon.  Providing a clean data set of movie data and asking participants to predict the popular pictures. 

Britta, a member of the Amazing Prime video team, has been tasked with creating the datasets for the hackathon.  There are two data sources: a scrape of Wikipedia for all movies released since 1990, and rating data from the Movie Land's website.  She'll need to extract the data from the two sources, transform it into one clean data set, and finally load that data set into a SQL table. 

My job is to assist Britta in creating these data sets and loading the SQL Table.

## Resources
* Data Sources: wikipedia-movies.json, movies_metadata.csv, ratings.csv
* Softweare: Python 3.7.10, Jupyter Notebook 6.3.0, PostgreSQL 14, pgAdmin 4

## ETL Process
<img src="images/ETL.png" width=35% height=35% />
The Extract-Transform-Load process I will follow for this analysis will be as follows:

---
### EXTRACT
<img src="images/ETL-Extract.png" width=35% height=35% />

1. Extract the Wikipedia, Kaggle and Rating Data, we created the initial _function_ **extract_transform_load(wiki, kaggle, ratings)** 
2. We created ETL_function_test.ipynb to extract the files.

```
def extract_transform_load(wiki, kaggle, rating):
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(kaggle, low_memory=False)
    rating = pd.read_csv(rating)

    # 3. Open the read the Wikipedia data JSON file.
    with open(wiki, mode='r') as file:
        wiki_movies_raw = json.load(file)
        
    # 4. Read in the raw wiki movie data as a Pandas DataFrame.
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # 5. Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, rating
```

---
### TRANSFORM
<img src="images/ETL-Transform.png" width=35% height=35% />

<img src="images/Inspect-Plan-Execute.png" width=35% height=35% />

* The first part of the Transform Process is to __inspect, plan, execute__
    * _inspect the data_
    * _identify the problem, plan the repair, if necessary_
    * _execute the repair_
    * _repeat until all repairs are done_

1. Upon Inspecting the Wikipedia Data we then:
2. Plan the repair by creating _function_ **clean_movie(movie)** and modifying _function_ **extract_transform_load(wiki,kaggle,ratings)**
3. Created _ETL_clean_wiki_movies.ipynb_ using those 2 functions to clean the Wikipedia data
4. Created _ETL_clean_kaggle_data.ipynb_, modifying the _function_ **extract_transform_load(wiki,kaggle,ratings)** again, to clean the kaggle and ratings data. 

**STEP 3 above ETL_clean_wiki_movies.ipynb**
* We called the _function_ **clean_movie(movies)**, then we:
```
def clean_movie(movie):
    # Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    movie_copy = dict(movie)             # Make a copy of movie (DataFrame)
    alt_titles = {}                      # Make a new DataFrame for saving these alternate titles
    for key in ['Also known as','Arabic','Cantonese','Chinese','French','Hangul','Hebrew','Hepburn','Japanese',
                'Literally','Mandarin','McCune–Reischauer','Original Title','Polish','Revised Romanization',
                'Romanized','Russian','Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key] # add to alt_titles (DataFrame)
            movie.pop(key)               # remove from movie (DataFrame)    
            
    if len(alt_titles) > 0:              # add alternative titles dictionary to movie object
        movie['alt_titles'] = alt_titles
    
    # Function to merge columns, resulting in changing column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
        
    # Call function change_column_name to merge and rename columns
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
        
    return movie
```
* Modified the _function_ **extract_transform_load(wiki, kaggle, rating)** for the 'Wikipedia Data' with the following:
    1. box_office - set each column to the same $ format using regular expressions
    2. budget - set each column to the same $ format using regular expressions
    3. release_date - set each column to the same date format using regular expressions
    4. running_time - set each column to the same time format using regular expressions
```
def extract_transform_load(wiki, kaggle, ratings):

### >>> START HERE WITH MODIFICATIONS <<<< ####

    # 6. Write a try-except block to catch errors while extracting the IMDb ID using a regular expression 
    #    string and dropping any imdb_id duplicates. If there is an error, capture and print the exception.
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset="imdb_id", inplace=True)
    except Exception:
        print("Error occured..." (Exception)) 

    #  7. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # 8. Create a variable that will hold the non-null values from the “Box office” column.
    box_office = wiki_movies_df['Box office'].dropna()
    
    # 9. Convert the box office data created in Step 8 to string values using the lambda and join functions.
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # 10. Write a regular expression to match the six elements of "form_one" of the box office data.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]ill?i?on'
    
    # 11. Write a regular expression to match the three elements of "form_two" of the box office data.
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]ill?i?on)'
    
    # 12. Add the parse_dollars function.
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan   
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*mill?i?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value
        # if input is the form of $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*bill?i?on', s, flags=re.IGNORECASE):  
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value
        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]ill?i?on)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value
        # otherwise, return NaN
        else:
            return np.nan    
    
    # 13. Clean the box office column in the wiki_movies_df DataFrame.
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})',flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # 14. Clean the budget column in the wiki_movies_df DataFrame.
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # 15. Clean the release date column in the wiki_movies_df DataFrame.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    # 16. Clean the running time column in the wiki_movies_df DataFrame.
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    # Return three variables. The first is the wiki_movies_df DataFrame
    return wiki_movies_df, kaggle_metadata, ratings
```
---
**STEP 4 above ETL_clean_kaggle_movies.ipynb**
* We called the _function_ **clean_movie(movies)**, then we:
* Modified the _function_ **extract_transform_load(wiki, kaggle, rating)** for the 'Kaggle and Ratings Data' to clean the data for:
    1. Corrected data types - on columns: budget, id, popularity, release_date 
    3. Created new DataFrame: movies_df: from wiki_movies_df with kaggle_metadata 
    4. Dropped duplicate columns - title_wiki, release_date_wiki, Language, Production company(s)
    5. Filled in missing data into kaggle columns from wiki columns
    6. Filter the movies DataFrame for specific columns
    7. Remove column - video - found the column has NaN for all rows
    8. Rename the columns in the movies DataFrame
    9. Transform and merge the ratings DataFrame

```
def extract_transform_load(wiki, kaggle, ratings):
    ... Start after # 16. Clean the running time column in the wiki_movies_df DataFrame
    
    ### >>> START HERE WITH MODIFICATIONS <<<< ####
    # Return three variables. The first is the wiki_movies_df DataFrame
    # return wiki_movies_df, kaggle_metadata, ratings
     
    # 2. Clean the Kaggle metadata.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    # set video to True
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # convert dtypes on budget, id, popularity, release_dt
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    # 3. Merged the two DataFrames into the movies DataFrame.
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # 4. Drop unnecessary columns from the merged DataFrame.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # 5. Add in the function to fill in the missing Kaggle data.
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis = 1)
        df.drop(columns=wiki_column, inplace=True)

    # 6. Call the function in Step 5 with the DataFrame and columns as the arguments.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # 7. Filter the movies DataFrame for specific columns.
    for col in movies_df.columns:
        lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
        value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
        num_values = len(value_counts)
        if num_values == 1:
            print(col)
            
    movies_df.drop(columns=['video'], inplace=True)

    # 8. Rename the columns in the movies DataFrame.
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
        'genres','original_language','overview','spoken_languages','Country',
        'production_companies','production_countries','Distributor',
        'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
        ]]

    movies_df.rename({'id':'kaggle_id',
              'title_kaggle':'title',
              'url':'wikipedia_url',
              'budget_kaggle':'budget',
              'release_date_kaggle':'release_date',
              'Country':'country',
              'Distributor':'distributor',
              'Producer(s)':'producers',
              'Director':'director',
              'Starring':'starring',
              'Cinematography':'cinematography',
              'Editor(s)':'editors',
              'Writer(s)':'writers',
              'Composer(s)':'composers',
              'Based on':'based_on'
             }, axis='columns', inplace=True)
    
    # 9. Transform and merge the ratings DataFrame.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
```    
---
### LOAD
<img src="images/ETL-Load.png" width=35% height=35% />

1. The final step in ETL is Load, we created a new file ETL_create_database.ipynb.
2. Pre-setup steps were needed:
    1. Create a PostgreSQL database called 'movie_data'
    2. Know the "db_password" and "server IP" and "port" of this database
    3. Place the db_password inside your config.py file
    4. Place config.py inside your .gitignore file
    5. Ensure you have the PostgreSQL database online and you have access to it
    6. Place the proper imports/config information at the top of your ETL_create_database.ipynb file.
3. Here is an example of what you need at the top of your ETL_create_database.ipynb file.

```
import psycopg2
from config import db_password
from sqlalchemy import create_engine
db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5433/movie_data"
engine = create_engine(db_string)
```

4. We also had to make a final modification to our _function_ **extract_transform_load(wiki, kaggle, ratings)** 
```
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    # Move movies_df DataFrame to Postgresql
    # using if_exists='replace' will create the wikipedia/kaggle table data and if_exists, will replace the data if already in the table
    movies_df.to_sql(name='movies_data', con=engine, if_exists='replace', index=False)
    
    rows_imported = 0
    start_time = time.time()
    # chunksize is needed as ratings_file is very large and it needs to be broken up to complete
    for data in pd.read_csv(ratings_file, chunksize=1000000):

        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

        # using if_exists='append' will allow the process to continue if the data is already in the table
        data.to_sql(name='ratings', con=engine, if_exists='append', index=False)

        # increment the number of rows imported by the chunksize
        rows_imported += len(data)

        # print that the rows have finished importing
        print(f'Done. {time.time() - start_time} total seconds elapsed.')
```
### RESULTS

Our PostgreSQL Database now holds 3 new tables, with clean data, transformed into readable formats for the Hackathon. Each table within the movie_data database, movies, movies_data and ratings are now ready for the challenge set forth by Amazing Prime.  The hackers will all be given the same set of data, in the same format, from the same location, so there is no bias or advantage to anyone.   There is also a set dataset so Amazing Prime can also do their own analysis and see which Hacker from the challenge meets their idea of what Amazing Prime believes are the up and coming movies to purchase quickly.

### Movies
![movies](images/movies.png)

---
### Movies Data
![movies_data](images/movies_data.png)
![logo](images/Module8_logo.png)

---
### Ratings
![ratings](images/ratings.png)

