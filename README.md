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
<img src="images/ETL-Extract.png" width=35% height=35% />
1. Extract the Wikipedia, Kaggle and Rating Data

* This will be performed by the _Function_ **extract_transform_load(wiki, kaggle, rating)**
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
<img src="images/ETL-Transform.png" width=35% height=35% />

<img src="images/Inspect-Plan-Execute.png" width=35% height=35% />

* The first part of the Transform Process is to __inspect, plan, execute__
    * _inspect the data_
    * _identify the problem, plan the repair, if necessary_
    * _execute the repair_
    * _repeat until all repairs are done_

1. Clean the Wikipedia and Kaggle Data

* This will be performed by the _Function_ **clean_movie(movie)**
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
2. Recall the _Function_ **extract_transform_load(wiki, kaggle, rating)** for the 'Wikipedia Data' to clean the data for:
    1. box_office - set each column to the same $ format using regular expressions
    2. budget - set each column to the same $ format using regular expressions
    3. release_date - set each column to the same date format using regular expressions
    4. running_time - set each column to the same time format using regular expressions
```
# 2. Add the function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)
def extract_transform_load(wiki, kaggle, ratings):
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(kaggle, low_memory=False)
    ratings = pd.read_csv(ratings)     

    # Open and read the Wikipedia data JSON file.
    with open(wiki, mode='r') as file:
        wiki_movies_raw = json.load(file)
    
    # 3. Write a list comprehension to filter out TV shows.
    wiki_movies = [movie for movie in wiki_movies_raw
        if 'No. of episodes' not in movie]

    # 4. Write a list comprehension to iterate through the cleaned wiki movies list
    # and call the clean_movie function on each movie.
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # 5. Read in the cleaned movies list from Step 4 as a DataFrame.
    wiki_movies_df = pd.DataFrame(clean_movies)

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
