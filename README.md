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
1. Extract the Wikipedia Data, the Kaggle and Rating Data

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
    
 
