'''
ETL and Preprocessing steps including:

- Extracting data from CSV files. 
- Parsing the release year information from movie titles, and converting it to datetime format. 
- Empty release year data was filled in with the date '1111'. 
- The genre information was separated into multiple rows and checked for misspellings. 
- 16 NaN tags were detected. 
- Timestamp text in the ratings and tags tables was transformed to datetime format.
- All tags were converted to lowercase. 
- The preprocessed tables were loaded into the BigQuery data warehouse."

'''

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import re
from datetime import datetime

#Google cloud Authentication.
client = bigquery.Client(project="funda-assignment-380910")
storage_client = storage.Client(project="funda-assignment-380910")
path = 'gs://funda_assignment_data_resources/funda assignment data/'

# Extracting data from CSV files in Google Cloud Storage. 
movies_df = pd.read_csv(path+'movies.csv')
ratings_df = pd.read_csv(path+'ratings.csv')
tags_df = pd.read_csv(path+'tags.csv')

# Parsing the release year information from movie titles. 
# Empty release year data was filled in with the date '1111'
movieInfo = movies_df.values.tolist()
movies_with_year_info = []
for i in range(len(movieInfo)):
    try: #the movie release years info in the titles will be convert from str to int to be verified if it's a valid release year info.
        movies_with_year_info.append([movieInfo[i][0],movieInfo[i][1],str(int(re.findall(re.compile(r'[(](.*?)[)]',re.S), movieInfo[i][1])[-1])),movieInfo[i][2]])
    except: # Empty release year data was filled in with the date '1111'. 
        movies_with_year_info.append([movieInfo[i][0],movieInfo[i][1],'1111',movieInfo[i][2]])
        continue

# The genre information was separated into multiple rows and checked for misspellings. 
movies_with_year_and_genres = []
genres = []
for i in range(len(movies_with_year_info)):
    for j in range(len(movies_with_year_info[i][3].split('|'))):
        movies_with_year_and_genres.append([movies_with_year_info[i][0],
                                            movies_with_year_info[i][1],
                                            # Converting releaseYear to datetime format
                                            datetime.strptime(movies_with_year_info[i][2],'%Y'),
                                            movies_with_year_info[i][3].split('|')[j]
                                            ])
        genres.append(movies_with_year_info[i][3].split('|')[j])

# Manually check if there are misspellings in genres. 
# The print results shows all the genres were well given.
print('The unique genres text from all the movies:')
print(list(set(genres)))

# Check if there is any NaN cells among the 3 source tables
print('movies NaN cells:')
print(movies_df[movies_df.isnull().T.any()])
print('ratings NaN cells:')
print(ratings_df[ratings_df.isnull().T.any()])
print('tags NaN cells:') #16 NaN tags were detected. And they will be ignored in SQL views.
print(tags_df[tags_df.isnull().T.any()])

# Timestamp text in the ratings and tags tables was transformed to datetime format.
# movieId, and userId were transformed to text format.
ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'],unit='s')
ratings_df['movieId'] = ratings_df['movieId'].apply(str)
ratings_df['userId'] = ratings_df['userId'].apply(str)
tags_df['timestamp'] = pd.to_datetime(tags_df['timestamp'],unit='s')
tags_df['movieId'] = tags_df['movieId'].apply(str)
tags_df['userId'] = tags_df['userId'].apply(str)
#All tags were converted to lowercase. 
tags_df['tag'] = tags_df['tag'].str.lower()

#The preprocessed tables were loaded into the BigQuery data warehouse.
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
movie_releaseYears_genre_df = []
for i in range(len(movies_with_year_and_genres)):
    movie_releaseYears_genre_df.append({"movieId":str(movies_with_year_and_genres[i][0]),
                                       "title": str(movies_with_year_and_genres[i][1]),
                                       "releaseYear": movies_with_year_and_genres[i][2],
                                       "genre": movies_with_year_and_genres[i][3]
                                       })
movie_releaseYears_genre_df = pd.DataFrame(movie_releaseYears_genre_df,columns=["movieId",
                                                             "title",
                                                             "releaseYear",
                                                             "genre"])
job = client.load_table_from_dataframe(movie_releaseYears_genre_df, "preproceed_movie_data.movie_releaseYears_genre", job_config=job_config)
job = client.load_table_from_dataframe(ratings_df, "preproceed_movie_data.movie_ratings", job_config=job_config)
job = client.load_table_from_dataframe(tags_df, "preproceed_movie_data.movie_tags", job_config=job_config)