'''
Data visualization steps including:

- Creating views based on preprocessed data tables in the BigQuery data warehouse.
- Utilizing the Google Looker studio for visualization.
- As Looker studio has a maximum limit of 5000 points, I used matplotlib to visualize the data distribution for some of the exploratory data analysis.

'''

from google.cloud import bigquery
import matplotlib.pyplot as plt

#Google cloud Authentication.
client = bigquery.Client(project="funda-assignment-380910")

query = """
CREATE VIEW `results.avg_rating_of_each_genre` AS
    SELECT 
        movie_releaseYears_genre.genre,
        ROUND(AVG(movie_ratings.rating),2) AS avg_rating_of_each_genre,
        COUNT(DISTINCT movie_releaseYears_genre.movieId) as count_movies
    FROM `preproceed_movie_data.movie_releaseYears_genre` AS movie_releaseYears_genre
    JOIN `preproceed_movie_data.movie_ratings` AS movie_ratings
    ON movie_releaseYears_genre.movieId = movie_ratings.movieId
    GROUP BY movie_releaseYears_genre.genre
    ORDER BY avg_rating_of_each_genre, count_movies
"""
query_job = client.query(query)

query = """
CREATE VIEW `funda-assignment-380910.results.avg_rating_of_each_release_year` AS
    SELECT 
        movie_releaseYears_genre.releaseYear,
        ROUND(AVG(movie_ratings.rating),2) AS avg_rating_of_each_year,
        COUNT(DISTINCT movie_releaseYears_genre.movieId) as count_movies
    FROM `preproceed_movie_data.movie_releaseYears_genre` AS movie_releaseYears_genre
    JOIN `preproceed_movie_data.movie_ratings` AS movie_ratings
    ON movie_releaseYears_genre.movieId = movie_ratings.movieId
    GROUP BY movie_releaseYears_genre.releaseYear
"""
query_job = client.query(query)

query = """
CREATE VIEW `results.user_rate_behavior`
    SELECT 
        movie_ratings.userId,
        COUNT(movie_ratings.movieId) AS count_movies,
    FROM `preproceed_movie_data.movie_ratings` AS movie_ratings
    GROUP BY movie_ratings.userId
"""
query_job = client.query(query)

query = """
CREATE VIEW `results.user_123100_rate_behavior` AS
    SELECT 
        COUNT(movie_ratings.movieId) count_rated_movies, 
        CAST( movie_ratings.timestamp AS DATE) AS rate_date
    FROM `preproceed_movie_data.movie_ratings` AS movie_ratings
    WHERE movie_ratings.userId = '123100'
    GROUP BY rate_date
"""
query_job = client.query(query)

query = """
CREATE VIEW `results.tags_repeat_times` AS
    SELECT movie_tags.movieId AS movieId, 
    movie_tags.tag AS tag,
    COUNT(userId) AS count_users,
    FROM `funda-assignment-380910.preproceed_movie_data.movie_tags` AS movie_tags
    GROUP BY movieId, tag
"""
query_job = client.query(query)

# Visualize the quantity of movies rated by each user
query = """
    SELECT *
    FROM `results.user_rate_behavior`
"""
query_job = client.query(query)
user_ratings = query_job.to_dataframe().values.tolist()
user_ratings = sorted(user_ratings, key= lambda x:x[1], reverse=True)
X = [0] * len(user_ratings)
Y = []
for i in user_ratings: 
    Y.append(i[1])
plt.scatter(Y,Y, alpha=0.6,edgecolors='r')
plt.xlabel('the quantity of movies rated by each user')
plt.ylabel('the quantity of movies rated by each user')
plt.show()

# Visualize the tags occurred times distribution
query = """
    SELECT *
    FROM `results.tags_repeat_times`
"""
query_job = client.query(query)
repeat_tags = query_job.to_dataframe().values.tolist()
X = list(range(len(repeat_tags)))
Y = []
repeat_more_than_once = 0
for i in repeat_tags:
    if i[2] > 1:
        repeat_more_than_once +=1
    Y.append(i[2])
Y = sorted(Y,reverse=True)
plt.scatter(X,Y, alpha=0.6,edgecolors='r')
plt.xlabel('tag index (unique tags of each movie)')
plt.ylabel('The number of users who gave the same tag')
plt.show()
print('Out of all the unique tags, '+str(repeat_more_than_once)+' were assigned by more than one user, which accounts for '+str((repeat_more_than_once/len(repeat_tags))*100)+'% of the total unique tags.')