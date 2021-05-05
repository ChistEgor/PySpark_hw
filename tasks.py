from datetime import datetime

from pyspark.sql.functions import col

import functions as f
import main


# 1
def get_top_movie(df):
    """
    Shows the top 100 films
    """
    return df.select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear').limit(100)


def find_top_movie_last_10y(df):
    """
    Filters out the dataframe from the last 10 years
    """
    return df.where(col('startYear') >= datetime.now().year - 10) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear').limit(100)


def find_top_movie_60s(df):
    """
    Shows the top films of the 60s
    """
    return df.where(col('startYear').between(1960, 1969)) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear').limit(100)


# 2
def find_top_movie_each_genre(df):
    """
    Shows up to 10 the top films by genre
    """
    df = f.explode_by_genres(df)
    df = f.row_number_window(df, 'genres', 'averageRating', 'row_number')
    return df \
        .where(col('row_number') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes')


# 3
def find_top_movie_by_year_range(df):
    """
    Finding the top movie in every decade beginning with 1950 years
    """
    df = df.where(col('startYear') > 1949)
    df = f.explode_by_genres(df)
    df = f.make_year_range(df)
    df = f.row_number_window(df, 'year_range', 'year_range', 'number_year_range')
    df = f.row_number_window(df, 'genres', 'averageRating', 'number_genre')

    return df \
        .where(col('number_genre') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genres', 'averageRating', 'numVotes', 'year_range') \
        .orderBy(col('year_range').desc(), col('genres').asc(), col('number_genre'))


# 4
def find_top_actors(df):
    """
    Shows the best actors which have acted more than 1 time
    """
    df = f.join_table(df, main.info_staff, 'tconst')
    df = f.join_table(df, main.info_names, 'nconst')

    return df \
        .where(col('category').like('%act%')) \
        .groupby('nconst', 'primaryName').count() \
        .orderBy(col('count').desc()) \
        .select('primaryName')


# 5
def find_top_5_by_directors(df):
    """
    Shows the best five movies of every director
    """
    df = f.join_table(df, main.info_crew, 'tconst')
    df = f.join_table(df, main.info_names, df.directors == main.info_names.nconst)
    df = f.row_number_window(df, 'primaryName', 'averageRating', 'row_number')

    return df \
        .where(col('row_number') < 6) \
        .orderBy(col('directors')) \
        .select('primaryName', 'primaryTitle', 'startYear',
                'averageRating', 'numVotes')
