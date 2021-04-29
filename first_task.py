from datetime import datetime

from pyspark.sql.functions import col


def find_top_movie(top_movie):
    return top_movie.select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')


def find_top_movie_last_10y(top_movie):
    return top_movie.where(col('startYear') >= datetime.now().year - 10) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')


def find_top_movie_60s(top_movie):
    return top_movie.where(col('startYear').between('1960', '1969')) \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')
