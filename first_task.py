from datetime import datetime
from pyspark.sql import SparkSession
from data import info_cinema, info_ratings
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def join_two_table():
    return info_cinema.join(info_ratings, 'tconst', 'inner')


def filter_joined_tables():
    return join_two_table() \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc()).limit(100)


def find_top_movie():
    return filter_joined_tables() \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear')


def find_top_movie_last_10y():
    return find_top_movie() \
        .where(col('startYear') >= datetime.now().year - 10)


def find_top_movie_60s():
    return find_top_movie() \
        .where(col('startYear').between('1960', '1969'))

