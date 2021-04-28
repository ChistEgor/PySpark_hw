from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, row_number
from pyspark.sql import Window

from first_task import filter_joined_tables as top_movie

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def explode_genre():
    return explode(split(top_movie().genres, ','))


def make_window_by_genre():
    return row_number().over(Window.partitionBy('genre').orderBy(col('averageRating').desc()))


def add_columns():
    top_movie_each_genre = top_movie() \
        .withColumn('genre', explode_genre()) \
        .withColumn('row_number', make_window_by_genre())
    return top_movie_each_genre


def find_top_movie_each_genre():
    return add_columns() \
        .where(col('row_number') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes')
