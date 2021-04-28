from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, row_number, floor, concat, lit
from pyspark.sql import Window

from first_task import filter_joined_tables as top_movie

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def filter_top_movie():
    return top_movie().where(col('startYear') > 1949)


def explode_genre():
    return explode(split(filter_top_movie().genres, ','))


def make_window_by_genre():
    return row_number().over(Window.partitionBy('genre').orderBy(col('averageRating').desc()))


def make_window_by_year_range():
    return row_number().over(Window.partitionBy('yearRange').orderBy(col('yearRange').desc()))


def make_year_range():
    range_small = (filter_top_movie().startYear - filter_top_movie().startYear % 10).cast('int')
    return concat(range_small, lit('-'), range_small)


def add_columns():
    return filter_top_movie() \
        .withColumn('genre', explode_genre()) \
        .withColumn('yearRange', make_year_range()) \
        .withColumn('number_genre', make_window_by_genre()) \
        .withColumn('number_year_range', make_window_by_year_range())


def find_top_movie_by_year_range():
    return add_columns() \
        .where(col('number_genre') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes', 'yearRange') \
        .orderBy(col('yearRange').desc(), col('genre').asc(), col('number_genre'))
