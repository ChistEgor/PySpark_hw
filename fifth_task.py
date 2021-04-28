from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window

from data import info_names, info_crew

from first_task import filter_joined_tables as top_movie

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def make_window_by_name():
    return row_number().over(Window.partitionBy('primaryName').orderBy(col('averageRating').desc()))


def join_movie_and_crew():
    return top_movie().join(info_crew, 'tconst', 'inner')


def join_names():
    return join_movie_and_crew().join(info_names, join_movie_and_crew().directors == info_names.nconst, 'inner') \
        .withColumn('row_number', make_window_by_name())


def find_top_5_by_directors():
    return join_names() \
        .where(col('row_number') < 6) \
        .orderBy('directors') \
        .select('primaryName', 'primaryTitle', 'startYear',
                'averageRating', 'numVotes')
