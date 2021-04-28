from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from data import info_names, info_staff
from first_task import filter_joined_tables as top_movie

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def join_movie_and_staff():
    return top_movie().join(info_staff, 'tconst', 'inner')


def join_names():
    return join_movie_and_staff() \
        .join(info_names, 'nconst', 'inner')


def find_top_actors():
    return join_names() \
        .where(col('category').like('%act%')) \
        .groupby('primaryName', 'nconst').count() \
        .orderBy(col('count').desc()) \
        .select('primaryName')
