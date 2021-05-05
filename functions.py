from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, row_number, concat, lit
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('main') \
    .getOrCreate()


def read_data(file_name):
    """
    Reads csv file from input_data
    """
    return spark.read.csv(f'input_data/{file_name}', sep=r'\t', header=True, inferSchema=True)


def write_to_csv(data, file):
    """
    Makes one dataframe from group of and saves to file
    """
    return data.coalesce(1).write.mode('overwrite').save(f'output_data/{file}', format='csv')


def join_table(left_df, right_df, condition, how='inner'):
    """
    Create dataframe by joining two datasets and condition for joining
    """
    return left_df.join(right_df, condition, how)


def find_top_movie(info_cinema, info_ratings):
    """
    Filters each movie by numVotes
    """
    return info_cinema \
        .join(info_ratings, 'tconst', 'inner') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc())


def explode_by_genres(df):
    """
    Explode column by one which contains many strings
    """
    return df.withColumn('genres', explode(split(col('genres'), ',')))


def row_number_window(df, partition, column_order, name_column):
    """
    Makes window row_number
    """
    window = Window.partitionBy(partition).orderBy(col(column_order).desc())
    return df.withColumn(name_column, row_number().over(window))


def filter_top_movie(df):
    """
    Only films from 1949 until now
    """
    return df.where(col('startYear') > 1949)


def make_year_range(df):
    """
    Brakes each films by decade
    """
    decade = (df.startYear - df.startYear % 10).cast('int')
    return df.withColumn('year_range', concat(decade, lit('-'), decade + 10))
