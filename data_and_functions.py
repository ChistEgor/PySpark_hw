from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, row_number
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


info_names = read_data('name.basics.tsv')
info_cinema = read_data('title.basics.tsv')
info_ratings = read_data('title.ratings.tsv')
info_staff = read_data('title.principals.tsv')
info_crew = read_data('title.crew.tsv')


def write_to_csv(data, file):
    """
    Makes one dataframe from group of and saves to file
    """
    return data.coalesce(1).write.mode('overwrite').save(f'output_data/{file}', format='csv')


def find_top_movie():
    """
    Filters each movie and return the top 100
    """
    return info_cinema \
        .join(info_ratings, 'tconst', 'inner') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc()).limit(100)


top_100_movie = find_top_movie()


def explode_genre(column):
    """
    Explode column by one which contains many strings
    """
    return explode(split(column, ','))


def make_window(part, ord):
    """
    Window function implementation
    """
    return row_number().over(Window.partitionBy(part).orderBy(col(ord).desc()))


def join_table(first, second, first_predicate, second_predicate, how='inner'):
    """
    Join implementation
    """
    return first.join(second, first_predicate == second_predicate, how)
