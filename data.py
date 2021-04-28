from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .appName('а шо здесь писать') \
    .getOrCreate()


def read_data(file_name):
    return spark.read.csv(f'input_data/{file_name}', sep=r'\t', header=True, inferSchema=True)


info_names = read_data('name.basics.tsv')
info_region = read_data('title.akas.tsv')
info_cinema = read_data('title.basics.tsv')
info_ratings = read_data('title.ratings.tsv')
info_staff = read_data('title.principals.tsv')
info_tv_series = read_data('title.episode.tsv')
info_director_writer = read_data('title.crew.tsv')
