from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, row_number, explode, floor, concat, lit
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('main') \
        .getOrCreate()

    # The First Task'a

    df_tb = spark.read.csv('input_data/title.basics.tsv', sep=r'\t', header=True, inferSchema=True)
    df_tr = spark.read.csv('input_data/title.ratings.tsv', sep=r'\t', header=True, inferSchema=True)

    df_tb_tr = df_tb.join(df_tr, 'tconst', 'inner')

    all_top_movie = df_tb_tr \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc()).limit(100)
    all_top_movie.write.mode('overwrite').save('output_data/1_1', format='csv')

    top_movie_last_10y = all_top_movie.where(col('startYear') > 2000)  # TODO change -10
    top_movie_last_10y.write.mode('overwrite').save('output_data/1_2', format='csv')

    top_movie_in_60s = all_top_movie.where(col('startYear').between('1960', '1969'))
    top_movie_in_60s.write.mode('overwrite').save('output_data/1_3', format='csv')

    # The Second Task'a

    genre_explode = explode(split(df_tb_tr.genres, ',')).alias('genre')

    all_top_movie = df_tb_tr \
        .select('tconst', 'primaryTitle', 'numVotes', genre_explode, 'averageRating', 'startYear') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000))

    top_movie_each_genre = all_top_movie \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes',
                row_number().over(Window.partitionBy('genre').orderBy(col('averageRating').desc())).alias(
                    'row_number'))

    top_10_each_genre = top_movie_each_genre \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes') \
        .where(col('row_number') < 11)

    top_10_each_genre.coalesce(1).write.mode('overwrite').save('output_data/2', format='csv')

    # The Third Task'a
    top_movie_year_range = all_top_movie.where(col('startYear') > 1949)

    top_movie_year_range = top_movie_year_range \
        .withColumn('RangeSmall', (floor(col('startYear') / 10) * 10)) \
        .withColumn('RangeBig', (floor(col('startYear') / 10) * 10) + 10) \
        .withColumn('yearRange', concat(col('RangeSmall'), lit('-'), col('RangeBig')))

    window_genre = row_number() \
        .over(Window.partitionBy('genre').orderBy(col('averageRating').desc(), col('numVotes').desc()))

    window_year_range = row_number() \
        .over(Window.partitionBy('yearRange').orderBy(col('yearRange').desc()))

    top_movie_year_range = top_movie_year_range \
        .withColumn('number_genre', window_genre) \
        .withColumn('number_year_range', window_year_range)

    top_movie_year_range = top_movie_year_range \
        .where(col('number_genre') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes', 'yearRange') \
        .orderBy(col('yearRange').desc(), col('genre').desc(), col('number_genre'))

    # top_movie_year_range.printSchema()
    # top_movie_year_range.show(200)

    # 4 task

    df_nb = spark.read.csv('input_data/name.basics.tsv', sep=r'\t', header=True, inferSchema=True)
    df_tp = spark.read.csv('input_data/title.principals.tsv', sep=r'\t', header=True, inferSchema=True)

