from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, row_number, explode
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[12]") \
        .appName("main") \
        .getOrCreate()

    """
    The First Task'a
    """

    df_tb = spark.read.csv('input_data/title.basics.tsv', sep=r'\t', header=True)
    df_tr = spark.read.csv('input_data/title.ratings.tsv', sep=r'\t', header=True)

    df_tb_tr = df_tb.join(df_tr, 'tconst', 'inner')

    all_top_movie = df_tb_tr \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc()).limit(100)
    all_top_movie.write.mode('overwrite').save('output_data/1_1', format='csv')

    top_movie_last_10y = all_top_movie.where(col('startYear') > 2000)
    top_movie_last_10y.write.mode('overwrite').save('output_data/1_2', format='csv')

    top_movie_in_60s = all_top_movie.where(col('startYear').between('1960', '1969'))
    top_movie_in_60s.write.mode('overwrite').save('output_data/1_3', format='csv')

    """
    The Second Task'a
    """

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
