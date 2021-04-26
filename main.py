import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, row_number
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[12]") \
        .appName("main") \
        .getOrCreate()

    data_path = os.getcwd() + '/input_data/'

    df_nb = spark.read.csv(data_path + 'name.basics.tsv', sep=r'\t', header=True)
    df_ta = spark.read.csv(data_path + 'title.akas.tsv', sep=r'\t', header=True)
    df_tb = spark.read.csv(data_path + 'title.basics.tsv', sep=r'\t', header=True)
    df_tc = spark.read.csv(data_path + 'title.crew.tsv', sep=r'\t', header=True)
    df_te = spark.read.csv(data_path + 'title.episode.tsv', sep=r'\t', header=True)
    df_tp = spark.read.csv(data_path + 'title.principals.tsv', sep=r'\t', header=True)
    df_tr = spark.read.csv(data_path + 'title.ratings.tsv', sep=r'\t', header=True)

    """
    The First Task'a
    """
    df_tb_tr = df_tb.join(df_tr, 'tconst', 'inner')

    df_tb_tr_all_top_movie = df_tb_tr \
        .select('tconst', 'primaryTitle', 'numVotes', 'averageRating', 'startYear') \
        .where((col('titleType') == 'movie') & (col('numVotes') >= 100000)) \
        .orderBy(col('averageRating').desc()).limit(100)
    df_tb_tr_all_top_movie.write.save('1_1', format='csv')

    df_tb_tr_top_movie_last_10y = df_tb_tr_all_top_movie \
        .where(col('startYear') > 2000)
    df_tb_tr_top_movie_last_10y.write.save('1_2', format='csv')

    df_tb_tr_top_movie_in_60s = df_tb_tr_all_top_movie \
        .where(col('startYear').between('1960', '1969'))
    df_tb_tr_top_movie_in_60s.write.save('1_3', format='csv')

"""
"""