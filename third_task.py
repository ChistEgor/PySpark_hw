from pyspark.sql.functions import col, concat, lit

from data_and_functions import top_100_movie, explode_genre as explode_g, make_window


def filter_top_movie():
    return top_100_movie.where(col('startYear') > 1949)


top_movie = filter_top_movie()

explode_genre = explode_g(top_movie.genres)

year_range_window = make_window('yearRange', 'yearRange')
genre_window = make_window('genre', 'averageRating')


def make_year_range():
    range_small = (top_movie.startYear - top_movie.startYear % 10).cast('int')
    return concat(range_small, lit('-'), range_small)


year_range = make_year_range()


def add_columns():
    return top_movie \
        .withColumn('genre', explode_genre) \
        .withColumn('yearRange', year_range) \
        .withColumn('number_genre', genre_window) \
        .withColumn('number_year_range', year_range_window)


result_table = add_columns()


def find_top_movie_by_year_range():
    return result_table \
        .where(col('number_genre') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes', 'yearRange') \
        .orderBy(col('yearRange').desc(), col('genre').asc(), col('number_genre'))
