from pyspark.sql.functions import col

from data_and_functions import top_100_movie, explode_genre, make_window

genre_window = make_window('genre', 'averageRating')


def add_columns():
    """
    Addes new columns by that will filtered
    """
    return top_100_movie \
        .withColumn('genre', explode_genre(top_100_movie.genres)) \
        .withColumn('row_number', genre_window)


table_each_genre = add_columns()


def find_top_movie_each_genre():
    """
    Shows up to 10 the top films by genre
    """
    return table_each_genre \
        .where(col('row_number') < 11) \
        .select('tconst', 'primaryTitle', 'startYear', 'genre', 'averageRating', 'numVotes')
