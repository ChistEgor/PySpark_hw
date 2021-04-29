from pyspark.sql.functions import col

from data_and_functions import join_table, top_100_movie, info_names, info_staff

table_movie_staff = join_table(top_100_movie, info_staff, top_100_movie.tconst, info_staff.tconst)

table_movie_staff_names = join_table(table_movie_staff, info_names, table_movie_staff.nconst, info_names.nconst) \
    .drop(info_names.nconst)


def find_top_actors():
    """
    Shows the best actors which have acted more than 1 time
    """
    return table_movie_staff_names \
        .where(col('category').like('%act%')) \
        .groupby('nconst', 'primaryName').count() \
        .orderBy(col('count').desc()) \
        .select('primaryName')
