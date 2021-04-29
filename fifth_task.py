from pyspark.sql.functions import col

from data_and_functions import make_window, join_table, top_100_movie, info_crew, info_names

window_name = make_window('primaryName', 'averageRating')

table_movie_crew = join_table(top_100_movie, info_crew, top_100_movie.tconst, info_crew.tconst)

table_movie_crew_names = join_table(table_movie_crew, info_names, table_movie_crew.directors, info_names.nconst)

table_movie_crew_names = table_movie_crew_names.withColumn('row_number', window_name)


def find_top_5_by_directors():
    return table_movie_crew_names \
        .where(col('row_number') < 6) \
        .orderBy('directors') \
        .select('primaryName', 'primaryTitle', 'startYear',
                'averageRating', 'numVotes')
