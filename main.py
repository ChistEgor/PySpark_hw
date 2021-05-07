import tasks, functions

if __name__ == '__main__':

    info_names = functions.read_data('name.basics.tsv')
    info_cinema = functions.read_data('title.basics.tsv')
    info_ratings = functions.read_data('title.ratings.tsv')
    info_staff = functions.read_data('title.principals.tsv')
    info_crew = functions.read_data('title.crew.tsv')

    # For 1-5
    top_all_time = functions.find_top_movie(info_cinema, info_ratings)

    functions.write_to_csv(tasks.get_top_movie(top_all_time), '1_1')
    functions.write_to_csv(tasks.find_top_movie_last_10y(top_all_time), '1_2')
    functions.write_to_csv(tasks.find_top_movie_60s(top_all_time), '1_3')

    functions.write_to_csv(tasks.find_top_movie_each_genre(top_all_time), '2')

    functions.write_to_csv(tasks.find_top_movie_by_year_range(top_all_time), '3')

    functions.write_to_csv(tasks.find_top_actors(top_all_time), '4')

    functions.write_to_csv(tasks.find_top_5_by_directors(top_all_time), '5')
