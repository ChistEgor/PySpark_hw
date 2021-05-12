import tasks
from functions import read_data, find_top_movie, write_to_csv

if __name__ == '__main__':
    info_names = read_data('name.basics.tsv')
    info_cinema = read_data('title.basics.tsv')
    info_ratings = read_data('title.ratings.tsv')
    info_staff = read_data('title.principals.tsv')
    info_crew = read_data('title.crew.tsv')

    # For 1-5
    top_all_time = find_top_movie(info_cinema, info_ratings)

    write_to_csv(tasks.get_top_movie(top_all_time), '1_1')
    write_to_csv(tasks.find_top_movie_last_10y(top_all_time), '1_2')
    write_to_csv(tasks.find_top_movie_60s(top_all_time), '1_3')

    write_to_csv(tasks.find_top_movie_each_genre(top_all_time), '2')

    write_to_csv(tasks.find_top_movie_by_year_range(top_all_time), '3')

    write_to_csv(tasks.find_top_actors(top_all_time, info_staff, info_names), '4')

    write_to_csv(tasks.find_top_5_by_directors(top_all_time, info_crew, info_names), '5')
