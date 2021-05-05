import tasks
import data
import functions

if __name__ == '__main__':
    functions.write_to_csv(tasks.get_top_movie(data.top_all_time), '1_1')
    functions.write_to_csv(tasks.find_top_movie_last_10y(data.top_all_time), '1_2')
    functions.write_to_csv(tasks.find_top_movie_60s(data.top_all_time), '1_3')

    functions.write_to_csv(tasks.find_top_movie_each_genre(data.top_all_time), '2')

    functions.write_to_csv(tasks.find_top_movie_by_year_range(data.top_from_1950), '3')

    functions.write_to_csv(tasks.find_top_actors(data.top_all_time), '4')

    functions.write_to_csv(tasks.find_top_5_by_directors(data.top_all_time), '5')
