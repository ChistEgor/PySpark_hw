import first_task
import second_task
import third_task
import fourth_task
import fifth_task

from data_and_functions import write_to_csv, top_100_movie

if __name__ == '__main__':
    write_to_csv(first_task.find_top_movie(top_100_movie), '1_1')
    write_to_csv(first_task.find_top_movie_last_10y(top_100_movie), '1_2')
    write_to_csv(first_task.find_top_movie_60s(top_100_movie), '1_3')

    write_to_csv(second_task.find_top_movie_each_genre(), '2')

    write_to_csv(third_task.find_top_movie_by_year_range(), '3')

    write_to_csv(fourth_task.find_top_actors(), '4')

    write_to_csv(fifth_task.find_top_5_by_directors(), '5')
