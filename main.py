import first_task, second_task, third_task, fourth_task, fifth_task

from data import write_to_csv

if __name__ == '__main__':
    write_to_csv(first_task.find_top_movie(), '1_1')
    write_to_csv(first_task.find_top_movie_last_10y(), '1_2')
    write_to_csv(first_task.find_top_movie_60s(), '1_3')

    write_to_csv(second_task.find_top_movie_each_genre(), '2')

    write_to_csv(third_task.find_top_movie_by_year_range(), '3')

    write_to_csv(fourth_task.find_top_actors(), '4')

    write_to_csv(fifth_task.find_top_5_by_directors(), '5')
