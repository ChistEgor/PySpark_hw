import functions

info_names = functions.read_data('name.basics.tsv')
info_cinema = functions.read_data('title.basics.tsv')
info_ratings = functions.read_data('title.ratings.tsv')
info_staff = functions.read_data('title.principals.tsv')
info_crew = functions.read_data('title.crew.tsv')

# For 1-5
top_all_time = functions.find_top_movie(info_cinema, info_ratings)
# For 3
top_from_1950 = functions.filter_top_movie(top_all_time)
