from __future__ import print_function, unicode_literals
from PyInquirer import style_from_dict, Token, prompt
from cli_app.inputValidator import *
import requests
from core.SparkInterface import SparkInterface

# This app displays on a terminal the features from the requirements in parts 1 and 2 of the practical
# After entering the inputs required for a chosen feature, a call to the corresponding function in SparkInterface
# is made. The results are either displayed on the terminal or sent via http as a POST request to the web app.


# Style used by PyInquirer
style = style_from_dict({
    Token.Separator: '#cc5454',
    Token.QuestionMark: '#673ab7 bold',
    Token.Selected: '#cc5454',  # default
    Token.Pointer: '#673ab7 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#f44336 bold',
    Token.Question: '',
})


spark = SparkInterface()

# The root url of the web app
web_app_url = 'http://localhost:5050'


def main_menu():
    repeat = True
    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'main_menu',
            'message': 'Welcome to PysparkMovieLens !',
            'choices': ['Read a dataset', 'Exit']
        }
        answers = prompt(prompt_text)
        if answers['main_menu'] == 'Read a dataset':
            read_dataset()
        else:
            repeat = False
    return 0


def read_dataset():
    prompt_text = {
        'type': 'input',
        'name': 'dataset',
        'message': 'Enter the path to your dataset files',
        'validate': DatasetPathValidator
    }
    answers = prompt(prompt_text)
    spark.read_dataset(answers['dataset']+'\\')
    select_feature()


def select_feature():
    repeat = True
    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'features',
            'message': 'This is the list of features offered to you, please pick one',
            'choices': ['Search for users',
                        'Search for movies',
                        'Search for genres',
                        'Find the favourite genre of a user',
                        'Find the favourite genre of users',
                        'Compare the movie tastes of two users',
                        'Return']
        }
        answers = prompt(prompt_text)
        if answers['features'] == 'Search for users':
            search_users()
        elif answers['features'] == 'Search for movies':
            search_movies()
        elif answers['features'] == 'Search for genres':
            search_genres()
        elif answers['features'] == 'Find the favourite genre of a user':
            genre_user()
        elif answers['features'] == 'Find the favourite genre of users':
            genre_group_users()
        elif answers['features'] == 'Compare the movie tastes of two users':
            compare_taste()
        elif answers['features'] == 'Return':
            repeat = False

    return 0


def search_users():
    repeat = True
    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'search_users',
            'message': 'Search users',
            'choices': ['By ID', 'By a list of IDs', 'Return']
        }
        answers = prompt(prompt_text)
        if answers['search_users'] == 'By ID':
            user_id = prompt({
                'type': 'input',
                'name': 'user_id',
                'message': 'Enter the user ID ',
                'validate': IdValidator
            })['user_id']
            # core call to search for users
            requests.post(url= web_app_url + '/MoviesPerGenreByUser/modify',
                          data=spark.movies_per_genre_watched_by_user(int(user_id)).toPandas().to_json())
        elif answers['search_users'] == 'By a list of IDs':
            user_ids = prompt({
                'type': 'input',
                'name': 'user_id',
                'message': 'Enter the list of IDs separated by a comma ',
                'validate': IdListValidator
            })['user_id']
            user_ids = [int(i) for i in user_ids.split(',')]
            # core call to search for users
            requests.post(url= web_app_url + '/MoviesWatchedByUsers/modify',
                          data=spark.movies_watched_by_users(user_ids).toPandas().to_json())
        elif answers['search_users'] == 'Return':
            repeat = False

    return 0


def search_movies():
    repeat = True
    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'search_movies',
            'message': 'Search movies',
            'choices': ['By ID',
                        'By title',
                        'By year',
                        'Top n movies by rating',
                        'top n movies by number of watches',
                        'Return']
        }
        answers = prompt(prompt_text)
        if answers['search_movies'] == 'By ID':
            movie_id = prompt({
                'type': 'input',
                'name': 'movie_id',
                'message': 'Enter the movie ID ',
                'validate': IdValidator
            })['movie_id']
            movie_id = int(movie_id)
            # core call
            requests.post(url=web_app_url + '/MoviesByID/modify',
                          data=spark.search_movie_by_id(movie_id).toPandas().to_json())
        elif answers['search_movies'] == 'By title':
            movie_title = prompt({
                'type': 'input',
                'name': 'movie_title',
                'message': 'Enter the movie title '
            })['movie_title']
            # core call
            requests.post(url= web_app_url + '/MoviesByTitle/modify',
                          data=spark.search_movie_by_title(movie_title).toPandas().to_json())
        elif answers['search_movies'] == 'By year':
            movie_year = prompt({
                'type': 'input',
                'name': 'movie_year',
                'message': 'Enter the movie year ',
                'validate': YearValidator
            })['movie_year']
            movie_year = int(movie_year)
            # core call
            requests.post(url=web_app_url + '/MoviesByYear/modify',
                          data=spark.search_movies_by_year(movie_year).toPandas().to_json())
        elif answers['search_movies'] == 'Top n movies by rating':
            list_length = prompt({
                'type': 'input',
                'name': 'list_length',
                'message': 'Enter n value ',
                'validate': IntValidator
            })['list_length']
            list_length = int(list_length)
            # core call
            requests.post(url=web_app_url + '/TopRatedMovies/modify',
                          data=spark.top_rating_movies(list_length, 'desc').toPandas().to_json())
        elif answers['search_movies'] == 'top n movies by number of watches':
            list_length = prompt({
                'type': 'input',
                'name': 'list_length',
                'message': 'Enter n value ',
                'validate': IntValidator
            })['list_length']
            list_length = int(list_length)
            # core call
            requests.post(url=web_app_url + '/TopWatchedMovies/modify',
                          data=spark.top_watched_movies(list_length, 'desc').toPandas().to_json())
        elif answers['search_movies'] == 'Return':
            repeat = False
    return 0


def search_genres():
    genres_list = spark.get_genres_list()
    choices = [{'name': genre} for genre in genres_list]
    genres = prompt({
        'type': 'checkbox',
        'name': 'genres',
        'message': 'Select genres',
        'choices': choices
    })['genres']
    # call core
    requests.post(url= web_app_url + '/MoviesPerGenre/modify',
                  data=spark.search_movies_by_genres(genres).toPandas().to_json())
    return 0


def genre_user():
    user_id = prompt({
        'type': 'input',
        'name': 'user_id',
        'message': 'Enter the user ID ',
    })['user_id']
    uid = int(user_id)
    factor = prompt({
        'type': 'input',
        'name': 'factor',
        'message': 'Formula used: count * factor + average_rating * ( 1 - factor). Enter the factor ',
    })['factor']
    f = float(factor)
    tastes = spark.favorite_genre_user(uid, f)

    favourite_taste = tastes[-1:]['genres'].values[0]

    print("---------------------------------")
    print("The favourite genre of this user is:", favourite_taste)
    print("---------------------------------")
    print("The following table gives the whole analysis information about user's taste:")
    print("Description: count - the number of specific genre movies watched. genres - the movie genre. "
          "avg_rating - average rating of this user to this genre's movie. "
          "genre_score - the calculate formula is [count * factor + average_rating * ( 1 - factor)]")
    print("---------------------------------")
    print(tastes)
    print("---------------------------------")

    return 0


def genre_group_users():
    user_ids = prompt({
        'type': 'input',
        'name': 'user_id',
        'message': 'Enter the list of user IDs separated by a comma',
    })['user_id']
    uids = [int(i) for i in user_ids.split(',')]
    factor = prompt({
        'type': 'input',
        'name': 'factor',
        'message': 'Formula used: count * factor + average_rating * ( 1 - factor). Enter the factor ',
    })['factor']
    f = float(factor)
    tastes = spark.favorite_genre_usersgroup(uids, f)

    favourite_taste = tastes[-1:]['genres'].values[0]
    print("---------------------------------")
    print("The favourite genre of this group of users is:", favourite_taste)
    print("---------------------------------")
    print("The following table gives the whole analysis information about users' taste:")
    print("Description: count - the number of specific genre movies watched. genres - the movie genre. "
          "avg_rating - average rating of  user group to this genre's movie. "
          "genre_score - the calculate formula is [count * factor + average_rating * ( 1 - factor)]")
    print("---------------------------------")
    print(tastes)
    print("---------------------------------")

    return 0


def compare_taste():
    user_ids = prompt({
        'type': 'input',
        'name': 'user_id',
        'message': 'Enter the list of user IDs separated by a comma',
    })['user_id']
    uids = [int(i) for i in user_ids.split(',')]
    common_movie_pandasdf = spark.compare_taste(uids)

    num = len(common_movie_pandasdf)

    print("Description: Movie taste is compared in two aspects. 1. Number of common watched movie by two users. "
          "2. similarity score out of 10 based on ratings")
    print("---------------------------------")
    print("Number of common watched movies is :", num)
    print("---------------------------------")
    print("Table of common watched movies:")
    print(common_movie_pandasdf)
    print("---------------------------------")

    spark.similarity_score(common_movie_pandasdf)

    return uids


if __name__ == '__main__':
    main_menu()
