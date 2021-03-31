from __future__ import print_function, unicode_literals

from PyInquirer import style_from_dict, Token, prompt, Separator
from pprint import pprint
from prompt_toolkit.validation import Validator, ValidationError
from examples import custom_style_2
from core.SparkInterface import SparkInterface

import os.path
from os import path

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


class DatasetValidator(Validator):
    def validate(self, document):
        dataset_path = document.text
        if not path.isfile(dataset_path + 'links.csv'):
            raise ValidationError(
                message="Please enter a valid path to your dataset : links.csv not found",
                cursor_position=len(document.text)
            )
        elif not path.isfile(dataset_path + 'movies.csv'):
            raise ValidationError(
                message="Please enter a valid path to your dataset : movies.csv not found",
                cursor_position=len(document.text)
            )
        elif not path.isfile(dataset_path + 'ratings.csv'):
            raise ValidationError(
                message="Please enter a valid path to your dataset : ratings.csv not found",
                cursor_position=len(document.text)
            )
        elif not path.isfile(dataset_path + 'tags.csv'):
            raise ValidationError(
                message="Please enter a valid path to your dataset : tags.csv not found",
                cursor_position=len(document.text)
            )


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
        'validate': DatasetValidator
    }
    answers = prompt(prompt_text)
    spark.read_dataset(answers['dataset'])
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
                        'Return']
        }
        answers = prompt(prompt_text)
        if answers['features'] == 'Search for users':
            search_users()
        elif answers['features'] == 'Search for movies':
            search_movies()
        elif answers['features'] == 'Search for genres':
            search_genres()
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
                'message': 'Enter the user ID '
            })['user_id']
            user_ids = [int(user_id)]
            # core call to search for users
            spark.movies_watched_by_users(user_ids).show()
        elif answers['search_users'] == 'By a list of IDs':
            user_ids = prompt({
                'type': 'input',
                'name': 'user_id',
                'message': 'Enter the list of IDs separated by a comma '})['user_id']
            user_ids = [int(i) for i in user_ids.split(',')]
            # core call to search for users
            spark.movies_watched_by_users(user_ids).show()
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
                'message': 'Enter the movie ID '
            })['movie_id']
            movie_id = int(movie_id)
            # core call
            spark.search_movie_by_id(movie_id).show()
        elif answers['search_movies'] == 'By title':
            movie_title = prompt({
                'type': 'input',
                'name': 'movie_title',
                'message': 'Enter the movie title '
            })['movie_title']
            # core call
            spark.search_movie_by_title(movie_title).show()
        elif answers['search_movies'] == 'By year':
            movie_year = prompt({
                'type': 'input',
                'name': 'movie_year',
                'message': 'Enter the movie year '
            })['movie_year']
            movie_year = int(movie_year)
            # core call
            spark.search_movies_by_year(movie_year).show()

        elif answers['search_movies'] == 'Top n movies by rating':
            list_length = prompt({
                'type': 'input',
                'name': 'list_length',
                'message': 'Enter n value '
            })['list_length']
            list_length = int(list_length)
            # core call
            spark.top_rating_movies(list_length, 'desc').show()
        elif answers['search_movies'] == 'top n movies by number of watches':
            list_length = prompt({
                'type': 'input',
                'name': 'list_length',
                'message': 'Enter n value '
            })['list_length']
            list_length = int(list_length)
            # core call
            spark.top_watched_movies(list_length, 'desc').show()
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
    #call core
    spark.search_movies_by_genres(genres).show()
    return 0


if __name__ == '__main__':
    main_menu()
