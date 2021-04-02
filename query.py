from __future__ import print_function, unicode_literals
from PyInquirer import style_from_dict, Token, prompt
import os
import sys

from spark import SparkQuery

style = style_from_dict({
    Token.Separator: '#cc5454',
    Token.QuestionMark: '#673ab7 bold',
    Token.Selected: '#cc5454',  # default
    Token.Pointer: '#673ab7 bold',
    Token.Instruction: '',  # default
    Token.Answer: '#f44336 bold',
    Token.Question: '',
})


#remeber to decomment
sparkquery = SparkQuery()


def input_datapath():
    print("---------------------------------")
    print("To use this system, please input the directory path where you store the data(movies.csv and ratings.csv) first ")
    print("For example: E:\PycharmProject\spark_practical")
    print("---------------------------------")
    datapath = input("Please input data path: ")
    #datapath = "E:\CS5052\Practicals\ml-latest-small"
    print("---------------------------------")
    return datapath


path = input_datapath()
sparkquery.init_dataframe(path)

def main_menu_part2():
    repeat = True
    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'main_menu_part2',
            'message': 'Welcome to Part 2: genre and movie taste analysis!',
            'choices': ['Continue', 'Exit']
        }
        answers = prompt(prompt_text)
        if answers['main_menu_part2'] == 'Continue':
            select_option()
        else:
            repeat = False
    return 0

def select_option():
    repeat = True

    while repeat:
        prompt_text = {
            'type': 'list',
            'name': 'options',
            'message': 'Please choose the function you want to run',
            'choices': ['Find the favourite genre of a user',
                        'Find the favourite genre of users',
                        'Compare the movie tastes of two users',
                        'Return']
        }
        answers = prompt(prompt_text)
        if answers['options'] == 'Find the favourite genre of a user':
            genre_user()
        elif answers['options'] == 'Find the favourite genre of users':
            genre_group_users()
        elif answers['options'] == 'Compare the movie tastes of two users':
            compare_taste()
        elif answers['options'] == 'Return':
            repeat = False
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
    sparkquery.favorite_genre_user(uid, f)
    return uid


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
    sparkquery.favorite_genre_usersgroup(uids, f)
    return uids


def compare_taste():
    user_ids = prompt({
        'type': 'input',
        'name': 'user_id',
        'message': 'Enter the list of user IDs separated by a comma',
    })['user_id']
    uids = [int(i) for i in user_ids.split(',')]
    sparkquery.compare_taste(uids)
    return uids


if __name__ == '__main__':
    # test
    #genre_user()
    #genre_group_users()
    #compare_taste()

    main_menu_part2()
