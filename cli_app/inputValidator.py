from __future__ import print_function, unicode_literals
from prompt_toolkit.validation import Validator, ValidationError
import re
from os import path


# This validator checks if the enter value corresponds to a valid year
class YearValidator(Validator):
    def validate(self, document):
        match = re.match(r'^[1-2][0-9][0-9][0-9]$', document.text)
        if match is None:
            raise ValidationError(
                message="Please enter a valid year",
                cursor_position=len(document.text)
            )


class IntValidator(Validator):
    def validate(self, document):
        try:
            val = int(document.text)
        except ValueError:
            raise ValidationError(
                message="Please enter a valid value",
                cursor_position=len(document.text)
            )


class IdValidator(Validator):
    def validate(self, document):
        try:
            id = int(document.text)
        except ValueError:
            raise ValidationError(
                message="Please enter a valid id value",
                cursor_position=len(document.text)
            )


class IdListValidator(Validator):
    def validate(self, document):
        try:
            ids = [int(i) for i in document.text.split(',')]
        except ValueError:
            raise ValidationError(
                message="Please enter a valid list of ids",
                cursor_position=len(document.text)
            )


class DatasetPathValidator(Validator):
    def validate(self, document):
        dataset_path = document.text + "\\"
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
