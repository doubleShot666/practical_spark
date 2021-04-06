import pandas as pd
from bokeh.io import show
from flask import Flask, jsonify, make_response, request
from bokeh.layouts import row
from bokeh.models import (Panel)
from bokeh.models.widgets import Tabs

from web_app.MoviesByID import MoviesByID
from web_app.MoviesByTitle import MoviesByTitle
from web_app.MoviesByYear import MoviesByYear
from web_app.MoviesPerGenre import MoviesPerGenre
from web_app.MoviesPerGenreByUser import MoviesPerGenreByUser
from web_app.MoviesWatchedByUsers import MoviesWatchedByUsers
from web_app.TopRatedMovies import TopRatedMovies
from web_app.TopWatchedMovies import TopWatchedMovies


web_app_url = 'http://localhost:5050'


component1 = MoviesPerGenreByUser(web_app_url)
component2 = MoviesWatchedByUsers(web_app_url)
component3 = MoviesByID(web_app_url)
component4 = MoviesByTitle(web_app_url)
component5 = MoviesByYear(web_app_url)
component6 = TopRatedMovies(web_app_url)
component7 = TopWatchedMovies(web_app_url)
component8 = MoviesPerGenre(web_app_url)


class Presenter:
    @staticmethod
    def show():
        tab1 = Panel(child=row([component1.make_plot()]), title="tab one")
        tab2 = Panel(child=row([component2.make_plot()]), title="tab two")
        tab3 = Panel(child=row([component3.make_plot()]), title="tab three")
        tab4 = Panel(child=row([component4.make_plot()]), title="tab four")
        tab5 = Panel(child=row([component5.make_plot()]), title="tab five")
        tab6 = Panel(child=row([component6.make_plot()]), title="tab six")
        tab7 = Panel(child=row([component7.make_plot()]), title="tab seven")
        tab8 = Panel(child=row([component8.make_plot()]), title="tab eight")
        tabs = Tabs(tabs=[tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8])
        show(tabs)


app = Flask(__name__)


def crossdomain(f):
    def wrapped_function(*args, **kwargs):
        resp = make_response(f(*args, **kwargs))
        h = resp.headers
        h['Access-Control-Allow-Origin'] = '*'
        h['Access-Control-Allow-Methods'] = "GET, OPTIONS, POST"
        h['Access-Control-Max-Age'] = str(21600)
        requested_headers = request.headers.get('Access-Control-Request-Headers')
        if requested_headers:
            h['Access-Control-Allow-Headers'] = requested_headers
        return resp

    wrapped_function.__name__ = f.__name__
    return wrapped_function


@app.route('/MoviesPerGenreByUser', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component1():
    return jsonify(data=component1.make_dataset())


@app.route('/MoviesPerGenreByUser/modify', methods=['POST'])
def modify_component1():
    parsed = pd.read_json(request.data)
    component1.update(parsed)
    return jsonify(data=component1.make_dataset())


@app.route('/MoviesWatchedByUsers', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component2():
    return jsonify(data=component2.make_dataset())


@app.route('/MoviesWatchedByUsers/modify', methods=['POST'])
def modify_component2():
    parsed = pd.read_json(request.data)
    component2.update(parsed)
    return jsonify(data=component2.make_dataset())


@app.route('/MoviesByID', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component3():
    return jsonify(data=component3.make_dataset())


@app.route('/MoviesByID/modify', methods=['POST'])
def modify_component3():
    parsed = pd.read_json(request.data)
    component3.update(parsed)
    return jsonify(data=component3.make_dataset())


@app.route('/MoviesByTitle', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component4():
    return jsonify(data=component4.make_dataset())


@app.route('/MoviesByTitle/modify', methods=['POST'])
def modify_component4():
    parsed = pd.read_json(request.data)
    component4.update(parsed)
    return jsonify(data=component4.make_dataset())


@app.route('/MoviesByYear', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component5():
    return jsonify(data=component5.make_dataset())


@app.route('/MoviesByYear/modify', methods=['POST'])
def modify_component5():
    parsed = pd.read_json(request.data)
    component5.update(parsed)
    return jsonify(data=component5.make_dataset())


@app.route('/TopRatedMovies', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component6():
    return jsonify(data=component6.make_dataset())


@app.route('/TopRatedMovies/modify', methods=['POST'])
def modify_component6():
    parsed = pd.read_json(request.data)
    component6.update(parsed)
    return jsonify(data=component6.make_dataset())


@app.route('/TopWatchedMovies', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component7():
    return jsonify(data=component7.make_dataset())


@app.route('/TopWatchedMovies/modify', methods=['POST'])
def modify_component7():
    parsed = pd.read_json(request.data)
    component7.update(parsed)
    return jsonify(data=component7.make_dataset())


@app.route('/MoviesPerGenre', methods=['GET', 'OPTIONS', 'POST'])
@crossdomain
def data_component8():
    return jsonify(data=component8.make_dataset())


@app.route('/MoviesPerGenre/modify', methods=['POST'])
def modify_component8():
    parsed = pd.read_json(request.data)
    component8.update(parsed)
    return jsonify(data=component8.make_dataset())


# show and run
if __name__ == '__main__':
    Presenter.show()
    app.run(port=5050)
