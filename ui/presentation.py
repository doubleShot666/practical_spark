import pandas as pd
from bokeh.plotting import figure
from bokeh.io import show, output_file
from flask import Flask, jsonify, make_response, request
from bokeh.layouts import column, row
from bokeh.models import (Button, ColumnDataSource, CustomJS, DataTable, Text,
                          NumberFormatter, RangeSlider, TableColumn, Panel, HoverTool, AjaxDataSource, Toolbar, Select)
from bokeh.models.widgets import Tabs


# styling a plot
def style(p):
    # Title
    p.title.align = 'center'
    p.title.text_font_size = '20pt'
    p.title.text_font = 'serif'

    # Axis titles
    p.xaxis.axis_label_text_font_size = '14pt'
    p.xaxis.axis_label_text_font_style = 'bold'
    p.yaxis.axis_label_text_font_size = '14pt'
    p.yaxis.axis_label_text_font_style = 'bold'

    # Tick labels
    p.xaxis.major_label_text_font_size = '12pt'
    p.yaxis.major_label_text_font_size = '12pt'

    return p


class MoviesWatchedByUsers:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {users: [], movies: []}
            const {data} = cb_data.response
            result.users = data.users
            result.movies = data.movies
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesWatchedByUsers',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="users", title="User ID"),
            TableColumn(field="movies", title="Movie"),
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['users'] = list(new_datset['userId'])
        self.dataset['movies'] = list(new_datset['title'])


class MoviesPerGenreByUser:
    def __init__(self):
        self.dataset = dict(genres=list(), count=list())
        self.__adapter = CustomJS(code="""
            const result = {genres: [], count: []}
            const {data} = cb_data.response
            result.genres = data.genres
            result.count = data.count
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesPerGenreByUser',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = list(self.dataset.keys())
        p = figure(x_range=list(
            ['Adventure', 'Animation', 'Children', 'Comedy', 'Fantasy',
             'Romance', 'Action', 'Crime', 'Thriller', 'Mystery', 'Horror',
             'Drama', 'War', 'Western', 'Sci-Fi', 'Musical', 'Film-Noir',
             'IMAX', 'Documentary']),
            y_range=(0, 100), plot_height=600, plot_width=1000,
            title=" The number of movies/genre that a user has watched",
            x_axis_label='Genre', y_axis_label='Nb. of movies')
        # Quad glyphs to create a histogram
        p.vbar(x=columns[0], top=columns[1], width=0.5, source=self.__source,
               line_color='black', fill_color='navy')

        hover = HoverTool(tooltips=[('Count', '@count')])

        p.xaxis.major_label_orientation = 1
        p.add_tools(hover)
        p = style(p)
        return p

    def make_dataset(self):
        return self.dataset

    def update(self, new_dataset):
        self.dataset['genres'] = list(new_dataset['genres'])
        self.dataset['count'] = list(new_dataset['count'])


class MoviesByID:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {ids: [], titles: [], avgs: [], watches:[]}
            const {data} = cb_data.response
            result.ids = data.ids
            result.titles = data.titles
            result.avgs = data.avgs
            result.watches = data.watches
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesByID',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="ids", title="Movie ID"),
            TableColumn(field="titles", title="Title"),
            TableColumn(field="avgs", title="average rating"),
            TableColumn(field="watches", title="number of watches")
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['ids'] = list(new_datset['movieId'])
        self.dataset['titles'] = list(new_datset['title'])
        self.dataset['avgs'] = list(new_datset['avg'])
        self.dataset['watches'] = list(new_datset['nb_users'])


class MoviesByTitle:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {ids: [], titles: [], avgs: [], watches:[]}
            const {data} = cb_data.response
            result.ids = data.ids
            result.titles = data.titles
            result.avgs = data.avgs
            result.watches = data.watches
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesByTitle',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="ids", title="Movie ID"),
            TableColumn(field="titles", title="Title"),
            TableColumn(field="avgs", title="average rating"),
            TableColumn(field="watches", title="number of watches")
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['ids'] = list(new_datset['movieId'])
        self.dataset['titles'] = list(new_datset['title'])
        self.dataset['avgs'] = list(new_datset['avg'])
        self.dataset['watches'] = list(new_datset['nb_users'])


class MoviesByYear:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {year: [], ids: [], titles: []}
            const {data} = cb_data.response
            result.ids = data.ids
            result.titles = data.titles
            result.year = data.year
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesByYear',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="year", title="Year"),
            TableColumn(field="ids", title="Movie ID"),
            TableColumn(field="titles", title="Title")
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['ids'] = list(new_datset['movieId'])
        self.dataset['titles'] = list(new_datset['title'])
        self.dataset['year'] = list(new_datset['year'])


class MoviesPerGenre:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {genre: [], movieId: [], title: []}
            const {data} = cb_data.response
            result.genre = data.genre
            result.movieId = data.movieId
            result.title = data.title
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/MoviesPerGenre',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="genre", title="Genre"),
            TableColumn(field="movieId", title="Movie ID"),
            TableColumn(field="title", title="Title")
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['genre'] = list(new_datset['genres'])
        self.dataset['movieId'] = list(new_datset['movieId'])
        self.dataset['title'] = list(new_datset['title'])


class TopRatedMovies:
    def __init__(self):
        self.dataset = dict()
        self.__adapter = CustomJS(code="""
            const result = {genre: [], movieId: [], title: [], avg: []}
            const {data} = cb_data.response
            result.genre = data.genre
            result.movieId = data.movieId
            result.title = data.title
            result.avg = data.avg
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/TopRatedMovies',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="movieId", title="Movie ID"),
            TableColumn(field="title", title="Title"),
            TableColumn(field="genre", title="Genre"),
            TableColumn(field="avg", title="Average rate"),
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['avg'] = list(new_datset['avg'])
        self.dataset['genre'] = list(new_datset['genres'])
        self.dataset['movieId'] = list(new_datset['movieId'])
        self.dataset['title'] = list(new_datset['title'])


class TopWatchedMovies:
    def __init__(self):
        self.dataset = dict(users=list(), movies=list())
        self.__adapter = CustomJS(code="""
            const result = {genre: [], movieId: [], title: [], watches: []}
            const {data} = cb_data.response
            result.genre = data.genre
            result.movieId = data.movieId
            result.title = data.title
            result.watches = data.watches
            return result
        """)
        self.__source = AjaxDataSource(data_url='http://localhost:5050/TopWatchedMovies',
                                       polling_interval=1000, adapter=self.__adapter)

    def make_plot(self):
        columns = [
            TableColumn(field="movieId", title="Movie ID"),
            TableColumn(field="title", title="Title"),
            TableColumn(field="genre", title="Genre"),
            TableColumn(field="watches", title="Watches"),
        ]
        data_table = DataTable(source=self.__source, columns=columns, width=800)
        return data_table

    def make_dataset(self):
        return self.dataset

    def update(self, new_datset):
        self.dataset['watches'] = list(new_datset['nb_users'])
        self.dataset['genre'] = list(new_datset['genres'])
        self.dataset['movieId'] = list(new_datset['movieId'])
        self.dataset['title'] = list(new_datset['title'])


component1 = MoviesPerGenreByUser()
component2 = MoviesWatchedByUsers()
component3 = MoviesByID()
component4 = MoviesByTitle()
component5 = MoviesByYear()
component6 = TopRatedMovies()
component7 = TopWatchedMovies()
component8 = MoviesPerGenre()


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
Presenter.show()
app.run(port=5050)
