from bokeh.models import CustomJS, AjaxDataSource, HoverTool
from bokeh.plotting import figure

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


class MoviesPerGenreByUser:
    def __init__(self,web_app_url):
        self.dataset = dict(genres=list(), count=list())
        self.__adapter = CustomJS(code="""
            const result = {genres: [], count: []}
            const {data} = cb_data.response
            result.genres = data.genres
            result.count = data.count
            return result
        """)
        self.__source = AjaxDataSource(data_url=web_app_url + '/MoviesPerGenreByUser',
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