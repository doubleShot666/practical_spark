from bokeh.models import CustomJS, AjaxDataSource, TableColumn, DataTable


class TopRatedMovies:
    def __init__(self,web_app_url):
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
        self.__source = AjaxDataSource(data_url=web_app_url + '/TopRatedMovies',
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