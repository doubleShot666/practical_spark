from bokeh.models import CustomJS, AjaxDataSource, TableColumn, DataTable


class TopWatchedMovies:
    def __init__(self,web_app_url):
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
        self.__source = AjaxDataSource(data_url=web_app_url + '/TopWatchedMovies',
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