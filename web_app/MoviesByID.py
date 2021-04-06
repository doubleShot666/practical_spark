from bokeh.models import CustomJS, AjaxDataSource, TableColumn, DataTable


class MoviesByID:
    def __init__(self, web_app_url):
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
        self.__source = AjaxDataSource(data_url=web_app_url + '/MoviesByID',
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