from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import *


class SparkInterface:
    def __init__(self):
        self.__spark = SparkSession.builder.appName("Practical Spark").getOrCreate()
        self.__movies_df = None
        self.__tags_df = None
        self.__links_df = None
        self.__ratings_df = None
        self.__movies_df_split_genres = None
        self.__movies_df_with_year_col = None
        self.__reduced_ratings = None
        self.__reduced_tags = None
        self.__movies_user_df = None

    def read_dataset(self, path):
        # Manually create schema to avoid possible problems when inferring schema and to gain time
        links_schema = StructType([StructField('movieId', IntegerType(), True),
                                   StructField('imdbId', IntegerType(), True),
                                   StructField('rmdbId', IntegerType(), True)])

        movies_schema = StructType([StructField('movieId', IntegerType(), True),
                                    StructField('title', StringType(), True),
                                    StructField('genres', StringType(), True)])

        ratings_schema = StructType([StructField('userId', IntegerType(), True),
                                     StructField('movieId', IntegerType(), True),
                                     StructField('rating', FloatType(), True),
                                     StructField('timestamp', IntegerType(), True)])

        tags_schema = StructType([StructField('userId', IntegerType(), True),
                                  StructField('movieId', IntegerType(), True),
                                  StructField('tag', StringType(), True),
                                  StructField('timestamp', IntegerType(), True)])
        # Read csv file
        self.__links_df = self.__spark.read.csv(path + 'links.csv', header=True, schema=links_schema)
        self.__tags_df = self.__spark.read.csv(path + 'tags.csv', header=True, schema=tags_schema)
        self.__movies_df = self.__spark.read.csv(path + 'movies.csv', header=True, schema=movies_schema)
        self.__ratings_df = self.__spark.read.csv(path + 'ratings.csv', header=True, schema=ratings_schema)
        self.prepare_dataset()

    def prepare_dataset(self):
        self.__movies_df_split_genres = self.__movies_df.withColumn('genres',
                                                                    explode(split(self.__movies_df.genres, "\\|")))
        self.__movies_df_with_year_col = self.__movies_df \
            .withColumn('year',
                        regexp_extract(self.__movies_df['title'], '[1-2][0-9][0-9][0-9]', 0).cast(IntegerType())) \
            .withColumn('title', split(self.__movies_df['title'], '\([1-2][0-9][0-9][0-9]\)').getItem(0))

        self.__reduced_ratings = self.__ratings_df.select(col("userId"), col("movieId")).distinct()
        self.__reduced_tags = self.__tags_df.select(col("userId"), col("movieId")).distinct()
        self.__movies_user_df = self.__reduced_ratings.union(self.__reduced_tags).distinct()

    def movies_watched_by_users(self, user_ids):
        movies_user_info_df = self.__movies_user_df.join(self.__movies_df_with_year_col,
                                                         self.__movies_user_df.movieId == self.__movies_df_with_year_col.movieId,
                                                         how='inner')

        # All movies watched by each user of a given list of users
        results = movies_user_info_df.select(col("userId"), col("title")) \
            .distinct() \
            .filter(col("userId").isin(user_ids)) \
            .orderBy(col("userId"))
        return results

    def avg_rating_per_movie(self):
        return self.__ratings_df.groupby(col("movieId")).agg(avg('rating').alias('avg'))

    def count_user_per_movie(self):
        return self.__movies_user_df.groupby(col("movieId")).agg(count("userId").alias('nb_users'))

    def search_movie_by_id(self, movie_id):
        return self.__movies_df.filter(col("movieId") == movie_id) \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movie_by_title(self, title):
        return self.__movies_df.withColumn("key_word", lit(title)) \
            .filter(self.__movies_df.title.contains(title) | (levenshtein(col("title"), col("key_word")) < 10)) \
            .drop("key_word") \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movies_by_genres(self, genres):
        return self.__movies_df_split_genres.filter(col("genres").isin(genres)) \
            .select(col("genres"), col("movieId"), col("title")).orderBy("title")

    def search_movies_by_year(self, year):
        return self.__movies_df_with_year_col.filter(col("year") == year).select(col("year"), col("title")).orderBy(
            "title")

    def top_rating_movies(self, length, order):
        return self.avg_rating_per_movie().orderBy(col("avg").desc()).limit(length).join(self.__movies_df, on="movieId",
                                                                                         how="inner")

    def top_watched_movies(self, length, order):
        return self.count_user_per_movie().orderBy(col("nb_users").desc()). \
            limit(length). \
            join(self.__movies_df, on="movieId", how="inner"). \
            orderBy(col("nb_users").desc())

    def get_genres_list(self):
        return list(self.__movies_df_split_genres.select(col('genres')).distinct().orderBy('genres').cache().toPandas()['genres'])
