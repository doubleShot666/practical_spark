from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import explode, split, regexp_extract, col, avg, count, levenshtein, lit


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
        self.__favor_genre_df = None

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
        self.__links_df = self.__spark.read.csv(path + 'links.csv', header=True, schema=links_schema).dropna()
        self.__tags_df = self.__spark.read.csv(path + 'tags.csv', header=True, schema=tags_schema).dropna().dropna()
        self.__movies_df = self.__spark.read.csv(path + 'movies.csv', header=True, schema=movies_schema).dropna()
        self.__ratings_df = self.__spark.read.csv(path + 'ratings.csv', header=True, schema=ratings_schema).dropna()
        self.prepare_dataset()

    def prepare_dataset(self):
        self.__movies_df_split_genres = self.__movies_df\
            .withColumn('genres', explode(split(self.__movies_df.genres, "\\|")))\
            .filter(self.__movies_df.genres != "(no genres listed)")\
            .filter(self.__movies_df.genres != "(s listed)")\
            .dropna()

        self.__movies_df_with_year_col = self.__movies_df \
            .withColumn('year',
                        regexp_extract(self.__movies_df['title'], '[1-2][0-9][0-9][0-9]', 0).cast(IntegerType())) \
            .withColumn('title', split(self.__movies_df['title'], '\([1-2][0-9][0-9][0-9]\)').getItem(0))

        self.__reduced_ratings = self.__ratings_df.select(col("userId"), col("movieId")).distinct()
        self.__reduced_tags = self.__tags_df.select(col("userId"), col("movieId")).distinct()
        self.__movies_user_df = self.__reduced_ratings.union(self.__reduced_tags).distinct().cache()

        self.__favor_genre_df = self.__movies_df_split_genres\
            .join(self.__ratings_df, self.__movies_df_split_genres.movieId == self.__ratings_df.movieId)\
            .drop(self.__ratings_df.movieId)\
            .drop(self.__ratings_df.timestamp)

    def movies_per_genre_watched_by_user(self, user_id):
        movies_user_info_df = self.__movies_user_df\
            .join(self.__movies_df_split_genres,
                  self.__movies_user_df.movieId == self.__movies_df_split_genres.movieId,
                  how='inner')\
            .filter(col("userId") == user_id) \
            .groupby(col("userId"), col("genres")) \
            .count() \
            .orderBy(col("userId"))
        return movies_user_info_df

    def movies_watched_by_users(self, user_ids):
        # All movies watched by each user of a given list of users
        results = self.__movies_user_df\
            .join(self.__movies_df_with_year_col,
                  self.__movies_user_df.movieId == self.__movies_df_with_year_col.movieId,
                  how='inner')\
            .select(col("userId"), col("title")) \
            .distinct() \
            .filter(col("userId").isin(user_ids)) \
            .orderBy(col("userId"))
        return results

    def avg_rating_per_movie(self):
        return self.__ratings_df.groupby(col("movieId")).agg(avg('rating').alias('avg')).cache()

    def count_user_per_movie(self):
        return self.__movies_user_df.groupby(col("movieId")).agg(count("userId").alias('nb_users')).cache()

    def search_movie_by_id(self, movie_id):
        return self.__movies_df.filter(col("movieId") == movie_id) \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movie_by_title(self, title):
        return self.__movies_df.withColumn("key_word", lit(title)) \
            .filter(self.__movies_df.title.contains(title) | (levenshtein(col("title"), col("key_word")) < 4)) \
            .drop("key_word") \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movies_by_genres(self, genres):
        return self.__movies_df_split_genres.filter(col("genres").isin(genres)) \
            .select(col("genres"), col("movieId"), col("title")).orderBy("title")

    def search_movies_by_year(self, year):
        return self.__movies_df_with_year_col\
            .filter(col("year") == year)\
            .select(col("year"), col('movieId'), col("title"))\
            .orderBy("title")

    def top_rating_movies(self, length, order):
        return self.avg_rating_per_movie()\
            .orderBy(col("avg").desc())\
            .limit(length)\
            .join(self.__movies_df, on="movieId", how="inner")

    def top_watched_movies(self, length, order):
        return self.count_user_per_movie()\
            .orderBy(col("nb_users").desc())\
            .limit(length)\
            .join(self.__movies_df, on="movieId", how="inner")\
            .orderBy(col("nb_users").desc())

    def get_genres_list(self):
        return list(self.__movies_df_split_genres.select(col('genres'))
                    .distinct()
                    .orderBy('genres')
                    .cache()
                    .toPandas()['genres'])

    def favorite_genre_user(self, user_id, factor_genre):

        genre_number_df = self.__favor_genre_df \
            .filter(self.__favor_genre_df.userId == user_id) \
            .groupBy(self.__favor_genre_df.genres) \
            .count()

        avg_rating_df = self.__favor_genre_df \
            .filter(self.__favor_genre_df.userId == user_id) \
            .groupBy(self.__favor_genre_df.genres) \
            .agg(avg("rating").alias("avg_rating"))

        user_favor_df = genre_number_df \
            .join(avg_rating_df, genre_number_df.genres == avg_rating_df.genres, how='inner') \
            .drop(genre_number_df.genres)

        # count * factor + average_rating * ( 1 - factor)
        weighted_df = user_favor_df.withColumn("genre_score", col('count') * factor_genre + col('avg_rating') * (
                1 - factor_genre)).orderBy(col("genre_score"))

        return weighted_df.toPandas()

    def favorite_genre_usersgroup(self, user_ids, factor_genre):
        genre_number_df = self.__favor_genre_df\
            .filter(col("userId").isin(user_ids))\
            .groupBy(self.__favor_genre_df.genres)\
            .count()

        avg_rating_df = self.__favor_genre_df\
            .filter(col("userId").isin(user_ids))\
            .groupBy(self.__favor_genre_df.genres)\
            .agg(avg("rating").alias("avg_rating"))

        user_favor_df = genre_number_df\
            .join(avg_rating_df, genre_number_df.genres == avg_rating_df.genres, how='inner')\
            .drop(genre_number_df.genres)

        # count * factor + average_rating * ( 1 - factor)
        weighted_df = user_favor_df\
            .withColumn("genre_score", col('count') * factor_genre + col('avg_rating') * (1 - factor_genre))\
            .orderBy(col("genre_score"))

        return weighted_df.toPandas()

    # compare movie taste: Number of common movie
    #                      Similarity score
    def compare_taste(self, uids):
        uid1 = uids[0]
        uid2 = uids[1]

        user1_df = self.__favor_genre_df\
            .filter(self.__favor_genre_df.userId == uid1)\
            .dropDuplicates(["movieId"])\
            .withColumnRenamed("rating", "rating_user1")

        user2_df = self.__favor_genre_df\
            .filter(self.__favor_genre_df.userId == uid2)\
            .select(col("movieId"),col("rating"))\
            .dropDuplicates(["movieId"])\
            .withColumnRenamed("rating", "rating_user2")

        common_movie_df = user1_df\
            .join(user2_df, user1_df.movieId == user2_df.movieId, how='inner')\
            .drop(user2_df.movieId)

        return common_movie_df.toPandas()

    def similarity_score(self, common_movie_pandasdf):

        common_movie_pandasdf.eval('abs = abs(rating_user1 - rating_user2)', inplace=True)

        abs_list = common_movie_pandasdf['abs'].values.tolist()

        length = len(abs_list)

        print("Note: The range of similarity score of two users' taste is 0-10, 10 means the most relevant ")

        score = 0
        for x in abs_list:
            if x == 0.0:
                score += 10
            elif x == 0.5:
                score += 8
            elif x == 1.0:
                score += 6
            elif x == 1.5:
                score += 5
            elif x == 2.0:
                score += 4
            elif x == 2.5:
                score += 3
            elif x == 3.0:
                score += 2
            elif x == 3.5:
                score += 1
            elif x == 4.0:
                score += 0.5
            else:
                score += 0  # x == 4.5
        score = score / length
        print("The similarity score of two users' taste is: ", score)
        print("---------------------------------")
        return score