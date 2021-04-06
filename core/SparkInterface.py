from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
from pyspark.sql.functions import explode, split, regexp_extract, col, avg, count, levenshtein, lit, udf
from Levenshtein import distance as levenshtein_distance

distance = udf(lambda c1, c2: sum(1 for word in c1.split() if (levenshtein_distance(c2, word) < 3)) >= 1,
               BooleanType())


class SparkInterface:
    """
    SparkInterface, consisting of the functions corresponding to the practical requirements of
    parts 1 and 2.

    It includes functions for reading and preprocessing the dataset
    """
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
        """
        Read the csv files in a given folder as Spark DataFrames.

        Data source :
        Jesse Vig, Shilad Sen, and John Riedl. 2012. The Tag Genome: Encoding Community Knowledge to
        Support Novel Interaction. ACM Trans. Interact. Intell. Syst. 2, 3: 13:1–13:44.
        <https://doi.org/10.1145/2362394.2362395>

        :param path: path to the folder containing the csv files
        :return:
        """
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
        """
        Compute common intermediate DataFrames and cache to reduce the execution time.
        """

        # A DataFrame of movies where the genre cells are split into several rows
        self.__movies_df_split_genres = self.__movies_df \
            .withColumn('genres', explode(split(self.__movies_df.genres, "\\|"))) \
            .filter(self.__movies_df.genres != "(no genres listed)") \
            .filter(self.__movies_df.genres != "(s listed)") \
            .dropna()

        # A DataFrame of the movies where the title and year of the movie are in separated columns
        self.__movies_df_with_year_col = self.__movies_df \
            .withColumn('year',
                        regexp_extract(self.__movies_df['title'], '[1-2][0-9][0-9][0-9]', 0).cast(IntegerType())) \
            .withColumn('title', split(self.__movies_df['title'], '\([1-2][0-9][0-9][0-9]\)').getItem(0))

        # A DataFrame that contains only the movies that have been rated or tagged
        self.__reduced_ratings = self.__ratings_df.select(col("userId"), col("movieId")).distinct()
        self.__reduced_tags = self.__tags_df.select(col("userId"), col("movieId")).distinct()
        self.__movies_user_df = self.__reduced_ratings.union(self.__reduced_tags).distinct().cache()

        # A DataFrame combining average rating per movie where genres are split in rows
        self.__favor_genre_df = self.__movies_df_split_genres \
            .join(self.__ratings_df, self.__movies_df_split_genres.movieId == self.__ratings_df.movieId) \
            .drop(self.__ratings_df.movieId) \
            .drop(self.__ratings_df.timestamp)

    def movies_per_genre_watched_by_user(self, user_id):
        """
        Count the movies watched by a given user for each genre ordered by the genre with the
        highest number of movies watched.

        :param user_id: user ID
        :return: DataFrame of the movie count per genre (userId, genres, count)
        """
        movies_user_info_df = self.__movies_user_df \
            .join(self.__movies_df_split_genres,
                  self.__movies_user_df.movieId == self.__movies_df_split_genres.movieId,
                  how='inner') \
            .filter(col("userId") == user_id) \
            .groupby(col("userId"), col("genres")) \
            .count() \
            .orderBy(col("userId"))
        return movies_user_info_df

    def movies_watched_by_users(self, user_ids):
        """
        List all the movies watched by a list of users and order it by user ID
        :param user_ids: list of users IDs
        :return: DataFrame (userId,title)
        """
        results = self.__movies_user_df \
            .join(self.__movies_df_with_year_col,
                  self.__movies_user_df.movieId == self.__movies_df_with_year_col.movieId,
                  how='inner') \
            .select(col("userId"), col("title")) \
            .distinct() \
            .filter(col("userId").isin(user_ids)) \
            .orderBy(col("userId"))
        return results

    def avg_rating_per_movie(self):
        """
        Compute the average rating of each movie.

        :return DataFrame(movieId, avg)
        """
        return self.__ratings_df.groupby(col("movieId")).agg(avg('rating').alias('avg')).cache()

    def count_user_per_movie(self):
        """
        Compute the number of watched per movie
        :return: DataFrame(moviesId,nb_users)
        """
        return self.__movies_user_df.groupby(col("movieId")).agg(count("userId").alias('nb_users')).cache()

    def search_movie_by_id(self, movie_id):
        """
        Search a movie by its ID

        :param movie_id: ID of the movie
        :return: DataFrame(movieId, title, genres, avg, nb_users)
        """
        return self.__movies_df.filter(col("movieId") == movie_id) \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movie_by_title(self, title):
        """
        Search a movie by its title.

        Fuzzy matching algorithm is used to find all the title that contains a word similar to the given
        keyword.

        The Levenshtein distance computes the similarity between two words. Only distances less then 3 are
        accepted.

        Example : With a keyword = "Jumangi" the returned DataFrame contains a movie with title
                  "Jumanji : Welcome to the Jungle"

        :param title: the keyword to search with
        :return: DataFrame(moviesId, title, genres)
        """
        return self.__movies_df.withColumn("key_word", lit(title)) \
            .withColumn("distance", distance(self.__movies_df.title, col("key_word"))) \
            .filter(self.__movies_df.title.contains(title) | col("distance")) \
            .drop("key_word") \
            .drop("distance") \
            .join(self.avg_rating_per_movie(), on='movieId', how='inner') \
            .join(self.count_user_per_movie(), on='movieId', how='inner')

    def search_movies_by_genres(self, genres):
        """
        Search the movies included in given genres

        :param genres: list of genres
        :return: DataFrame(genres,moviesId,title)
        """
        return self.__movies_df_split_genres.filter(col("genres").isin(genres)) \
            .select(col("genres"), col("movieId"), col("title")).orderBy("title")

    def search_movies_by_year(self, year):
        """
        Search for all movies released in a given year
        :param year: year of movie release
        :return: DataFrame(year, moviesId, title)
        """
        return self.__movies_df_with_year_col \
            .filter(col("year") == year) \
            .select(col("year"), col('movieId'), col("title")) \
            .orderBy("title")

    def top_rating_movies(self, length, order):
        """
        Search for the top rated movies

        :param length: the number of movies to return
        :return: DataFrame(movieId,title,genres,avg)
        """
        return self.avg_rating_per_movie() \
            .orderBy(col("avg").desc()) \
            .limit(length) \
            .join(self.__movies_df, on="movieId", how="inner")

    def top_watched_movies(self, length, order):
        """
        Search for most viewed movies

        :param length: the number of movies to return
        :return: DataFrame(movieId,title,genres,nb_users)
        """
        return self.count_user_per_movie() \
            .orderBy(col("nb_users").desc()) \
            .limit(length) \
            .join(self.__movies_df, on="movieId", how="inner") \
            .orderBy(col("nb_users").desc())

    def get_genres_list(self):
        """
        Search for genres labels in the dataset

        :return: list of genres labels
        """
        return list(self.__movies_df_split_genres.select(col('genres'))
                    .distinct()
                    .orderBy('genres')
                    .cache()
                    .toPandas()['genres'])

    def favorite_genre_user(self, user_id, factor_genre):
        """
        Find the favourite genre of a given user.

        This is done by calculating the formula : count * factor + average_rating * ( 1 - factor) on users' ratings
        and movies watched.
        - count : is the number of movies watched per genre for the given user.
        - average_rating : is the average rating of a genre for the given user.
        - factor : is the weight value of count. That's to say, how much important
        count is when deciding a favorite genre.

        :param user_id: the Id of the user
        :param factor_genre: the proportion of count to average rating in the formula
        :return: Pandas DataFrame(count,genres,avg_rating,genre_score)
        """
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
        """
        Find the favourite genre of group of users.

        This is done by calculating the formula : count * factor + average_rating * ( 1 - factor) on users' ratings
        and movies watched.
        - count : is the number of movies watched per genre for the given group of users.
        - average_rating : is the average rating of a genre for the given group of users.
        - factor : is the weight value of count. That's to say, how much important
        count is when deciding a favorite genre.

        :param user_ids: list of users IDs
        :param factor_genre: the proportion of count to average rating in the formula
        :return: Pandas DataFrame(count,genres,avg_rating,genre_score)
        """
        genre_number_df = self.__favor_genre_df \
            .filter(col("userId").isin(user_ids)) \
            .groupBy(self.__favor_genre_df.genres) \
            .count()

        avg_rating_df = self.__favor_genre_df \
            .filter(col("userId").isin(user_ids)) \
            .groupBy(self.__favor_genre_df.genres) \
            .agg(avg("rating").alias("avg_rating"))

        user_favor_df = genre_number_df \
            .join(avg_rating_df, genre_number_df.genres == avg_rating_df.genres, how='inner') \
            .drop(genre_number_df.genres)

        # count * factor + average_rating * ( 1 - factor)
        weighted_df = user_favor_df \
            .withColumn("genre_score", col('count') * factor_genre + col('avg_rating') * (1 - factor_genre)) \
            .orderBy(col("genre_score"))

        return weighted_df.toPandas()

    def compare_taste(self, uids):
        """
        Compare the movie tastes of two users.

        Movie taste is compared in two aspects.
            1. Number of common watched movies by two users.
            2. Similarity score out of 10 based on two users' ratings on these common movies.

        :param uids: list of two user IDs
        :return: Pandas DataFrame(title,genres,userId,rating_user1,moviesId,rating_user2)
        """
        uid1 = uids[0]
        uid2 = uids[1]

        user1_df = self.__favor_genre_df \
            .filter(self.__favor_genre_df.userId == uid1) \
            .dropDuplicates(["movieId"]) \
            .withColumnRenamed("rating", "rating_user1")

        user2_df = self.__favor_genre_df \
            .filter(self.__favor_genre_df.userId == uid2) \
            .select(col("movieId"), col("rating")) \
            .dropDuplicates(["movieId"]) \
            .withColumnRenamed("rating", "rating_user2")

        common_movie_df = user1_df \
            .join(user2_df, user1_df.movieId == user2_df.movieId, how='inner') \
            .drop(user2_df.movieId)

        return common_movie_df.toPandas()

    def similarity_score(self, common_movie_pandasdf):
        """

        :param common_movie_pandasdf:
        :return:
        """

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
