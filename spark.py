#!/usr/bin/env python
# coding: utf-8
import os
import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.context import SparkContext
from pyspark.sql import SparkSession


class SparkQuery:
    def __init__(self):
        # init spark
        self.__spark = SparkSession.builder.appName("spark_practical").getOrCreate()
        #SparkQuery.init_dataframe(self)

    def init_dataframe(self, datapath):

        os.chdir(sys.path[0])
        #path = os.getcwd()
        #print(path)
        schema_rating = (StructType().add("userId", IntegerType()).add("movieId", IntegerType()).add("rating", FloatType()).
                         add("timestamp", StringType()))
        self.__ratingsdf = self.__spark .read.csv(datapath + r"\ratings.csv", schema=schema_rating, header=True)

        schema = (StructType().add("movieId", IntegerType()).add("title", StringType()).add("genres", StringType()))
        self.__moviesdf = self.__spark.read.csv(datapath + "\movies.csv", schema=schema, header=True)
        self.__moviesdf = self.__moviesdf.withColumn("genres", explode(split("genres", "[|]")))
        self.__moviesdf = self.__moviesdf.filter(self.__moviesdf.genres != "(no genres listed)")

        self.__favor_genre_df = self.__moviesdf.join(self.__ratingsdf, self.__moviesdf.movieId == self.__ratingsdf.movieId).drop(self.__ratingsdf.movieId).drop(
            self.__ratingsdf.timestamp)

    def favorite_genre_user(self, user_id, factor_genre):

        genre_number_df = self.__favor_genre_df.filter(self.__favor_genre_df.userId == user_id).groupBy(self.__favor_genre_df.genres).count()
        avg_rating_df = self.__favor_genre_df.filter(self.__favor_genre_df.userId == user_id).groupBy(self.__favor_genre_df.genres).agg(
        avg("rating").alias("avg_rating"))
        user_favor_df = genre_number_df.join(avg_rating_df, genre_number_df.genres == avg_rating_df.genres, how='inner').\
            drop(genre_number_df.genres)
        # count * factor + average_rating * ( 1 - factor)
        weighted_df = user_favor_df.withColumn("genre_score", col('count') * factor_genre + col('avg_rating') * (
                1 - factor_genre)).orderBy(col("genre_score"))
        pandasdf = weighted_df.toPandas()
        taste = pandasdf[-1:]['genres'].values[0]
        print("---------------------------------")
        print("The favourite genre of this user is:", taste)
        print("---------------------------------")
        print("The following table gives the whole analysis information about user's taste:")
        print("Description: count - the number of specific genre movies watched. genres - the movie genre. "
              "avg_rating - average rating of this user to this genre's movie. "
              "genre_score - the calculate formula is [count * factor + average_rating * ( 1 - factor)]")
        print("---------------------------------")
        print(pandasdf)
        print("---------------------------------")
        return taste

    def favorite_genre_usersgroup(self, user_ids, factor_genre):

        genre_number_df = self.__favor_genre_df.filter(col("userId").isin(user_ids)).groupBy(self.__favor_genre_df.genres).count()
        avg_rating_df = self.__favor_genre_df.filter(col("userId").isin(user_ids)).groupBy(self.__favor_genre_df.genres).agg(avg("rating").alias("avg_rating"))
        user_favor_df = genre_number_df.join(avg_rating_df, genre_number_df.genres == avg_rating_df.genres, how='inner').drop(genre_number_df.genres)
        # count * factor + average_rating * ( 1 - factor)
        weighted_df = user_favor_df.withColumn("genre_score", col('count') * factor_genre + col('avg_rating') * (1 - factor_genre)).orderBy(col("genre_score"))
        pandasdf = weighted_df.toPandas()
        taste = pandasdf[-1:]['genres'].values[0]
        print("---------------------------------")
        print("The favourite genre of this group of users is:", taste)
        print("---------------------------------")
        print("The following table gives the whole analysis information about users' taste:")
        print("Description: count - the number of specific genre movies watched. genres - the movie genre. "
              "avg_rating - average rating of  user group to this genre's movie. "
              "genre_score - the calculate formula is [count * factor + average_rating * ( 1 - factor)]")
        print("---------------------------------")
        print(pandasdf)
        print("---------------------------------")
        return taste

    # compare movie taste: Number of common movie
    #                      Similarity score
    def compare_taste(self, uids):
        uid1 = uids[0]
        uid2 = uids[1]
        user1_df = self.__favor_genre_df.filter(self.__favor_genre_df.userId == uid1).dropDuplicates(["movieId"]).withColumnRenamed(
            "rating", "rating_user1")
        user2_df = self.__favor_genre_df.filter(self.__favor_genre_df.userId == uid2).select(col("movieId"),
                                                                               col("rating")).dropDuplicates(
            ["movieId"]).withColumnRenamed("rating", "rating_user2")
        common_movie_df = user1_df.join(user2_df, user1_df.movieId == user2_df.movieId, how='inner').drop(user2_df.movieId)
        common_movie_pandasdf = common_movie_df.toPandas()
        num = len(common_movie_pandasdf)
        print("Description: Movie taste is compared in two aspects. 1. Number of common watched movie by two users. "
              "2. similarity score out of 10 based on ratings")
        print("---------------------------------")
        print("Number of common watched movies is :", num)
        print("---------------------------------")
        print("Table of common watched movies:")
        print(common_movie_pandasdf)
        print("---------------------------------")
        SparkQuery.similarity_score(self, common_movie_pandasdf)
        return common_movie_pandasdf

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











