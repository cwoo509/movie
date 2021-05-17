import re
from dbmodule import dbModule
import pandas as pd

oracle_db = dbModule.Database()

# one-hot-encode movies_df's genres column


def get_contentbased_recommendation(M_ID):
    print("oracle_db.get_members_userId(M_ID):",oracle_db.get_members_userId(M_ID).values)
    members_userId = oracle_db.get_members_userId(M_ID).values.tolist()[0][0]
    members_ratings_df = oracle_db.get_members_ratings(members_userId)
    members_movieIds = members_ratings_df["movieid"].to_list()
    members_one_hotted_movies = oracle_db.get_members_one_hotted_movies(members_movieIds)

    same_members_ratings_df = members_ratings_df[members_ratings_df.movieid.isin(members_one_hotted_movies.movieid.tolist())]
    members_one_hotted_movies = members_one_hotted_movies.reset_index(drop=True)
    drop_these_infos = ["movieid", "title", "genres", "year"]
    members_one_hot = members_one_hotted_movies.drop(drop_these_infos, axis='columns')

    user_profile = members_one_hot.T.dot(list(map(list, zip(*[same_members_ratings_df["rating"]]))))

    movies_with_genres_df = oracle_db.get_one_hotted_movies()
    genre_df = movies_with_genres_df.set_index(movies_with_genres_df['movieid'])

    genre_df.drop(['movieid', 'title', 'genres', 'year'],axis='columns', inplace=True)
    recommendation_df = ((genre_df*user_profile[0]).sum(axis="columns")) / user_profile[0].sum()
    recommendation_df = recommendation_df.sort_values(ascending=False).head(10).index
    recommend_top10_movieid_list = [id for id in recommendation_df]

    oracle_db.write_on_top_ten('contents_top_ten', members_userId, recommend_top10_movieid_list)

    return recommend_top10_movieid_list