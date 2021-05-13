from dbmodule import dbModule
import pandas as pd

oracle_db = dbModule.Database()

# one-hot-encode movies_df's genres column
def one_hot_encode_genres(df: pd.DataFrame):
    # Copying the movie dataframe into a new one since we won't need to use
    # the genre information in our content-based recommendation system.
    movies_with_genres_df = df.copy()

    # For every row in the dataframe, iterate through the list of genres and place
    # a 1 into the corresponding column
    for index, row in df.iterrows():
        for genre in row.genres:
            movies_with_genres_df.at[index, genre] = 1

    # Filling in the NaN values with 0 to show that a movie doesn't have that column's genre
    movies_with_genres_df.fillna(0, inplace=True)

    return movies_with_genres_df
        
def get_member_recommendation(M_ID):
    # by userId
    members_userId = oracle_db.get_members_userId(M_ID).values.tolist()[0][0]
    # 유저 아이디 번호 (숫자)
    # userId로 RATINGS에서 유저가 점수를 매긴 영화들을 찾기!
    members_ratings_df = oracle_db.get_members_ratings(members_userId)
    members_movieIds = members_ratings_df["movieid"].to_list()
    members_one_hotted_movies = oracle_db.get_members_one_hotted_movies(members_movieIds)

    same_members_ratings_df = members_ratings_df[members_ratings_df.movieid.isin(members_one_hotted_movies.movieid.tolist())]
    members_one_hotted_movies = members_one_hotted_movies.reset_index(drop=True)
    drop_these_infos = ["movieid", "title", "genres", "year"]
    members_one_hot = members_one_hotted_movies.drop(drop_these_infos, axis='columns')
        
    # (20,626) * (626,) == (20,)
    # print("members_one_hot.T")
    # print(members_one_hot.transpose())
    # print('same_members_ratings_df["rating"]')
    # print(same_members_ratings_df["rating"])
    user_profile = members_one_hot.T.dot(list(map(list, zip(*[same_members_ratings_df["rating"]]))))
    # user_profile = same_members_ratings_df
    # user_profile = user_profile.sort_values(by=0, ascending=False)
    # print(user_profile)

    # 여기서부터는 모든 영화 데이터 사용
    # Now let's get the genres of every movie in our original dataframe
    movies_with_genres_df = oracle_db.get_one_hotted_movies()
    genre_df = movies_with_genres_df.set_index(movies_with_genres_df['movieid'])

    # Droping the unnecessary information
    genre_df.drop(['movieid', 'title', 'genres', 'year'], axis='columns', inplace=True)
    recommendation_df = ((genre_df*user_profile[0]).sum(axis="columns")) / user_profile[0].sum()
    recommend_top10_movieid_list = recommendation_df.sort_values(ascending=False).head(10).index
    # 전체 영화중에 최고 10개만 보내기
    return recommend_top10_movieid_list

top10_movieid_list = get_member_recommendation("stkim")
print(top10_movieid_list)