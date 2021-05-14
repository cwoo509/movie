from dbmodule import dbModule
import numpy as np
from scipy.linalg import svd
import pandas as pd

oracle_db = dbModule.Database()

df_ratings = oracle_db.get_ratings()
df_movies = oracle_db.get_movies()

def get_userbased_recommend(M_ID):

    df_user_movie_ratings = df_ratings.pivot(
        index='userid',
        columns='movieid',
        values='rating'
    ).fillna(0)

    # matrix는 pivot_table 값을 numpy matrix로 만드는 것
    matrix = df_user_movie_ratings.values

    # user_raitngs_mean은 사용자의 평균 평점
    user_ratings_mean = np.mean(matrix, axis=1)

    # R_user_mean : 사용자-영화에 대해 사용자 평균 평점을 뺀것
    matrix_user_mean = matrix - user_ratings_mean.reshape(-1, 1)

    U, sigma, V = np.linalg.svd(matrix_user_mean, full_matrices=False)

    sigma = np.diag(sigma)

    # U, sigma, V 의 내적을 수행하면 다시 원본 행렬로 복원해야 한다
    # 거기에 + 사용자 평균 rating 을 적용한다
    svd_user_predicted_ratings = np.dot(np.dot(U, sigma), V) + user_ratings_mean.reshape(-1, 1)
    df_svd_preds = pd.DataFrame(svd_user_predicted_ratings, columns=df_user_movie_ratings.columns)

    already_rated = recommend_movies(df_svd_preds, M_ID, df_movies, df_ratings, 100)

    return already_rated[0]


def recommend_movies(df_svd_preds, user_id, ori_movies_df, ori_ratings_df, num_recommendations=5):
    # 현재는 index로 적용되어 있으므로 user_ud -1d을 해야함
    user_row_number = user_id - 1
    # 최종적으로 만든 pred_df 에서 사용자 index에 따라 영화 데이터를 정렬
    sorted_user_predictions = df_svd_preds.iloc[user_row_number].sort_values(ascending=False)
    # 원본 평점 데이터에서 uesr id 에 해당하는 데이터를 뽑아냅니다.
    user_data = ori_ratings_df[ori_ratings_df.userid == user_id]

    # 위에서 뽑은 user_data 와 원본 영화 데이터를 합친디.
    user_history = user_data.merge(ori_movies_df, on='movieid').sort_values(['rating'], ascending=False)

    # 원본 영화 데이터에서 사용자가 본 영화데이터를 제외한 데이터를 추출
    recommendations = ori_movies_df[~ori_movies_df['movieid'].isin(user_history['movieid'])]
    # 사용자의 영화 평점이 높은 순으로 정렬된 데이터와 위 추천을 합친다
    recommendations = recommendations.merge(pd.DataFrame(sorted_user_predictions).reset_index(), on='movieid')
    # 컬럼 이름 바꾸고 정렬해서 return
    recommendations = recommendations.rename(columns={user_row_number: "Predictions"}).sort_values('Predictions',
                                                                                                   ascending=False)
    return user_history, recommendations

print(get_userbased_recommend(220))