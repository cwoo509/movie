import numpy as np
from dbmodule import dbModule
import pandas as pd
import random
oracle_db = dbModule.Database()

# user_id = 5

def merge_chart(userid):
    movies = oracle_db.read_data('movies')
    sql = """select count(movieid) as vote_count, round(avg(rating),1) as vote_average, movieid 
    from ratings group by movieid"""
    md = oracle_db.read_sql(sql)
    movies['genres'] = movies['genres'].str.replace('|', ' ').tolist()
    movies['year'] = movies['title'].str[-7:].str.replace('(', '')
    movies['year'] = movies['year'].str.replace(')', '')
    movies['title'] = movies['title'].str.strip().str[:-7]
    # userid 기반 데이터
    sql = "select movieid from ratings where userid = {}".format(user_id)
    user_watch_movie = oracle_db.read_sql(sql)
    user_watch_movie = np.array(user_watch_movie['movieid'].tolist())

    mask = md['movieid'].isin(user_watch_movie)
    md = md[~mask] # ~를 포함하게 되면 mask의 값을 제외, ~을 제외하면 mask의 값을 포함입니다.
    mask2 = movies['movieid'].isin(user_watch_movie)
    movies = movies[~mask2] # ~를 포함하게 되면 mask의 값을 제외, ~을 제외하면 mask의 값을 포함입니다.

    data = pd.merge(movies, md, on='movieid')
    return data

def user_genre(userid):
    sql = "select M_GENRE from members where userid = {}".format(user_id)
    genre_ = oracle_db.read_sql(sql)

    genrelist = pd.array(genre_['m_genre'].tolist())
    genrelist = '|'.join(genrelist)
    genrelist = genrelist.split("|")
    genre_name = random.choice(genrelist)
    return genre_name

def build_chart(user_id, genre=None, percentile=0.85):
    movies = oracle_db.read_data('movies')
    sql = """select count(movieid) as vote_count, round(avg(rating),1) as vote_average, movieid 
    from ratings group by movieid"""
    md = oracle_db.read_sql(sql)
    movies['genres'] = movies['genres'].str.replace('|', ' ').tolist()
    movies['year'] = movies['title'].str[-7:].str.replace('(', '')
    movies['year'] = movies['year'].str.replace(')', '')
    movies['title'] = movies['title'].str.strip().str[:-7]
    # userid 기반 데이터
    sql = "select movieid from ratings where userid = {}".format(user_id)
    user_watch_movie = oracle_db.read_sql(sql)
    user_watch_movie = np.array(user_watch_movie['movieid'].tolist())

    mask = md['movieid'].isin(user_watch_movie)
    md = md[~mask] # ~를 포함하게 되면 mask의 값을 제외, ~을 제외하면 mask의 값을 포함입니다.
    mask2 = movies['movieid'].isin(user_watch_movie)
    movies = movies[~mask2] # ~를 포함하게 되면 mask의 값을 제외, ~을 제외하면 mask의 값을 포함입니다.

    data = pd.merge(movies, md, on='movieid')
    if genre is not None:
        df = data[data['genres'].str.contains(genre)]
    vote_counts = df[df['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = df[df['vote_average'].notnull()]['vote_average'].astype('int')
    C = vote_averages.mean()
    m = vote_counts.quantile(percentile)

    qualified = df[(df['vote_count'] >= m) & (df['vote_count'].notnull()) & (df['vote_average'].notnull())][
        ['title', 'year', 'vote_count', 'vote_average', 'movieid']]
    qualified['vote_count'] = qualified['vote_count'].astype('int')
    qualified['vote_average'] = qualified['vote_average'].astype('int')
    qualified = qualified

    qualified['wr'] = qualified.apply(
        lambda x: (x['vote_count'] / (x['vote_count'] + m) * x['vote_average']) + (m / (m + x['vote_count']) * C),
        axis=1)
    qualified = qualified.sort_values('wr', ascending=False).head(10)
    # 추천 영화 10개 저장
    already_rated_10 = qualified['movieid'].head(10)
    recommend_top10_movieid_list = [id for id in already_rated_10]
    oracle_db.write_on_top_ten(
        'imdb_top_ten', user_id, recommend_top10_movieid_list)
    return qualified

# data = merge_chart(user_id)
#
# genre_name = user_genre(user_id)
# 차트 넣기
# build_chart(user_id, genre_name)
