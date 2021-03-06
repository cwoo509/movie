from sqlalchemy import types, create_engine
import pandas as pd
from sqlalchemy.sql.expression import table
from dbmodule import conn

conn = conn.conn()


class Database():
    def __init__(self):
        self.pd = pd
# ===========================================================================

    def get_members_userId(self, M_ID):
        sql = "select USERID from MEMBERS where M_ID = '{}'".format(M_ID)
        data = self.pd.read_sql(sql, conn)
        return data

    def get_movies(self):
        sql = "select * from MOVIES"
        data = self.pd.read_sql(sql, conn)
        return data

    def get_ratings(self):
        sql = "select * from RATINGS"
        data = self.pd.read_sql(sql, conn)
        return data

    def get_others_ratings(self, userId):
        sql = "select MOVIEID from RATINGS where userid != {}".format(userId)
        print(sql)
        data = self.pd.read_sql(sql, conn)
        print(data)
        return data

    def get_members_ratings(self, userId):
        sql = "select MOVIEID, RATING from RATINGS where USERID = {}".format(
            userId)
        data = self.pd.read_sql(sql, conn)
        return data

    def get_members_ratings_userid_movieid(self, userId):
        sql = "select USERID, MOVIEID from RATINGS where USERID = {}".format(
            userId)
        data = self.pd.read_sql(sql, conn)
        return data

    def get_one_hotted_movies(self):
        sql = "select * from ONEHOT_MOVIES"
        data = self.pd.read_sql(sql, conn)
        return data

    def get_members_one_hotted_movies(self, members_movieIds):
        # members_movieIds--> []
        conditions = []
        for id in members_movieIds:
            conditions.append("MOVIEID=" + str(id))
        sql = "select * from ONEHOT_MOVIES where {}".format(
            ' OR '.join(conditions))
        data = self.pd.read_sql(sql, conn)
        return data

    def get_members_genre(self, M_ID):
        sql = "select M_GENRE from MEMBERS where M_ID = '{}'".format(M_ID)
        data = self.pd.read_sql(sql, conn)
        return data

    def preprocessed_movies(self, data, table_name):
        rows = [tuple(x) for x in data.values]
        sql = "INSERT INTO {} VALUES({})".format(table_name, ', '.join(
            [':'+str(i+1) for i in range(len(data.columns))]))
        conn.execute(sql, rows)

    def get_cleaned_movies_movieid_title(self):
        sql = "select MOVIEID, TITLE from ONEHOT_MOVIES"
        data = self.pd.read_sql(sql, conn)
        return data

    def write_on_top_ten(self, table_name, userId, top_ten_list):
        # print(table_name, userId, top_ten_list)
        # print(type(table_name), type(userId), type(top_ten_list))

        check_sql = "select * from {} where userId={}".format(
            table_name, userId)
        checked_data = self.pd.read_sql(check_sql, conn)
        if checked_data.__len__() == 0:
            for rank, movieid in enumerate(top_ten_list):
                # values(userId, movieid, ranking)
                sql = "insert into {} values({}, {}, {})".format(
                    table_name, userId, movieid, rank+1)
                print("try- sql:", sql)
                conn.execute(sql)
        else:
            for rank, movieid in enumerate(top_ten_list):
                sql = "update {} set movieid={}, ranking={} where userid={} and ranking={}".format(
                    table_name, movieid, rank+1, userId, rank+1)
                print("exception- sql:", sql)
                conn.execute(sql)
# ===============================stkim-userbased===============================
    # user-user :: collaborative filtering --> SVD
    # other member's rating list with the same movieid user watched before.

    def get_others_ratings_user_watched(self, userId, score):
        sql = """SELECT
                    A.USERID,
                    A.MOVIEID,
                    A.RATING
                FROM RATINGS A
                WHERE A.USERID!={} AND A.RATING>={} AND A.MOVIEID IN (SELECT MOVIEID
                                                    FROM RATINGS
                                                    WHERE (USERID={} AND RATING>={}))
              """.format(userId, score, userId, score)
        data = self.pd.read_sql(sql, conn)
        return data

    def get_members_ratings_over_score(self, userId, score):
        sql = "select MOVIEID, RATING from RATINGS where USERID = {} and RATING >= {}".format(
            userId, score)
        data = self.pd.read_sql(sql, conn)

        return data
# =============================================================================

    # def read_data(self, table_name, files_name):
    #     sql = "select * from {} where files_name = '{}'".format(
    #         table_name.lower(), files_name)
    #     data = self.pd.read_sql(sql, conn)
    #     # data ????????? ???????????????
    #     data.columns = data.columns.str.lower()
    #     return data

    def read_data(self, tablesName):
        if tablesName == 'members':
            sql = "select M_ID, M_AGE, M_SEX, M_GENRE, USERID, M_GENRE from {}".format(tablesName)
        else:
            sql = "select * from {}".format(tablesName)
        data = pd.read_sql(sql, conn)
        # data ????????? ???????????????
        data.columns = data.columns.str.lower()
        return data

    def read_data_all(self, table_name):
        data = self.pd.read_sql_table(table_name.lower(), conn)
        return data

    def create_data(self, data, new_table_name):
        # ????????? ???????????? ??????
        cols = data.columns
        type_dict = {}
        for col in cols:
            if type(data[col][0]) == str:
                type_dict[col] = types.VARCHAR(50)
        data.to_sql(new_table_name, conn, if_exists='append',
                    index=False, dtype=type_dict)

    def read_macro(self, macro_name):
        sql = "select * from macro where macro_name = '{}'".format(macro_name)
        macro_data = pd.read_sql(sql, conn)
        return macro_data

    def modeling_done(self, macro_name, score, report, kind):
        data = pd.DataFrame({'macro_name': macro_name, 'score': score,
                             'report': report, 'kind': kind}, index=[0])

        type_dict = {'macro_name': types.VARCHAR(
            20), 'score': types.FLOAT, 'report': types.CLOB, 'type': types.VARCHAR(20)}
        data.to_sql('macro_done', conn, if_exists='append',
                    index=False, dtype=type_dict)

    def set_storage(self, new_files_name, new_table_name):
        names = pd.DataFrame(
            data={'tables_name': new_table_name, 'files_name': new_files_name}, index=[0])
        names.to_sql('file_storage', conn, if_exists='append', index=False)

    def set_fk(self, new_table_name):
        new_table_name = new_table_name.lower()
        sql = "alter table {} add freoreign key (files_name) references file_storage(files_name) on delete cascade".format(
            new_table_name)
        conn.execute(sql)

    def check_table(self, new_table_name):
        new_table_name = new_table_name.upper()
        sql = "select count(*) from all_tables where table_name = '{}'".format(new_table_name)
        result = conn.execute(sql).scalar()
        return result

    def read_sql(self, sql):
        data = pd.read_sql(sql, conn)

        return data
# ===============================IMDB===============================