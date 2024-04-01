import mysql.connector
from datetime import date
import pandas as pd

def top_10():
    db = mysql.connector.connect(
        host='20.163.58.138',
        user='dvp',
        port='3306',
        password='Zzb33k23432@#$#@',
        database='test')

    query= """
    SELECT live_articles.key_id, live_rankings.main_rank, live_articles.title, live_articles.title_A, 
    live_articles.title_B, live_articles.title_C, live_articles.body, live_articles.create_date, live_articles.update_date
    FROM test.live_rankings
    LEFT JOIN test.live_articles
    ON live_rankings.key_id = live_articles.key_id
    ORDER BY live_rankings.main_rank;
    """

    df = pd.read_sql_query(query, db)

    # NEED TO REPLACE NOT COMMIT

    query1 = """
        SELECT live_cat.key_id, live_cat.cat
        FROM live_cat
        LEFT JOIN test.live_rankings
        ON live_cat.key_id = live_rankings.key_id
        ORDER BY live_rankings.main_rank;
        """
    df_cat_ = pd.read_sql_query(query1, db)
    df_cat_ = df_cat_.drop_duplicates()
    df_cat=df_cat_.groupby('key_id', as_index=False).agg(list)


    query2 = """
        SELECT live_img.key_id, live_img.cat
        FROM live_img
        LEFT JOIN live_rankings
        ON live_img.key_id = live_rankings.key_id
        ORDER BY live_rankings.main_rank;
            """
    df_img_ = pd.read_sql_query(query2, db)
    df_img_ = df_img_.drop_duplicates() # drop duplicate
    df_img = df_img_.groupby('key_id', as_index=False).agg(list)

    query3 = """
            SELECT live_urls.key_id, live_urls.cat
            FROM live_urls
            LEFT JOIN live_rankings
            ON live_urls.key_id = live_rankings.key_id
            ORDER BY live_rankings.main_rank;
                """
    df_url_ = pd.read_sql_query(query3, db)
    df_url_ = df_url_.drop_duplicates()  # drop duplicate
    df_url = df_url_.groupby('key_id', as_index=False).agg(list)


    query = """
        SELECT key_id FROM test.live_rankings
        ORDER BY live_rankings.main_rank;
        """

    uuid_lst = pd.read_sql_query(query, db)

    df_ = pd.merge(df_img,df_cat,how="inner", on='key_id')
    df_ = pd.merge(df_, df_url, how="inner", on='key_id')
    df_all = pd.merge(df, df_, how='inner', on='key_id')
    df = df_all.dropna()

    db.close()

    return df,uuid_lst['key_id'][9:].tolist()


def commit_cmnts(key_id, cmt_id, user, cmnt, date):
    db = mysql.connector.connect(
        host='20.163.58.138',
        user='dvp',
        port='3306',
        password='Zzb33k23432@#$#@',
        database='test')

    c = db.cursor()
    params = (key_id, cmt_id, user, cmnt, date)
    insert_query = "INSERT INTO cmnts (key_id, cmt_id, user, cmnt, date) VALUES (%s, %s, %s, %s, %s);"

    c.execute(insert_query, params)
    db.commit()
    c.close()
    db.close()

def commit_interactions(key_id, sessionId, eventType,  timestamp):
    db = mysql.connector.connect(
        host='20.163.58.138',
        user='dvp',
        port='3306',
        password='Zzb33k23432@#$#@',
        database='test')

    c = db.cursor()

    params = (key_id, sessionId, eventType, timestamp)
    insert_query = "INSERT INTO interactions (key_id, sessionId, eventType,  timestamp) VALUES (%s, %s, %s, %s);"

    c.execute(insert_query, params)

    db.commit()
    c.close()
    db.close()
