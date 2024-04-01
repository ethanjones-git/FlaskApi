'''
MAIN API
'''
import pandas as pd
import json
from intr_sql import top_10
from datetime import datetime

def seq(start,stop):
    # Define the starting number
    start_number = start

    # Define the sequence length
    sequence_length = stop

    # Generate the list of numbers
    sequence = [start_number + i for i in range(sequence_length)]
    return sequence

def articles_and_ranks():
    '''
    This takes 2 long-format tables, and merges them.

    This also finds two articles under the same rank. If there are two, it indents them in the json.

    :param article database path:
    :param rank database path:
    :return python dictionary:
    '''

    df,lst_ = top_10()

    df = df[df['main_rank'] >=11]
    upvotes = [0,1000,100,100,5,0]
    dwnvotes = upvotes[::-1]
    df['upvotes'], df['downvotes'] = upvotes, dwnvotes
    df['rank'] = df.index
    #df['cat_y'] = df['cat_y'].apply(lambda lst: [item.lower() for item in lst])

    out= df.to_dict(orient='records')

    json_data = {"articles":[]}
    for row in out:
        json_fnl = {
            'rank': row['rank'],
            'key_id': row['key_id'],
            'title': row['title'],
            'gist': row['title_A'],
            'title_B': row['title_B'],
            'title_C': row['title_C'],
            'category': row['cat_y'],
            'created_at': row['create_date'].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': row['update_date'].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'),
            'body': row['body'],
            'img': row['cat_x'],
            'urls': row['cat'],
            'comments': 0,
            'upvotes':row['upvotes'],
            'downvotes':row['downvotes']
        }
        json_data["articles"].append(json_fnl)

    return json_data

def categorical_pull(items):
    '''
    This takes 2 long-format tables, and merges them.
    This also finds two articles under the same rank. If there are two, it indents them in the json.

    :param article database path:
    :param rank database path:
    :return python dictionary:
    '''

    df, lst_ = top_10()

    df = df[df['main_rank'] >= 11]
    upvotes = [0, 1000, 100, 100, 5, 0]
    dwnvotes = upvotes[::-1]
    df['upvotes'], df['downvotes'] = upvotes, dwnvotes
    df['rank'] = df.index

    df = df[df['cat_y'].apply(lambda x: any(item in x for item in items))]

    out= df.to_dict(orient='records')

    json_data = {"articles":[]}
    for row in out:
        json_fnl = {
            'rank': row['rank'],
            'key_id': row['key_id'],
            'title': row['title'],
            'gist': row['title_A'],
            'title_B': row['title_B'],
            'title_C': row['title_C'],
            'category': row['cat_y'],
            'created_at': row['create_date'].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': row['update_date'].to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'),
            'body': row['body'],
            'img': row['cat_x'],
            'urls': row['cat'],
            'comments': 0,
            'upvotes':row['upvotes'],
            'downvotes':row['downvotes']
        }
        json_data["articles"].append(json_fnl)

    return json_data


