from random import randint

def get_list(text_file):
    with open(text_file, 'r') as file:
        list = file.read()
    return list.split('\n')

def get_table_ids(cursor, tablename):
    cursor.execute("SELECT * FROM " + tablename)
    data = list(map(lambda x: x[0], cursor.fetchall()))
    return data

def get_random_value(collection):
    return collection[randint(0, len(collection) - 1)]
