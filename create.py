import pandas as pd
from pymongo import MongoClient
import time
import csv


start = time.time()

rows = 100000

df_2019 = pd.read_csv('data/Odata2019File.csv', nrows=rows, sep=';', encoding='cp1251')
df_2020 = pd.read_csv('data/Odata2020File.csv', nrows=rows, sep=';', encoding='cp1251')
df_2019['year'] = 2019
df_2020['year'] = 2020

client = MongoClient('localhost', 27017)

db = client['zno']
collection = db['zno']

posts = collection.posts
posts.insert_many(df_2019.to_dict('recodrs'))
posts.insert_many(df_2020.to_dict('records'))


def db_select(posts):
    pipeline = [
        {"$match": {"mathTestStatus": "Зараховано"}},
        {"$group": {"_id": {"region": "$REGNAME", "year": "$year"},
                    "avg": {"$avg": "$mathBall"}}}]

    results = list(posts.aggregate(pipeline))
    with open('data/result.csv', 'w') as w:
        csv_out = csv.writer(w)
        csv_out.writerow(['region', 'year', 'ball'])
        for row in results:
            line = []
            line.append(row['_id']['region'])
            line.append(row['_id']['year'])
            line.append(row['avg'])
            csv_out.writerow(line)


db_select(posts)
print(start, 'seconds')



