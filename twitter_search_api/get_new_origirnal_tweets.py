from TwitterAPI import TwitterAPI
from pymongo import MongoClient
from keys import get_key
from datetime import date, timedelta, datetime
from get_tweets import save_tweet_to_db

def search_tw_info(cursor, db):
    print('Search tweets')
    try:
        count = 0
        key_count = 0
        for i in range(0, cursor.count(), 100):
            while 1:
                try:
                    group = []
                    print(i)
                    for j in range(i, i + 100):
                        if j < cursor.count():
                            group.append(str(cursor[j]['retweeted_status']))
                    str_gr = ','.join(group)
                    key = get_key(key_count)
                    key_count += 1
                    api = TwitterAPI(key[0], key[1], key[2], key[3])
                    r = api.request('statuses/lookup', {'id': str_gr})
                    tws = r.json()
                    for data in tws:
                        save_tweet_to_db(db, data)
                        count += 1
                        print(data['id'])
                    break
                except Exception as e:
                    print('error: %s' % e)

        print(str(count) + ' - ' + str(cursor.count()))

    except Exception as e:
           print(e)


connection = MongoClient('localhost', 27017)
db = connection.londonterract_group1


cursor = db.graph.find({'Target':[]})
search_tw_info(cursor, db)
