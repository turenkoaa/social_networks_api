from TwitterAPI import TwitterAPI
from datetime import date, timedelta, datetime
from exceptions import EXCEPTIONS
from keys import get_key
import time

def save_tweet_to_db(db, tw, id):
        db.users_timelines.update(
                          {'_id':tw['id']},
                          {'$set': {
                              'text':tw['text'],
                              'contributors':tw['contributors'],
                              'user':id,
                              'coordinates':tw['coordinates'],
                              'place':tw['place'],
                              'lang':tw['lang'],
                              'created_at':tw['created_at'],
                              'created_at_iso':tw['created_at'],
                              'retweet_count':tw['retweet_count'],
                              'favorite_count':tw['favorite_count'],
                              'is_quote_status':tw['is_quote_status'],
                              'source':tw['source'],
                              'geo':tw['geo'],
                              'favorited': tw['favorited'],
                              'retweeted':tw['retweeted'],
                              'entities':tw['entities'],
                              'in_reply_to_status_id':tw['in_reply_to_status_id']}},
                        upsert=True
                        )
        date_unixtime = datetime.strptime(tw['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timestamp()
        db.users_timelines.update({'_id': tw['id']}, {'$set': {'created_at_unixtime': date_unixtime}})


def find_max_id(db, id):
    try:
        return db.meta_users_timeline.find({'_id':id})[0][max_id]
    except Exception as e:
        db.metameta_users_timeline.update({'_id': id}, {'$set': {'max_id': 0}})
        return 0

def search_tweets(consumer_key, consumer_secret, access_token, access_token_secret, limit_tweets, db, id, since_id, count=200):

    print('start search')
    count_tweets = 0

    try:
        api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)
        r = api.request('statuses/user_timeline', {'user_id': id, 'count': count})
        data = r.json()
        if (data == []):
            print(data)
            print("The end")
            return [True, count_tweets]
        else:
            for tw in data:
                print(tw['created_at'])
                save_tweet_to_db(db, tw, id)
                count_tweets += 1
            max_id = data[-1]['id'] - 1
            db.metameta_users_timeline.update({'_id': id}, {'$set': {'max_id': max_id}})
            print(count_tweets)

        while 1 and db.users_timelines.find({'user':id}).count() <= limit_tweets:

            print('max_id = %s' % max_id)
            r = api.request('statuses/user_timeline', {'user_id': id, 'max_id':max_id, 'count':count})
            data = r.json()
            if (data==[]):
                print(data)
                print("The end")
                return [True, count_tweets]
            else:
                for tw in data:
                    print(tw['created_at'])
                    save_tweet_to_db(db, tw, id)
                    count_tweets += 1
                max_id = data[-1]['id'] - 1
                db.metameta_users_timeline.update({'_id':id}, {'$set': {'max_id':max_id}})
                print(count_tweets)

        return [True, count_tweets]

    except Exception as e:
        print(data)
        print('error: %s' % e)
        print('response status: %i - %s' % (r.status_code, EXCEPTIONS[r.status_code]))
        if r.status_code == 401:
            return [True, count_tweets]
        return [False, count_tweets]

def get_timeline(db, id, limit_tweets, since_id = 0):

    count_tweets = db.users_timelines.find({'user':id}).count()
    count = 0
    while count <= 1000 and db.users_timelines.find({'user':id}).count() <= limit_tweets:
        key = get_key(count)
        res = search_tweets(key[0], key[1], key[2], key[3], limit_tweets, db, id, since_id)
        if not res[0]:
            time.sleep(20)
            count = count + 1
            count_tweets = count_tweets + res[1]
        else:
            break
    print(count_tweets)


