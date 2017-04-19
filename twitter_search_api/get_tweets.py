from TwitterAPI import TwitterAPI
from datetime import date, timedelta, datetime
from exceptions import EXCEPTIONS
from keys import get_key
import time


def save_tweet_to_db(db, tw, search_string = "missing in search"):
        #date = datetime.strptime(tw['created_at'], '%a %b %d %H:%M:%S +0000 %Y').strftime("%d.%m.%Y")
        date_unixtime = datetime.strptime(tw['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timestamp()
        db.tweets.update( {'_id':tw['id']},
                          {'$set': {
                              'text':tw['text'],
                              'contributors':tw['contributors'],
                              'user':tw['user']['id'],
                              'coordinates':tw['coordinates'],
                              'place':tw['place'],
                              'lang':tw['lang'],
                              'created_at':tw['created_at'],
                              'created_at_iso':tw['created_at'],
                              #'date':date,
                              'retweet_count':tw['retweet_count'],
                              'favorite_count':tw['favorite_count'],
                              'is_quote_status':tw['is_quote_status'],
                              'source':tw['source'],
                              'geo':tw['geo'],
                              'entities':tw['entities'],
                              'created_at_unixtime': date_unixtime,
                              'in_reply_to_status_id':tw['in_reply_to_status_id']}},
                        upsert=True
                        )


        db.users.update({'_id': tw['user']['id']},
                         {'$set': {
                             'screen_name': tw['user']['screen_name'],
                             'name': tw['user']['name'],
                             'location': tw['user']['location'],
                             'created_at': tw['user']['created_at'],
                             'favourites_count': tw['user']['favourites_count'],
                             'followers_count': tw['user']['followers_count'],
                             'statuses_count': tw['user']['statuses_count'],
                             'friends_count': tw['user']['friends_count'],
                             'url': tw['user']['url'],
                            #"default_profile": info['default_profile'],
                             'listed_count': tw['user']['listed_count'],
                             'lang':tw['user']['lang'],
                             'geo_enabled': tw['user']['geo_enabled'],
                             'description': tw['user']['description'],
                             'verified': tw['user']['verified'],
                             'time_zone': tw['user']['time_zone'],
                            'protected': tw['user']['protected'],
                            'entities':tw['entities']}},
                         upsert=True
                         )

        db.tweets.update({'_id': tw['id']}, {'$push': {'search_strings': search_string}})



        try:
            db.tweets.update({'_id': tw['id']},
                             {'$set': {'retweeted_status': tw['retweeted_status']['id'],  'is_retweeted': 'True'}}) #является ретвитом!!!!!!!!!!!!!! так сделано, чтобы не изменять в других документах
        except Exception:
            db.tweets.update({'_id': tw['id']}, {'$set': {'is_retweeted': 'False'}})



        #quote
        #if tw['is_quote_status']:
        #    db.tweets.update({'_id': tw['id']}, {'$set': {'quoted_status_id': tw['quoted_status_id_str']}})

        try:
            for hash in tw['entities']['hashtags']:
                db.tweets.update({'_id': tw['id']}, {'$push': {'hashtags': hash['text']}})
        except Exception as e:
            print(e)


def find_max_id(db, search_string):
    try:
        return db.meta.find({'_id':'max_id'})[0][search_string]
    except Exception as e:
        db.meta.save({'_id':'max_id', search_string:0})
        return db.meta.find({'_id':'max_id'})[0][search_string]

def search_tweets(consumer_key, consumer_secret, access_token, access_token_secret, search_string,
                  db, date, until, since_id, count=150):

    meta_name = search_string + ' ' + date
    max_id = find_max_id(db, meta_name)
    count_tweets = 0

    try:
        api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

        while 1:
            print('max_id = %s' % max_id)
            r = api.request('search/tweets', {'q': search_string, 'until': until, 'max_id': max_id, 'count': count, 'since_id':since_id})
            data = r.json()
            if (data['statuses']==[]):
                print("The end of discussion")
                return [True, count_tweets]
            else:
                for tw in data['statuses']:
                    save_tweet_to_db(db, tw, search_string)
                    print(meta_name + ': ' + tw['id_str'] + '. ' + tw['created_at'])
                    count_tweets = count_tweets+1

                max_id = data['statuses'][-1]['id'] - 1
                db.meta.update({'_id':'max_id'}, {'$set': {meta_name:max_id}})
                print(count_tweets)

    except Exception as e:
        print('error: %s' % e)
        print('response status: %i - %s' % (r.status_code, EXCEPTIONS[r.status_code]))
        return [False, count_tweets]

def get_tweets(search_string, db, date = '', until=date.today() + timedelta(days=1), since_id = 0):

    if date != '':
        user_date = datetime.strptime(date, '%b-%d-%Y')
        until = user_date + timedelta(days=1)
        key = get_key(0)
        while 1:
            try:
                api = TwitterAPI(key[0], key[1], key[2], key[3])
                r = api.request('search/tweets', {'q': search_string, 'until': user_date.strftime("%Y-%m-%d"), 'max_id': 0, 'count': 1, 'since_id':0})
                data = r.json()
                if (data['statuses'] != []):
                    since_id = data['statuses'][0]['id']
                break
            except Exception as e:
                print('error: %s' % e)
                print('response status: %i - %s' % (r.status_code, EXCEPTIONS[r.status_code]))
                time.sleep(20)

    count_tweets = db.tweets.find().count()
    count = 0
    while 1:
        key = get_key(count)
        res = search_tweets(key[0], key[1], key[2], key[3], search_string, db, date, until.strftime("%Y-%m-%d"), since_id)
        if not res[0]:
            time.sleep(20)
            count = count + 1
            count_tweets = count_tweets + res[1]
        else:
            break

    print(count_tweets)

