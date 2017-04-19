import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
import time

def save_posts(db, posts):
    count_posts = 0
    for post in posts:
        print(post['id'], ' ', datetime.fromtimestamp(int(post['date'])).strftime('%Y-%m-%d %H:%M:%S'))
        db.posts.update({'_id': post['id']},
                     {'$set': {'created_at': datetime.fromtimestamp(int(post['date'])).strftime('%Y-%m-%d %H:%M:%S'),
                                'creted_at_unixtime': post['date'],
                                '_id': post['id'],
                                'text': post['text'],
                                'from_id': post['from_id'],
                                'user': post['owner_id'],
                                'retweets_count': post['reposts']['count'],
                                'retweets_count': post['reposts']['count'],
                                'comments_count': post['comments']['count'],
                                'favorite_count': post['likes']['count']
                                #, 'geo': post['geo']
                               }})
        count_posts = count_posts + 1
    '''
        try:
            db.posts.update({'signer_id ': post['signer_id']})
        except Exception as e:
            print(e)

        try:
            db.posts.update({'marked_as_ads': post['marked_as_ads']})
        except Exception as e:
            print(e)

        try:
            db.posts.update({'copy_history': post['copy_history']})
        except Exception as e:
            print(e)
    '''

    return count_posts

def save_groups(db, groups):
    for group in groups:
        db.groups.update({'_id': group['id']},
                {'$set': {'name':group['name'],
                          'screen_name':group['screen_name'],
                          'is_closed':group['is_closed'],
                          'type':group['type']
                          }})

def find_max_id(db, search_string):
    try:
        return db.meta.find({'_id':'max_id'})[0][search_string]
    except Exception as e:
        db.meta.save({'_id':'max_id', search_string:0})
        return db.meta.find({'_id':'max_id'})[0][search_string]

def search_posts(db, tag, token, meta_name, count, start_time_unix, end_time_unix):

    try:
        #get url
        method_search = 'https://api.vk.com/method/newsfeed.search?q={}&extended=1&fields=1&count={}&start_time={}&end_time={}&start_from={}$access_token={}&v=5.58'
        max_id = find_max_id(db, meta_name)
        url = method_search.format(tag, count, start_time_unix, end_time_unix, max_id, token)

        r = requests.get(url)
        if r.status_code == 200:

            rj = r.json()['response']

            if len(rj['items']) != 0:
                count_posts = save_posts(db, rj['items'])
                save_groups(db, rj['groups'])
                #save user
                #save_profiles

                db.meta.update({'_id': 'total count'}, {'$set': {meta_name: rj['total_count']}})
                try:
                    db.meta.update({'_id': 'max_id'}, {'$set': {meta_name: rj['next_from']}})
                    print(max_id, ' - ', db.meta.find({'_id': 'max_id'})[0][meta_name])
                except Exception:
                    print('The end of discussion')
                    return [count_posts, 'end']

                return [count_posts, 'next']
            else:
                print('Empty response')
                return [0, 'empty']
        else:
            print('response status: %i' % r.status_code)
            return [0, 'status_response']

    except Exception as e:
        print('error: %s' % e)
        return [0, 'error']

def get_posts(tokens, db, tag, count = 50, start_time = '', end_time = ''):

    #add sharp
    tag = tag.strip().lstrip('%23')

    #convert datetime
    start_time = (datetime.now() - timedelta(days=1) if start_time == '' else datetime.strptime(start_time, "%Y-%m-%d"))
    start_time_unix = start_time.timestamp()
    end_time = (datetime.now() + timedelta(days=1) if end_time == '' else datetime.strptime(end_time, "%Y-%m-%d")+ timedelta(days=1))
    end_time_unix = end_time.timestamp()

    meta_name = tag + ' ' + start_time.strftime('%Y-%m-%d %H:%M:%S')

    count_posts = 0
    count_tokens = 0
    while 1:
        token = tokens[count_tokens % len(tokens)]
        res = search_posts(db, tag, token, meta_name, count, start_time_unix, end_time_unix)
        count_posts = count_posts + res[0]

        if not res[1]:
            #time.sleep(2)
            count_tokens = count_tokens + 1
    print(count_posts)

connection = MongoClient('localhost', 27017)
db = connection.lovestory11
tokens = ['9d1f124a9d1f124a9d4c74ccf99d45099599d1f9d1f124ac5a66859c033d42963a468ba']
get_posts(tokens, db, 'шурыгина', start_time='2017-03-9', end_time='2017-03-11')

#user_access_token = '32c0099a4518f2475aee897c327b23b6fa96df635e0c7c31df32aeeaea4d7fa0f135c6220a2bd92928c79'
#app_service_key = '9d1f124a9d1f124a9d4c74ccf99d45099599d1f9d1f124ac5a66859c033d42963a468ba'