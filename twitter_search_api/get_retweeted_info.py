from TwitterAPI import TwitterAPI

def collect_retweets_of_tweet(id, api, to_print = False):
    try:
        retweets = api.request('statuses/retweets/:%i' % id)

        if to_print:
            count=0
            data = retweets.json()
            print(data)

            for tweet in data:
                count=count+1
                print('@%s tweeted: %s, %i' % (tweet['user']['screen_name'], tweet['text'], tweet['id']))
            print('count = %i' % count)
        return retweets

    except Exception as e:
        print('Stopping: %s' % str(e))
        return -1


def collect_retweeters_of_tweet(id, api, to_print = False):
    try:
        count=0
        next_cursor = -1
        retweeters_ids = []

        while next_cursor != 0:

            retweeters = api.request('statuses/retweeters/ids', {'id': id, 'next_cursor': next_cursor})
            data = retweeters.json()
            next_cursor = data['next_cursor']

            for item in data['ids']:
                count = count + 1

                retweeters_ids.append(item)


            if to_print:
                print(data)
                for item in retweeters_ids:
                    print("id = %s" % item)
                print("count = %i" % (count))

        return retweeters_ids

    except Exception as e:
        print('Stopping: %s' % str(e))
        return -1
