import xlsxwriter
from pymongo import MongoClient
from pymongo import DESCENDING
from datetime import datetime
from get_user_timeline import get_timeline

delta = {
    "SECONDS": 1,
    "MINUTES": 60,
    "HOURS": 3600,
    "DAYS": 3600 * 24,
    "WEEKS": 3600 * 24 * 7
}



time_interval = {'SOME_DATE', 'DISCUSSION_DATE', 'DATE_TIMELINE'}

#везде поиск по нужному времени
#вывод графиков для пользователей за пределами дискуссии
#'Users authority' сортировка по частоте публицаций и выдели для самых активных
#добавь описание какой период расматривается
#для каждого авктивного определенный интересующий отрезок времени timeline



class ExcelWriter:
    def __init__(self, db, workbook_path, hashtags, date_from, date_to, no_date_from= 0, no_date_to= 0):
        self.db = db
        self.workbook = xlsxwriter.Workbook(workbook_path)
        self.date_from = date_from
        self.date_to = date_to
        self.no_date_from = no_date_from
        self.no_date_to = no_date_to
        self.hashtags = hashtags
        self.bold = self.workbook.add_format({'bold': True})
        self.schedule = {'TIMELINE_TWEETS': self.db.users_timelines, 'DISCUSSION_TWEETS': self.db.tweets}

    def __del__(self):
        self.workbook.close()

    def get_user(self, id):
        return self.db.users.find({'_id':id})[0]

    def write_list(self, worksheet, list, list_name, row, col):
        worksheet.write(row, col, list_name)
        for item in list:
            worksheet.write(row, col+1, item)
            row += 1
        return [row, col]

    def add_chart(self, worksheet, WSNAME, row, col, headings, data, name=0):

        if name == 0:
            name = WSNAME
        frow = row + 1
        lrow = len(data[0]) + frow

        worksheet.write_row(row, col, headings, self.bold)
        worksheet.write_column(frow, col, data[0])
        worksheet.write_column(frow, col+1, data[1])


        chart = self.workbook.add_chart({'type': 'line'})
        chart.add_series({
            'name': [WSNAME, 0, 0],
            'categories': [WSNAME, frow, col, lrow, col],
            'values': [WSNAME, frow, col+1, lrow, col+1],
        })

        chart.set_title({'name': name})
        chart.set_x_axis({'name': headings[0]})
        chart.set_y_axis({'name': headings[1]})

        chart.set_style(10)

        worksheet.insert_chart(frow, col+3, chart, {'x_offset': 150, 'y_offset': 100})

        return lrow+1, col

    def get_collection(self, cursor, str1, str2):
        data = [[], []]
        for document in cursor:
            data[0].append(document[str1])
            data[1].append(document[str2])
        return data

    def add_meta_info(self, worksheet):

        worksheet.write('A1', 'Meta info', self.bold)

        worksheet.set_column(0, 1, 15)
        worksheet.set_column(0, 2, 15)

        cell = self.write_list(worksheet, self.hashtags, 'hashtags:', 1, 0)
        cell = self.write_list(worksheet, [self.date_from, self.date_to], 'dates:', cell[0], cell[1])

        cursor = self.db.tweets.find({'is_retweeted':'False'}).count()
        cell = self.write_list(worksheet, [cursor], 'tweets count without reposts:', cell[0], cell[1])

        cursor = self.db.postFrequency.find().count()
        cell = self.write_list(worksheet, [cursor], 'users count without reposts:', cell[0], cell[1])

        cursor = self.db.tweets.find().count()
        cell = self.write_list(worksheet, [cursor], 'tweets coun with reposts:', cell[0], cell[1])

        cursor = self.db.users.find().count()
        cell = self.write_list(worksheet, [cursor], 'users count with reposts:', cell[0], cell[1])

        cell[0] += 2
        worksheet.set_column(0, 2, 30)
        return cell

    def save_active_user_info(self, worksheet, user, cell):
        try:
            cell = self.write_list(worksheet, [user['screen_name']], 'Screen name:', cell[0], cell[1])
            cell = self.write_list(worksheet, ["https://twitter.com/" + user['screen_name']], 'Url:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['name']], 'Name:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['location']], 'Location:', cell[0], cell[1])
            date = datetime.strptime(user['created_at'], '%a %b %d %H:%M:%S +0000 %Y').strftime(
                '%H:%M:%S %b %d %Y %a ')
            cell = self.write_list(worksheet, [date], 'Created at:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['lang']], 'Language:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['favourites_count']], 'Favourites count:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['followers_count']], 'Followers count:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['statuses_count']], 'Statuses count:', cell[0], cell[1])
            cell = self.write_list(worksheet, [user['friends_count']], 'Friends count:', cell[0], cell[1])
        except Exception as e:
            cell = self.write_list(worksheet, ['User has been suspended.'], 'No information:', cell[0] + 1, cell[1])
        return cell

    def show_tweets(self, worksheet, cell, collection, user, limit_tweets):

        if limit_tweets>200:
            limit_tweets = 200
        cursor1 = collection.find({'user': user['_id']}).limit(limit_tweets)
        worksheet.write(cell[0], cell[1], 'Tweets:', self.bold)
        data = self.get_collection(cursor1, 'created_at', 'text')
        cell[0] += 1
        worksheet.write_row(cell[0], cell[1], ['date', 'text'])
        worksheet.write_column(cell[0] + 1, cell[1], data[0])
        worksheet.write_column(cell[0] + 1, cell[1] + 1, data[1])
        cell[0] += limit_tweets + 2

        return cell


    def active_user_timeline(self, limit_users, limit_tweets, period, date_from=0):
        print('Active users timeline')

        WSNAME = 'Timelines from ' + datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S').strftime("%d-%m") \
                 #+ datetime.strptime(date_to, '%Y-%m-%d %H:%M:%S').strftime("%d-%m")
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)

        worksheet.write(cell[0], cell[1], 'Returns timeline of active user ('+str(limit_tweets) + ' tweets).', self.bold)
        cell[0] += 1

        cursor = self.db.postFrequency.aggregate([{'$limit':limit_users},
                {'$lookup':{'from': "users",'localField': "_id", 'foreignField': "_id", 'as': "user_data"}}])

        date_to = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dates = self.get_time_periods(period, date_from, date_to)

        for document in cursor:
            user = document['user_data'][0]
            cell = self.write_list(worksheet, [document['count']], 'Posts frequency:', cell[0], cell[1])
            cell = self.save_active_user_info(worksheet, user, cell)

            get_timeline(self.db, user['_id'], limit_tweets)
            cell = self.show_tweets(worksheet, cell, self.db.users_timelines, user, limit_tweets)

            dates = self.get_data_for_chart(self.db.users_timelines, user, dates, period)
        self.add_charts(WSNAME, dates)

        return

    def users_categories_to_excel(self, limit_users):

        print('All hashs')
        WSNAME = 'All hashs'
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)
        cell[0] += 2

        worksheet.write(cell[0], cell[1], 'Returs all hashtags included in the considered tweet. (1000 random tweets were considered)', self.bold)
        cell[0]+=1

        cursor_group = self.db.tweets.aggregate([{'$match': {'hashtags':{'$ne':[]}}},
                                                 {'$group': {'_id': '$user', 'hashs': {'$push': '$hashtags'}, 'search_strs':{'$push': '$search_strings'}}},
                                                 {'$limit' :limit_users},
                                                 {'$lookup': {'from': "users", 'localField': "_id",'foreignField': "_id", 'as': "user_data"}}],
                                                allowDiskUse=True)

        data = [[], [], [], [], [], []]
        for user in cursor_group:
            user_cursor = user['user_data'][0]
            try:
                date = datetime.strptime(user_cursor['created_at'], '%a %b %d %H:%M:%S +0000 %Y').strftime('%H:%M:%S %b %d %Y %a ')
                data[0].append(date)
            except:
                data[0].append('-')
            try:
                data[1].append(user_cursor['screen_name'])
                data[2].append("https://twitter.com/" + user_cursor['screen_name'])
            except:
                data[1].append('-')
                data[2].append('-')

            hash_dict = {}
            for array in user['hashs']:
                for item in array:
                    hash_dict[item] = 1
            hashs = ''
            for key in hash_dict:
                hashs = hashs + '#' + key + ' '

            search_str_dict = {}
            for array in user['search_strs']:
                for item in array:
                    search_str_dict[item] = 1
            search_strs = ''
            for key in search_str_dict:
                search_strs = search_strs + ' ' + key + ' '

            data[3].append(search_strs)
            data[4].append(hashs)

        worksheet.write_row(cell[0], cell[1], ['registration date', 'user', 'url', 'searching hash', 'all hashs'], self.bold)
        for i in [0, 1, 2, 3, 4]:
            worksheet.set_column(0, i, 30)
            worksheet.write_column(cell[0] + 1, cell[1]+ i, data[i])
        return

    def count_authority_users_of_publish_activity_to_excel(self):
        #graph
        WSNAME = 'Count users of publish activity'
        worksheet = self.workbook.add_worksheet(WSNAME)
        row, col = self.add_meta_info(worksheet)
        worksheet.write(row, col, 'Returs the dependence of the number of users from publication activity.', self.bold)
        row+=1

        cursor = self.db.postFrequency.aggregate([{'$group': {'_id': '$count', 'count': {'$sum': 1}}},
                                          {'$sort': {'_id': 1}}])
        self.add_chart(worksheet, WSNAME, row, col, ['posts frequency', 'number of users'], self.get_collection(cursor, '_id', 'count'))
        return

    def active_user_to_excel(self, limit_users, limit_tweets, period=0, date_from=0, date_to=0):
        print('Active user')

        WSNAME = 'Active users'
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)

        worksheet.write(cell[0], cell[1],
                        'Returs information of active users (If the user has been suspended, there are no information about him, and the next active user will be returned).',
                        self.bold)
        cell[0] += 1
        worksheet.write(cell[0], cell[1], 'Active users:', self.bold)
        cell[0] += 1

        cursor = self.db.postFrequency.aggregate([{'$limit': limit_users},
                {'$lookup': {'from': "users", 'localField': "_id", 'foreignField': "_id", 'as': "user_data"}}])
        if period != 0:
            dates = self.get_time_periods(period, date_from, date_to)

        for document in cursor:
            user = document['user_data'][0]
            cell = self.write_list(worksheet, [document['count']], 'Posts frequency:', cell[0], cell[1])
            cell = self.save_active_user_info(worksheet, user, cell)
            cell = self.show_tweets(worksheet, cell, self.db.tweets, user, limit_tweets)
            if period != 0:
                dates = self.get_data_for_chart(self.db.tweets, user, dates, period)

        if period != 0:
            self.add_charts(WSNAME, dates)
        return

    def user_autority_to_excel(self, limit, period=0, date_from=0, date_to=0):

        WSNAME = 'Users authority'
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)

        worksheet.write(cell[0], cell[1], 'Returs top of the ' + str(limit) + ' most authority users (likes and retweets)', self.bold)
        cell[0]+=1

        cursor = self.db.tweets.aggregate([
          {'$match': {'is_retweeted': 'False'}},
          {'$group': {'_id': '$user', 'count_retweets': {'$sum': '$retweet_count'}, 'count_likes': {'$sum': '$favorite_count'}}},
          {'$sort': {'count_retweets': -1}}, {'$limit':limit},
          {'$lookup':{ 'from': "users", 'localField': "_id", 'foreignField': "_id", 'as': "user_data"
            }}])

        data = [[], [], [], [], []]
        if period != 0:
            dates = self.get_time_periods(period, date_from, date_to)

        for document in cursor:
            user = document['user_data'][0]

            if period != 0:
                dates = self.get_data_for_chart(self.db.tweets, user, dates, period)

            try :
                data[0].append(user['screen_name'])
                data[3].append(self.db.postFrequency.find({'_id':user['_id']})[0]['count'])
                data[4].append("https://twitter.com/" + user['screen_name'])
            except Exception as e:
                data[0].append('-')
                data[3].append('-')
                data[4].append('-')
            data[1].append(document['count_retweets'])
            data[2].append(document['count_likes'])

        if period != 0:
            self.add_charts(WSNAME, dates)

        worksheet.write_row(cell[0], cell[1], ['screen_name', 'retweets count', 'likes count', 'post frequency', 'url'], self.bold)
        for i in [0, 1, 2, 3, 4]:
            worksheet.set_column(0, i, 25)
            worksheet.write_column(cell[0] + 1, cell[1] + i, data[i])
        return

    def tweets_to_excel(self, limit):
        print('Tweets')

        WSNAME = 'Tweets'
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)

        worksheet.write(cell[0], cell[1], 'Returs random ' + str(limit) + ' tweets of discussion.', self.bold)
        cell[0] += 1

        ut_from = datetime.strptime(self.date_from, "%Y-%m-%d %H:%M:%S").timestamp()
        ut_to = datetime.strptime(self.date_to, "%Y-%m-%d %H:%M:%S").timestamp()
        # ut_no_from = datetime.strptime(self.no_date_from, "%Y-%m-%d %H:%M:%S").timestamp()
        # ut_no_to = datetime.strptime(self.no_date_to, "%Y-%m-%d %H:%M:%S").timestamp()
        # cursor = self.db.tweets.find({'is_retweeted':'False','created_at_unixtime': {'$gte': ut_from, '$lte': ut_to}}).limit(limit)
        cursor = self.db.tweets.aggregate(
            [{'$match': {'is_retweeted': 'False', 'created_at_unixtime': {'$gte': ut_from, '$lte': ut_to}}},
             {'$limit': limit},
             {'$lookup': {'from': "users", 'localField': "user", 'foreignField': "_id", 'as': "user_data"}}])

        data = [[], [], []]
        for tw in cursor:
            data[0].append(datetime.strptime(tw['created_at'], '%a %b %d %H:%M:%S +0000 %Y').strftime("%Y-%m-%d %H:%M:%S"))
            # cursor_user = self.db.users.find({'_id': tw['user']})
            user = tw['user_data'][0]
            try:
                data[1].append(user['screen_name'])
            except:
                data[1].append('-')
            data[2].append(tw['text'])

        worksheet.write_row(cell[0], cell[1], ['created at', 'screen_name', 'text'], self.bold)
        for i in [0, 1, 2]:
            worksheet.set_column(0, i, 25)
            worksheet.write_column(cell[0] + 1, cell[1] + i, data[i])
        return

    def publish_actitity_to_excel(self, period, date_from=0, date_to=0):  # for dates(year, mounth, weeks 3600*24*7, day 3600*24), time(hours 3600, minuties 60, seconds 1)

        if date_from == 0:
            date_from = self.date_from
        if date_to == 0:
            date_to = self.date_to

        print('Publish activity')
        WSNAME = 'Publish activity'
        worksheet = self.workbook.add_worksheet(WSNAME)
        cell = self.add_meta_info(worksheet)
        worksheet.write(cell[0], cell[1], 'Returs publication activity during the period under review. (each ' + period + ')', self.bold)
        cell[0] += 1

        ut_between = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S").timestamp()
        ut_to = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S").timestamp()

        data = [[], []]

        while ut_between < ut_to:
            ut_from = ut_between
            ut_between += delta[period]
            cursor = self.db.tweets.find({'created_at_unixtime': {'$gte': ut_from, '$lte': ut_between}}).count()
            date = datetime.fromtimestamp(ut_from).strftime("%Y-%m-%d %H:%M:%S")
            data[0].append(date)
            data[1].append(cursor)

        self.add_chart(worksheet, WSNAME, cell[0], cell[1], ['time', 'tweets count'], data)
        return

    def get_time_periods(self, period, date_from=0, date_to=0):
        if date_from == 0:
            date_from = self.date_from
        if date_to == 0:
            date_to = self.date_to

        dates = {'normal': [], 'unix': [], 'headings': ['time'], 'count': []}

        try:
            ut_between = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S").timestamp()
            ut_to = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S").timestamp()
            while ut_between < ut_to:
                ut_from = ut_between
                ut_between += delta[period]
                dates['unix'].append(ut_from)
                dates['normal'].append(datetime.fromtimestamp(ut_from).strftime("%Y-%m-%d %H:%M:%S"))

        except Exception as e:
            print("get time periods: " + e)

        return dates

    def get_data_for_chart(self, collection, user, dates, period):
        data = []
        for date in dates['unix']:
            cursor = collection.find({'user': user['_id'], 'created_at_unixtime': {'$gte': date, '$lte': date + delta[period]}}).count()
            data.append(cursor)
        try:
            dates['headings'].append(user['screen_name'])
        except:
            dates['headings'].append('user ' + str(i))
            i += 1
        dates['count'].append(data)

        return dates

    def add_charts(self, WSNAME, dates):
        cell = [0, 0]
        frow = cell[0] + 1
        lrow = len(dates['unix']) + frow

        WSNAME_chart = WSNAME + " chart"
        worksheet = self.workbook.add_worksheet(WSNAME_chart)

        worksheet.write_row(cell[0], cell[1], dates['headings'], self.bold)
        worksheet.write_column(frow, cell[1], dates['normal'])
        chart = self.workbook.add_chart({'type': 'line'})

        i = 1
        while i <= len(dates['count']):
            worksheet.write_column(frow, cell[1] + i, dates['count'][i - 1])
            chart.add_series({'categories': [WSNAME_chart, frow, cell[1], lrow, cell[1]],
                              'values': [WSNAME_chart, frow, cell[1] + i, lrow, cell[1] + i],
                              'name': [WSNAME_chart, cell[0], cell[1] + i]})
            worksheet.set_column(0, i, 25)
            i += 1

        chart.set_title({'name': WSNAME})
        chart.set_x_axis({'name': dates['headings'][0]})
        chart.set_y_axis({'name': 'count tweets'})

        chart.set_style(10)

        worksheet.insert_chart(frow, cell[1], chart, {'x_offset': 25, 'y_offset': 10})



