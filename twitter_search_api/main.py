from pymongo import MongoClient
from get_excel_twitter import ExcelWriter
import sys

connection = MongoClient('localhost', 27017)
db = connection.londonterract_group1

#workbook = xlsxwriter.Workbook('excel/london1.xlsx')
#worksheet = workbook.add_worksheet()

HASHTAGS1 = ['#PrayForLondon','#LondonStrong','#WeAreNotAfraid','#LondonIsOpen','#WeStandTogether','#TrafalgarSquare','#ProudToBebritish','#PrayforUK']

writer = ExcelWriter(db, 'london2.xlsx', HASHTAGS1, "2017-3-22 00:00:00", "2017-3-24 23:59:59")

writer.active_user_to_excel(20, 10, "HOURS")
writer.count_authority_users_of_publish_activity_to_excel()
writer.active_user_timeline(20, 1000, 'HOURS', "2017-3-15 00:00:00", "2017-3-21 23:59:59")
writer.active_user_timeline(20, 1000, 'HOURS', "2017-3-25 00:00:00", "2017-4-6 23:59:59")
writer.users_categories_to_excel(200)
writer.tweets_to_excel(200)
writer.publish_actitity_to_excel("HOURS")
writer.user_autority_to_excel(30, "HOURS")

del writer



'''
#arg = sys.argv[1]

HASHTAGS = ['#PrayForLondon',
            '#londonattack',
            '#LondonStrong',
            '#WeAreNotAfraid',
            '#LondonIsOpen',
            '#WeStandTogether',
            '#londres',
            '#Westminster',
            '#WestminsterAttack',
            '#TrafalgarSquare',
            '#terroristattack',
            '#ParliamentAttack',
            '#ProudToBebritish',
            '#london',
            '#PrayforUK',
            'Scotland Yard',
            'London Ambulance Service',
            '#999family' ]

connection = MongoClient('localhost', 27017)
db = connection.londonterract


get_tweets('"Scotland Yard"', db, 'Mar-21-2017')'''
