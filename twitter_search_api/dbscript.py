from datetime import date, timedelta, datetime
from pymongo import MongoClient

connection = MongoClient('localhost', 27017)
db = connection.londonterract_group1

for tw in db.tweets.find():
    print(tw['created_at'])
    date_unixtime = datetime.strptime(tw['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timestamp()
    db.tweets.update({'_id': tw['_id']}, {'$set': {'created_at_unixtime': date_unixtime}})

db.runCommand({
  create: "postFrequency",
  viewOn: "tweets",
  pipeline: [{'$match': {'is_retweeted':'False'}}, {'$group': {'_id': '$user', 'count': {'$sum': 1}}}, {'$sort': {'count': -1}}]
})

db.tweets.ensureIndex({'is_retweeted':1, 'user':1})
db.tweets.ensureIndex({'hashtags':1, 'user':1})
db.tweets.ensureIndex({'user':1})
db.tweets.ensureIndex({'is_retweeted':1})
db.tweets.ensureIndex({'retweeted_status':1})
db.tweets.ensureIndex({'created_at_unixtime':1})
db.users_timelines.ensureIndex({'user':1})
db.users_timelines.ensureIndex({'created_at_unixtime':1})
db.postFrequency.ensureIndex({'count':1})


db.tweets.aggregate( [
{'$match': {'is_retweeted':'True'}},
{"$lookup":
    { "from": "tweets",
     "localField": "retweeted_status",
    "foreignField": "_id",
    "as": "graph"}},
{'$project': {
      "_id": 0,
      "is_retweeted":  "$graph.is_retweeted",
      "Source": "$user",
      "Target": "$graph.user",
     "retweeted_status": 1}},
{'$out': 'graph'}] )

cursor = db.graph.find({'Target':[]})
print('Нет оригиналов: ' + cursor.count())
search_tw_info(cursor, db)

db.tweets.aggregate( [
{'$match': {'is_retweeted':'True'}},
{"$lookup":
    { "from": "tweets",
     "localField": "retweeted_status",
    "foreignField": "_id",
    "as": "graph"}},
{'$unwind': "$graph" },
{'$project': {
      "Source": "$user",
      "Target": "$graph.user"}},
{'$out': 'edges'}] )

db.graph.drop()

cursor = db.edges.find({'Target':[]})
print('Нет оригиналов после обновления: ' + cursor.count())


db.edges.aggregate( [
{"$lookup":
    { "from": "users",
     "localField": "Source",
     "foreignField": "_id",
     "as": "user"}},
{'$unwind': "$user" },
{'$project': {
      "Id": "$user._id",
      "Label": "$user.screen_name"}},
{'$out': 'nodessource'}] )


db.edges.aggregate( [
{"$lookup":
    { "from": "users",
     "localField": "Target",
     "foreignField": "_id",
     "as": "user"}},
{'$unwind': "$user" },
{'$project': {
      "Id": "$user._id",
      "Label": "$user.screen_name"}},
{'$out': 'nodestarget'}] )
#mongoexport --db londonterract_group1 --collection edges --type=csv --fields Source,Target --out api/csv/edges_group1.csv
#mongoexport --db londonterract_group1 --collection nodessource --type=csv --fields Id,Label --out api/csv/nodes_group1.csv
#mongoexport --db londonterract_group1 --collection nodestarget --type=csv --fields Id,Label --out api/csv/nodes1_group1.csv

db.edges.drop()
db.nodestarget.drop()
db.nodessource.drop()

db.tweets.aggregate( [
{'$match': {'is_retweeted':'True'}},
{
   $graphLookup: {
      from: "tweets",
      startWith: "$retweeted_status",
      connectFromField: "retweeted_status",
      connectToField: "_id",
      as: "graph",
      maxDepth: 0,
      restrictSearchWithMatch: {'geo':{"$ne":null}}
   }
},
{'$unwind': "$graph" },
{'$project': {
      "Target": "$user",
      "Source": "$graph.user",
      "Latitude":{ $arrayElemAt: [ "$graph.geo.coordinates", 0 ] },
      "Longitude": { $arrayElemAt: [ "$graph.geo.coordinates", 1 ] }}
},
{'$out': 'edgesgeo'}])

db.edgesgeo.aggregate( [
{"$lookup":
    { "from": "users",
     "localField": "Source",
     "foreignField": "_id",
     "as": "user"}},
{'$unwind': "$user" },
{'$project': {
      "Id": "$user._id",
      "Label": "$user.screen_name",
      "Latitude":1,
      "Longitude":1
}},
{'$out': 'nodessourcegeo'}] )


db.edgesgeo.aggregate( [
{"$lookup":
    { "from": "users",
     "localField": "Target",
     "foreignField": "_id",
     "as": "user"}},
{'$unwind': "$user" },
{'$project': {
      "Id": "$user._id",
      "Label": "$user.screen_name",
      "Latitude":1,
      "Longitude":1
}},
{'$out': 'nodestargetgeo'}] )
      

#mongoexport --db londonterract_group1 --collection edgesgeo --type=csv --fields Source,Target --out csv/edgesgeo_group1.csv
#mongoexport --db londonterract_group1 --collection nodestargetgeo --type=csv --fields Id,Label,Latitude,Longitude --out csv/nodesgeo_group1.csv
#mongoexport --db londonterract_group1 --collection nodessourcegeo --type=csv --fields Id,Label,Latitude,Longitude --out csv/nodes1geo_group1.csv
