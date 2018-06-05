from pymongo import MongoClient

#Run the startMongoDb.bat before executing this (Install MongoDb on the machine)
#Details will be migrated to the config.ini file
client = MongoClient('localhost',27017)
db = client.crypto_analyzer

posts = db.posts
post_data = {
    'name' = 'Crpto-Analayzer',
    'author' = 'Bhalu'
}
result = posts.insert_one(post_data)
print('Demo: {0}'.format(result.inserted_id))

