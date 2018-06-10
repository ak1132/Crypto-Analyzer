from influxdb import DataFrameClient
from influxdb import InfluxDBClient

'''
Basics of InfluxDB

Measurement is equivalent to table in SQL
Tag is a key-value pair of columns that get indexed by the database
Field is a key value pair of columns that do not get indexed by the database
'''

#example JSON
json_body = [
    {
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "value": 0.64
        }
    }
]

'''
client = DataFrameClient(host='localhost',port=8086,database='crypto_analyzer')
print("Create pandas DataFrame")
    df = pd.DataFrame(data=list(range(30)),
                      index=pd.date_range(start='2014-11-16',
                                          periods=30, freq='H'))
print("Write DataFrame")
client.write_points(df, 'demo', protocol=protocol)

print("Write DataFrame with Tags")
client.write_points(df, 'demo',
                        {'k1': 'v1', 'k2': 'v2'}, protocol=protocol)
'''

user = 'root'
password = 'root'
dbname = 'example'
dbuser = 'smly'
dbuser_password = 'my_secret_password'
query = 'select value from cpu_load_short;'
host='localhost'
port = 8086

client = InfluxDBClient(host, port, user, password, dbname)

print("Create database: " + dbname)
client.create_database(dbname)

print("Create a retention policy")
client.create_retention_policy('awesome_policy', '3d', 3, default=True)

print("Switch user: " + dbuser)
client.switch_user(dbuser, dbuser_password)

print("Write points: {0}".format(json_body))
client.write_points(json_body)

print("Querying data: " + query)
result = client.query(query)

print("Result: {0}".format(result))

print("Switch user: " + user)
client.switch_user(user, password)

print("Drop database: " + dbname)
client.drop_database(dbname)


