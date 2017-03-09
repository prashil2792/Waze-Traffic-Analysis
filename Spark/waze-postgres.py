from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import desc, explode
from pyspark.sql import SQLContext, SparkSession
import datetime
import pandas as pd
import psycopg2
import os
import yaml
import json

APP_NAME = "Waze Traffic Notifications"

def convert_toDate(ts):
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    date = st.split(' ')[0]
    return date

def main():
	df = spark.read.json("s3a://wazedata/2017/03/*/*/*")
	#s3a://wazedata/2017/03/02/00

	# create dataframe for event_types
	event_types = df.select(df['time_stamp'], explode(df['alerts.type']))

	# create dataframe for event_subtypes
	event_subtypes = df.select(df['time_stamp'].alias("time"), explode(df['alerts.subtype']))

	# joining two tables
	df = event_types.join(event_subtypes, event_types.time_stamp== event_subtypes.time).drop("time")

	# convert to pandas and rename the columns
	df = df.toPandas()
	df.columns = ['time_stamp', 'type', 'subtype']

	# replace empty values with others
	df['subtype'].replace("", "OTHERS", inplace=True)

	#convert timestamp to date
	df['time_stamp'] = df['time_stamp'].apply(convert_toDate)

	# getting desired dataframes
	# table for event_subtypes
	final_event_subtypes = df.groupby(['time_stamp','type','subtype']).count().reset_index()
	final_event_subtypes['count'] = df.groupby(['time_stamp','type','subtype']).size().values

	# table for event_types
	final_events = df.groupby(['time_stamp','type']).count().reset_index()

	# Inserting records to postgres database
	rds_credentials = yaml.load(open(os.path.expanduser('~/postgres_cred.yml')))
	DB_NAME = rds_credentials['waze_postgres']['database']
	DB_user = rds_credentials['waze_postgres']['user']
	DB_password = rds_credentials['waze_postgres']['password']
	DB_host = rds_credentials['waze_postgres']['host']
	DB_port = rds_credentials['waze_postgres']['port']

	try:
		conn = psycopg2.connect(database=DB_NAME, user=DB_user, password=DB_password, host=DB_host, port=DB_port)
		print "Opened database successfully"

	except:
		print "Unable to connect to database"

	cur = conn.cursor()
	counter = 0

	# print(final_event_subtypes.values[0])
	# print(final_event_subtypes.info())

	for data in final_event_subtypes.values:
		#ts = str(data[0])
		cur.execute("INSERT INTO event_subtype (time_stamp, type, subtype,count) VALUES ('{}','{}','{}',{})".format(data[0],data[1],data[2],data[3]))

		counter += 1
		if counter % 20 == 0:
			conn.commit()
			print "Inserted {} records".format(counter)

	conn.commit()
	print "Records inserted successfully"

	counter = 0
	for data in final_events.values:
		#ts = str(data[0])
		cur.execute("INSERT INTO event_type (time_stamp, type, count) VALUES ('{}','{}',{})".format(data[0],data[1],data[2]))

		counter += 1
		if counter % 20 == 0:
			conn.commit()
			print "Inserted {} records".format(counter)

	conn.commit()
	print "Records inserted successfully"

	conn.close()


if __name__ == "__main__":
    # Configure spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    # filename = sys.argv[1]
    # Execute Main functionality
    main()