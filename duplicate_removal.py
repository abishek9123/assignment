import json

from pyspark import SparkContext, SparkConf, SQLContext

appName = "JSON Parse to Parquet File"
master = "local[2]"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# function to create final output
def remove_duplicates(file_name):
	# Opening JSON file
	f = open(file_name, )
	
	# returns JSON object as a dictionary
	data = json.load(f)
	
	# creating spark dataframe through the dictionary
	input_data = data['records']
	input_df = sqlContext.read.json(sc.parallelize(input_data))
	
	# closing file
	f.close()
	
	# removing duplicates if any
	result_df = input_df.drop_duplicates(['id', 'ts'])
	return result_df
	

output_df = remove_duplicates('data.json')
#output_df.write.parquet('result.parquet')