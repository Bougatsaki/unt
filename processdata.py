from pyspark import SparkConf, SparkContext
import sys,json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import *







def main(output):
    business_schema = types.StructType([
        StructField('business_id', StringType(), False),
        StructField('name', StringType(), False),
        StructField('address', StringType(), False),
        StructField('city', StringType(), False),
        StructField('state', StringType(), False),
        StructField('postal_code', StringType(), False),
        StructField('latitude', FloatType(), False),
        StructField('longitude', FloatType(), False),
        StructField('stars', FloatType(), False),
        StructField('review_count', IntegerType(), False),
        StructField('is_open', IntegerType(), False),
        StructField('attributes', StringType(), False),
        StructField('categories', StringType(), False),
        StructField('hours', StringType(), False)
])


    df = spark.read.format("json").option("mode", "DROPMALFORMED").schema(business_schema).load("yelp_business.json")
    df.write.save(output, format='json', mode='overwrite')
    


    

if __name__ == '__main__':
    spark = SparkSession.builder.appName('yelp').getOrCreate()
    output = sys.argv[1]
    main(output)

    



