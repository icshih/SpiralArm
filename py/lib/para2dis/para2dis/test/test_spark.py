from para2dis.Prior import Prior
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.Builder().appName('test').getOrCreate()

    pri = Prior()

    spark.stop()