from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SparkJob:
    def __init__(self, spark: SparkSession, args: dict):
        self.spark = spark
        self.data = args['data']

    def run(self):
        data = [(0, 'a'), (1, 'b'), (2, 'c')]
        df = self.spark.createDataFrame(data, ['id', 'name'])
        df = df.withColumn("data", F.lit(self.data))
        df.show()

        return df
