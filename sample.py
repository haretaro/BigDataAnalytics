#coding: utf-8
from pyspark.sql import SparkSession
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession\
        .builder\
        .appName('niconico')\
        .config('master', 'yarn')\
        .getOrCreate()

#start から end までのjsonlファイルを読み込んでDataFrameを返す
def metadata(spark, start, end):
    df = reduce(lambda d, acc: d.union(acc), [spark.read.json('meta/{0:04d}.jsonl'.format(i)) for i in range(start, end)])
    return df

df = metadata(spark, 1, 100)

df.printSchema()
df.show(5)
print(df.count())
