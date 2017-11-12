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

#メタデータを読む
df = spark.read.json('/user/seitaro-t/meta/*.jsonl')

#スキーマの表示
df.printSchema()

#先頭の5件を表示
df.show(5)

#件数を表示
print(df.count())
