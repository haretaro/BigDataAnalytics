#coding: utf-8
#スキーマ表示するだけ
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName('niconico')\
        .config('master', 'yarn')\
        .getOrCreate()

meta = spark.read.json('/user/seitaro-t/meta/0001.jsonl')
meta.printSchema()

comment = spark.read.json('/user/seitaro-t/comment/0001/sm19961.jsonl')
comment.printSchema()
