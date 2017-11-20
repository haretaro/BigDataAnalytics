#coding: utf-8
#最もコメント数の多い動画を表示
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, UserDefinedFunction
from pyspark.sql.types import ArrayType, StringType, LongType, StructType, StructField
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

spark = SparkSession\
        .builder\
        .appName('こんにちは')\
        .config('master', 'yarn')\
        .getOrCreate()

comment_schema = StructType([
    StructField('command', StringType()),
    StructField('content', StringType()),
    StructField('date', LongType()),
    StructField('vpos', LongType()),
    StructField('video_id', StringType())
    ])

meta_schema = StructType([
    StructField('category', StringType()),
    StructField('comment_num', LongType()),
    StructField('description', StringType()),
    StructField('file_type', StringType()),
    StructField('length', LongType()),
    StructField('mylist_num', LongType()),
    StructField('size_high', LongType()),
    StructField('size_low', LongType()),
    StructField('tags', ArrayType(StringType())),
    StructField('title', StringType()),
    StructField('upload_time', LongType()),
    StructField('video_id', StringType()),
    StructField('watch_num', LongType()),
    ])

comment = spark.read.json('/user/seitaro-t/comment/*.json', schema=comment_schema)

meta = spark.read.json('/user/seitaro-t/meta/*.jsonl', schema=meta_schema)

a = comment.groupBy('video_id').count()
b = meta.select('video_id', 'title')
c = a.join(b, a.video_id == b.video_id, 'inner').sort('count', ascending=False)
c.show()
