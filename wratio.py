#coding: utf-8
#Wだけのコメントが占める割合を日毎に計算する
import datetime
import locale
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, UserDefinedFunction, regexp_extract, col
from pyspark.sql.types import ArrayType, StringType, LongType, StructType, StructField, TimestampType, FloatType
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
locale.setlocale(locale.LC_ALL, 'ja_JP.UTF-8')

spark = SparkSession\
        .builder\
        .appName('草')\
        .config('master', 'yarn')\
        .getOrCreate()

comment_schema = StructType([
    StructField('command', StringType()),
    StructField('content', StringType()),
    StructField('date', TimestampType()),
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
    StructField('upload_time', TimestampType()),
    StructField('video_id', StringType()),
    StructField('watch_num', LongType()),
    ])

comment = spark.read.json('/user/seitaro-t/comment/*.json', schema=comment_schema)

add_fdate = UserDefinedFunction(
        lambda d: d.strftime('%Y%m%d') if d is not None else None,
        StringType())
comment = comment.withColumn('fdate', add_fdate('date'))
ws = comment.where(col('content').rlike('^(w|W|ｗ|W){2,}|^(w|W|ｗ|W)$')).select('fdate', 'content')
ws.show()

n_ws = ws.count()
n_comment = comment.count()
print(n_ws)
print(n_comment)
print(float(n_ws) / n_comment)

a = comment.groupBy('fdate').count().select('fdate', 'count')
b = ws.groupBy('fdate').count()\
        .select('fdate', 'count')\
        .withColumnRenamed('count', 'wcount')

wratio = UserDefinedFunction(
        lambda w, c: float(w)/c,
        FloatType())

c = b.join(a, 'fdate', 'inner').withColumn('wratio', wratio('wcount', 'count'))
c.show()
c.write.csv('output.csv')

