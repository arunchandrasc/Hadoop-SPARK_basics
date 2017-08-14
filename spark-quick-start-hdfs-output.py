import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName('Spark Quick Start Sample')

sc = SparkContext(conf=conf)

f = sc.textFile('/project/public/PGYR15/OP_PGYR2015_README_P01172017.txt')

word_counts = f.flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a+b) \
        .sortBy(lambda x: x[1], ascending=False)
        
word_counts.saveAsTextFile("quick-start-word-count")


