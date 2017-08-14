import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName('Spark Quick Start Sample')

sc = SparkContext(conf=conf)

f = sc.textFile('/project/public/PGYR15/OP_PGYR2015_README_P01172017.txt').cache()

max_length = f.map(lambda x: len(x.split())).reduce(lambda a, b: a if(a>b) else b)

word_counts = f.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b).sortBy(lambda x: x[1], ascending=False).take(5)

print "max line length: ", max_length

print "top 5 words: "

for w in word_counts:
    print w[0], w[1]


