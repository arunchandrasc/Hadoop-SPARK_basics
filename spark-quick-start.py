from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
conf.setAppName('Spark Quick Start Sample')

#sc = SparkContext(conf=conf)


f = sc.textFile('/Volumes/MAC_PART/OP_DTL_OWNRSHP_PGYR2015_P06302017.csv')

#f = sc.textFile('/Volumes/MAC_PART/OP_DTL_RSRCH_PGYR2015_P01172017.csv')
count = f.count()

print "total number of lines counted:", count

