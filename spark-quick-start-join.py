from pyspark import SparkContext
from pyspark import SparkConf

def extract_user_ip(line):
    tokens = line.split()
    return [tokens[8], tokens[10]]

conf = SparkConf()
conf.setAppName('Spark Quick Start Sample')

sc = SparkContext(conf=conf)

# (uid, ip)
f = sc.textFile('/project/public/pegasus_log_sample/*')\
        .filter(lambda x: x.find('Accept') > 0 and x.find('root') < 0)\
        .map(lambda x: extract_user_ip(x)).cache() 

# print user access source ip
# access = f.collect()
# for a in access:
#  print a[0], a[1]

# (uid, group)
user_map = sc.textFile('/project/public/pegasus_log_sample_user_group_map.txt').map(lambda x: x.split())

# (uid, count)
user_visits = f.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a+b)

# (group, count)
# after join (uid, (count, group))
results = user_visits.join(user_map).map(lambda (x, a): (a[1], a[0])) \
        .reduceByKey(lambda a, b: a+b).collect()

for u in results:
    print u[0], u[1]

