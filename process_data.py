# pyspark --master spark://r403.pvt.bridges.psc.edu:7077

lines = sc.textFile('/user/paciorek/data/wikistats-dated')

lines.getNumPartitions() # 396 partitions, because with gzipped files, data in each file goes into a partition

# note delayed evaluation
lines.count()

testLines = lines.take(10)

import re
from operator import add

def find(line):
    regex = "[Oo]bama"
    category = "en"
    vals = line.split(' ')
    if len(vals) < 6:
        return(False)
    tmp = re.search(regex, vals[3])
    if tmp is None or vals[2] != category :
        return(False)
    else:
        return(True)

# filter to only the Obama sites and look at a few
result = lines.filter(find).take(100)
# fairly quick - 10 sec.

result[0]
result[99]

# create key-value pairs where:
#   key = date-time
#   value = number of website hits
# and then let's count the number of hits for each date-time
def stratify(line):
    vals = line.split(' ')
    if len(vals) < 6:
        return('none', 0)
    return(vals[0] + '-' + vals[1], int(vals[4]))

result = lines.map(stratify).reduceByKey(add)

result.collect()

# filter to Obama sites, then count by date-time
result = lines.filter(find).map(stratify).reduceByKey(add)
# fairly quick - about 45 seconds with 5 worker nodes

result.take(1)  # force evaluation


def transform(vals):
    datetime = vals[0].split('-')
    return(datetime[0] + "," + datetime[1] + "," + str(vals[1]))

# prepare in nice format for output
result.map(transform).repartition(1).saveAsTextFile('/user/paciorek/data/obama-counts')

