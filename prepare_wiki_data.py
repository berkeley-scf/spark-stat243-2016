# code for preparing Wikistats data
# code below adds the filename (containing date-time information) to each row of the data files

# see http://stackoverflow.com/questions/29686573/spark-obtaining-file-name-in-rdds

# invocation of pySpark:
# need driver memory set or get Java heap out of memory
# pyspark  --executor-memory 60G --driver-memory 120G #  120G

# following seems to work 
# but may only use a couple cores... this seems to be because sc.wholeTextFiles only puts the files into two partitions - why?
files = sc.wholeTextFiles('user/paciorek/data/wikistats-original') # works but only 2 partitions so slow
# note that repartitioning seems to cause memory issues

# wikistats-original files have names like 'pagecounts-20081107-070000.gz'

def makeLines(file):
    name = file[0].split('/')
    name = name[-1].split('-')[1:3]
    name[1] = name[1][:-3]
    entries = file[1].split('\n')
    out = [name[0] + ' ' + name[1] + ' ' + tmp for tmp in entries]
    return(out)

# flatMap takes RDD where each record is a file and creates a record for each row in each file
lines = files.flatMap(makeLines)

# save output - only 2 plain text files because only 2 partitions per above comment
lines.saveAsTextFile('user/paciorek/data/wikistats-tmp')

# reading back in from 2 plain text files - by default 396 partitions created
lines = sc.textFile('user/paciorek/data/wikistats-tmp')

# save out as 396 gzipped files 
lines.saveAsTextFile('user/paciorek/data/wikistats-dated', "org.apache.hadoop.io.compress.GzipCodec")
