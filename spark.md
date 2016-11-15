% Spark demonstration
% November 16, 2016
% Chris Paciorek, Department of Statistics and Berkeley Research Computing, UC Berkeley

# 'Big Data'

Big data is [trendy these days](http://i2.wp.com/blog.datacamp.com/wp-content/uploads/2014/03/big-data.jpg). 


Personally, I think some of the hype is justified and some is hype. Large datasets allow us to address questions that we can't with smaller datasets, and they allow us to consider more sophisticated (e.g., nonlinear) relationships than we might with a small dataset. But they do not directly help with the problem of correlation not being causation. 

 - Having medical data on every American still doesn't tell me if higher salt intake causes high blood pressure.
 - Internet transaction data does not tell me if one website feature causes increased viewership or sales. 

One either needs to carry out a designed experiment or think carefully about how to infer causation from observational data. 

Nor does big data help with the problem that an ad hoc 'sample' is not a statistical sample and does not provide the ability to directly infer properties of a population. 
 - A well-chosen smaller dataset may be much more informative than a much larger, more ad hoc dataset. 

However, having big datasets might allow you to select from the dataset in a way that helps get at causation or in a way that allows you to construct a population-representative sample. Finally, having a big dataset also allows you to do a large number of statistical analyses and tests, so multiple testing is a big issue. With enough analyses, something will look interesting just by chance in the noise of the data, even if there is no underlying reality to it. 

Different people define the 'big' in big data differently. One definition involves the actual size of the data. Our efforts here will focus on dataset sizes that are large for traditional statistical work but would probably not be thought of as large in some contexts such as Google or the NSA. Another definition of 'big data' has more to do with how pervasive data and empirical analyses backed by data are in society and not necessarily how large the actual dataset size is.

# Overview of Hadoop, MapReduce, and Spark

Here we'll talk about a fairly recent development in parallel computing. Traditionally, high-performance computing (HPC) has concentrated on techniques and tools for message passing such as MPI and on developing efficient algorithms to use these techniques.

# MapReduce

A basic paradigm for working with big datasets is the MapReduce paradigm. The basic idea is to store the data in a distributed fashion across multiple nodes and try to do the computation in pieces on the data on each node. Results can also be stored in a distributed fashion.

A key benefit of this is that if you can't fit your dataset on disk on one machine you can on a cluster of machines. And your processing of the dataset can happen in parallel. This is the basic idea of MapReduce.

The basic steps of MapReduce are as follows:

 - read individual data objects (e.g., records/lines from CSVs or individual data files)
 - map: create key-value pairs using the inputs (more formally, the map step takes a key-value pair and returns a new key-value pair)
 - reduce - for each key, do an operation on the associated values and create a result - i.e., aggregate within the values assigned to each key
 - write out the {key,result} pair

An example of key-value pairs is as follows. Suppose you have a dataset of individuals with information on their income and the state in which they live and you want to calculate the average and income within each state. In this case, one starts with a dataset of individual-level rows and uses a map step to set the key to be the state and the value to be income for an individual. Then the reduce step finds the mean and standard deviation of all the values with the same key (i.e., in the same state).

More explicitly the reduce step involves summing income and summing squared income and summing the number of individuals in each state and using those summary statistics to compute average and standard deviation. 

A similar paradigm that is being implemented in some R packages by Hadley Wickham is the [split-apply-combine strategy](http://www.jstatsoft.org/v40/i01/paper).

# What does can we do with MapReduce?

 - basic database-like operations on datasets: transformation of records, filtering
 - aggregation/summarization by groups
 - run algorithms (e.g., statistical fitting) that can be written as a series of map and reduce steps (e.g., gradient-based optimization, certain linear algebra operations)

# Hadoop and Spark

Hadoop is an infrastructure for enabling MapReduce across a network of machines. The basic idea is to hide the complexity of distributing the calculations and collecting results. Hadoop includes a file system for distributed storage (HDFS), where each piece of information is stored redundantly (on multiple machines). Calculations can then be done in a parallel fashion, often on data in place on each machine thereby limiting the amount of communication that has to be done over the network. Hadoop also monitors completion of tasks and if a node fails, it will redo the relevant tasks on another node. Hadoop is based on Java but there are projects that allow R to interact with Hadoop, in particular RHadoop and RHipe. Rhadoop provides the rmr, rhdfs, and rhbase packages. For more details on RHadoop see [here](http://blog.revolutionanalytics.com/2011/09/mapreduce-hadoop-r.html).

Setting up a Hadoop cluster can be tricky. Hopefully if you're in a position to need to use Hadoop, it will be set up for you and you will be interacting with it as a user/data analyst.

Ok, so what is Spark? You can think of Spark as in-memory Hadoop. Spark allows one to treat the memory across multiple nodes as a big pool of memory. Spark should be faster than Hadoop when the data will fit in the collective memory of multiple nodes. In cases where it does not, Spark will sequentially process through the data, reading and writing to the HDFS.

# Spark: Overview

We'll focus on Spark rather than Hadoop for the speed reasons described above and because I think Spark provides a very nice environment/interface in which to work. Plus it comes out of the AmpLab here at Berkeley. We'll use the Python interface to Spark.

More details on Spark are in the [Spark programming guide](http://spark.apache.org/docs/latest/programming-guide.html).

Some key aspects of Spark:

  - Spark can read/write from various locations, but a standard location is the **HDFS**, with read/write done in parallel across the cores of the Spark cluster.
  - A common data structure in Spark is a **Resilient Distributed Dataset (RDD)**, which acts like a sort of distributed data frame. 
  - RDDs are stored in chunks called **partitions**, stored on the different nodes of the cluster (either in memory or if necessary on disk).
  - Spark has a core set of methods that can be applied to RDDs to do operations such as **filtering/subsetting, transformation/mapping, reduction, and others**.
  - The operations are done in **parallel** on the different partitions of the data
  - Some operations such as reduction generally involve a "shuffle", moving data between nodes of the cluster. This is costly.

# Getting set up on Spark and the HDFS

We'll use Spark on an US National Science Foundation (NSF) supercomputer called Bridges, hosted at the Pittsburgh Supercomputing Center. We'll work with a dataset of Wikipedia traffic.

First we need to get the data from the standard filesystem to the HDFS. Note that the file system commands are like standard UNIX commands, but you need to do hadoop fs - in front of the command. At the end of this chunk we'll start the Python interface for Spark. 

```
export DATADIR=/pylon1/ca4s8fp/paciorek/

ls ${DATADIR}
ls ${DATADIR}/wikistats-dated  # 396 gzipped files

du -h ${DATADIR}/wikistats-dated 

# what do data files look like?
gzip -cd $DATADIR/wikistats-dated/part-00381.gz | head

ssh r403  # master node of Spark cluster

hadoop fs -ls /
hadoop fs -ls /user
hadoop fs -mkdir /user/paciorek/data
hadoop fs -mkdir /user/paciorek/data/wikistats-dated

hadoop fs -copyFromLocal ${DATADIR}/wikistats-dated/* /user/paciorek/data/wikistats-dated/

# check files on the HDFS, e.g.:
hadoop fs -ls /user/paciorek/data/wikistats-dated

# start Spark's Python interface as interactive session
# specifying the URL of the master node, with a particular port

pyspark --master spark://r403.pvt.bridges.psc.edu:7077
# to use more memory on workers, one can do this:
# pyspark --master spark://r403.pvt.bridges.psc.edu:7077 --executor-memory 110G


# after processing retrieve data from HDFS
hadoop fs -copyToLocal /user/paciorek/data/obama-counts ${DATADIR}/
```

# Using Spark for processing large datasets for subsequent analysis

Now we'll do some basic manipulations with the Wikipedia dataset, with the goal of analyzing traffic to Barack Obama's sites during the days surrounding his election in 2008. 

  - We'll count the number of lines/observations in our dataset. 
  - then we'll do a map-reduce calculation that involves filtering to the Barack Obama sites, 
  - then do a map step that creates key-value pairs from each record/observation/row and 
  - then do a reduce that counts the number of views by hour, so hour-day will serve as the key. 

```
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
```

Note that all of the various operations are OOP methods applied to either the SparkContext management object or to a Spark dataset, called a Resilient Distributed Dataset (RDD). Here lines and output are both RDDs. However the result of collect() is just a standard Python object.

# Spark monitoring

There are various interfaces to monitor Spark and the HDFS.

  - `http://<master_url>:8080` -- general information about the Spark cluster
  - `http://<master_url>:4040` -- information about the Spark tasks being executed
  - `http://<master_url>:50070` -- information about the HDFS


# Spark operations

Let's consider some of the core methods we used. 

 - filter(): create a subset
 - map(): take an RDD and apply a function to each element, returning an RDD
 - reduce() and reduceByKey(): take an RDD and apply a reduction operation to the elements, doing the reduction stratified by the key values for reduceByKey(). Reduction functions need to be associative (order across records doesn't matter) and commutative (order of arguments doesn't matter) and take 2 arguments and return 1, all so that they can be done in parallel in a straightforward way.
 - collect(): collect results back to the master
 - cache(): tell Spark to keep the RDD in memory for later use
 - repartition(): rework the RDD so it is divided into the specified number of partitions

Question: how many chunks do you think we want the RDD split into? What might the tradeoffs be?

# Other comments

## Running a batch Spark job

We can run a Spark job using Python code as a batch script rather than interactively. Here's an example, which computes the value of Pi  by Monte Carlo simulation. Assuming the script is named *piCalc.py*, we would call the script like this: 

```
spark-submit piCalc.py 100000000 1000 
```

This code uses the idea that it's computationally more efficient to have each operation occur on a batch of data rather than an individual data point. So there are 1000 tasks and the total number of samples is broken up amongst those tasks. In fact, Spark has problems if the number of tasks gets too large.

## Python vs. Scala/Java

Spark is implemented natively in Java and Scala, so all calculations in Python involve taking Java data objects converting them to Python objects, doing the calculation, and then converting back to Java. This process is called serialization and takes time, so the speed when implementing your work in Scala (or Java) may be faster. Here's a [small bit of info](http://apache-spark-user-list.1001560.n3.nabble.com/Scala-vs-Python-performance-differences-td4247.html) on that.

## sparkR

Finally, there is an R interface for Spark, but I haven't had time to keep up to speed with it. It uses the idea of a distributed data frame, so is intended to fit well with your R code that works on plain old R data frames.
