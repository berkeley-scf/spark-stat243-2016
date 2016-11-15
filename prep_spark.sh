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
