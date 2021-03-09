source env.rc

P=param.properties
JAR=target/scala-2.12/ParquetJDBC.jar

echo "url=$DBURL" >$P
echo "user=$DBUSER" >>$P
echo "password=$DBPASSWORD" >>$P
cat param.source >>$P

I="/home/sbartkowski/work/parquet/asset=mach5ParquetTest/date=2021-01-10/part-r-00001-01915dae-c765-42fc-9d6c-efef81966e84-LMPI.snappy.parquet"

java -cp $JAR:$JDBCJAR ParquetJDBC -i $I -p $P -r $PARTITIONNUM -s $BATCHSIZE
