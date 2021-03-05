source env.rc

P=param.properties
JAR=target/scala-2.12/ParquetJDBC.jar

test1() {
  TEST=test1
  PARQUET=test1/test.par
}

test2() {
  TEST=test2
  PARQUET=test2/test1.parquet
}

test3() {
  TEST=test3
  PARQUET=test3/test2.parquet
}


#test1
#test2
test3

case $1 in

  1) echo "Test 1"; test1;;
  2) echo "Test 2"; test2;;
  3) echo "Test 3"; test3;;
  *) echo "Enter test number 1,2, or 3 as a paramter"; exit;;
  
esac

echo "url=$DBURL" >$P
echo "user=$DBUSER" >>$P
echo "password=$DBPASSWORD" >>$P
cat src/test/resource/$TEST/param.properties >>$P

java -cp $JAR:$JDBCJAR ParquetJDBC -i src/test/resource/$PARQUET -p $P -r $PARTITIONNUM -s $BATCHSIZE

