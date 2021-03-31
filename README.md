# ParquetJDBC

A simple Spark/Scala tool which reads data from Parquet files or directory and inserts to JDBC database.

# Prerequisties

* Java 1.8 or later
* JDBC driver, tested with DB2 and PostgreSQL
* sbt

https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html


# Installation

> git clone https://github.com/stanislawbartkowski/ParquetJDBC.git<br>
> cd ParquetJDBC<br>
> sbt assembly

Make sure that jar is created.<br>
> ls target/scala-2.12/<br>
```
 classes
 ParquetJDBC.jar
 update
 zinc
```

# Launching the application

Before launching the application, the corresponding table in JDBC database should be created manually and match Parquet file schema. <br>

> JAR=target/scala-2.12/ParquetJDBC.jar
> java -cp $JAR:$JDBCJAR ParquetJDBC -i /Parquet file or directory/ -p /parameter file/ -r /number of partitions/ -s /JDBC batch size/

| Parameter | Description | Sample value |
| --------- | ----------- | ------------- |
| -i /path/ | Path to Parquet file or directory | test2/test1.parquet
| -p /path/ | Path to property file (look below) | param.properties
| -r /number/ | Number of Parquet Spark RDD partitions, parallelism level | -r 8
| -s /number/ | Size of JDBC batch size before commiting | -s 500
| -t | Test existence of input directory and JDBC connecivity and exits | 

<br>
Property file.<br>

JDBC insert loading.<br>

| Property | Description | Sample value |
| ----- | ------ | --------- |
| url | URL | jdbc:db2://thinkde:50000/parqdb
| url (if SSL) | JDBC URL |  jdbc:db2://thinkde:50010/parqdb:sslConnection=true;
| user | JDBC connection user | db2inst1
| password | JDBC connection password | secret
| table  | Database table where data is inserted | testpar
| AWSKEY | | empty, do not set
| AWSSECRETKEY | | empty, do not set
| ENDPOINT |  | empty, do not set
| BUCKET| | empty, do not set


DB2 Warehouse Rest/API loading from S3 AWS

| Property | Description | Sample value |
| ----- | ------ | --------- |
| url | | empty, do not set
| user |  | empty, do not set
| password | | empty, do not set
| table  | Database table where data is inserted | testpar
| AWSKEY | AWS access credentials, AWS_ACCESS_KEY_ID | xxxxx
| AWSSECRETKEY | AWS access credentials,  AWS_SECRET_ACCESS_KEY |  xxxxx
| ENDPOINT |  | s3-us-west-2.amazonaws.com
| BUCKET| | wdftya-kops-state-store
| dirout| Mount S3 point on local file system | /mnt/s3/
| fileout | Subdirectory and file prefix to create delimited filr | sbtest/out1/export

More on *dirout* and *fileout*. <br>

The application is firstly generating delimited text in *dirout*/*fileout*-/<number/>  file. When file is ready, then DB2 Warehouse REST/API is launched to load data from S3 bucket and as an input text file is passed *fileout*-/<number/> in appropriate S3 bucker.<br>

Example:
* Local mount point for S3 bucket: /mnt/s3<br>
* File name in S3 mount: /sbtest/out1/export<br>

Assuming *-r* parameter as 4. The application creates in local file system four files.<br.
* /mnt/s3/sbtest/out1/export-0
* /mnt/s3/sbtest/out1/export-1
* /mnt/s3/sbtest/out1/export-2
* /mnt/s3/sbtest/out1/export-3

The DB2 Warehouse load REST/API is called and AWS Object Store parameters are passed: AWSKEY,AWSSECRETKEY, ENDPOINT and BUCKET. As a input text file, /sbtest/out1/export-0, /sbtest/out1/export-1, /sbtest/out1/export-2, /sbtest/out1/export-3 are used. Directory mount point */mnt/s3/* is valid only on local file system, in AWS bucket the correct file name is */mnt/s3/sbtest/out1/export-n*.

<br>


All other properties are Parquet schema to JDBC mapping.
<br>
Format<br>
* /Parquet column, SQL column/ = /number:type/

* SQL column is optional, if not provided, the same as Parquet column
* number: JDBC *set* column number, starts from 1 (not 0).
* type: INT,DOUBLE,STRING,DATE,DECIMAL

Example<br>

Parquet schema.
```
{
  "type" : "record",
  "name" : "hive_schema",
  "fields" : [ {
    "name" : "c1",
    "type" : [ "null", "int" ],
    "default" : null
  }, {
    "name" : "c2",
    "type" : [ "null", "string" ],
    "default" : null
  } ]
}
```
Table schema (should be created manually)<br>
```
CREATE TABLE TESTPAR (C1 INT, C2 VARCHAR(100));
```
Parquet file / table mapping<br>
```
c1=1,INT
c2=2,STRING
```

Assuming table schema using different column names:<br>
```
CREATE TABLE TESTPAR (ID INT, C2 NAME(100));
```
```
c1,ID=1,INT
c2,NAME=2,STRING
```

The application prepares corresponding *insert* SQL, the number of *?* is equal to the number of map properties: *INSERT INTO TESTPAR VALUES (col names) (?,?)*<br>
<br>
The data is inserted into the table using the following method<br>
```
val intVal = r.getAs[Int]('c1')
st.setInt(1, intVal)

val stringVal = r.getAs[String]('c2')
st.setInt(2, stringVal)

```

Important: the application does not validate the correctness of JDBC table definition or Parquet to table mapping. In case of any error or discrepancy, the exception will be thrown and the application will fail.<br>


# Run the tests provided

Create tables using provided schema.<br>
> db2 -tvf  src/test/resource/test1/testschema.sql<br>
> db2 -tvf  src/test/resource/test2/testschema.sql<br>
> db2 -tvf  src/test/resource/test3/testschema.sql<br>

> psql -h brunette-inf -U redhat persistentdb <  src/test/resource/test1/testschema.sql 
> psql -h brunette-inf -U redhat persistentdb <  src/test/resource/test2/testschema.sql 
> sed 's/DOUBLE/DOUBLE PRECISION/' src/test/resource/test3/testschema.sql | psql -h brunette-inf -U redhat persistentdb

<br>

> cp template/env.rc .<br>

Update *env.rc* file.<br>
> vi env.rc<br>
```
#DBUSER=db2inst1
#DBPASSWORD=db2inst1
#DBURL=jdbc:db2://thinkde:50000/parqdb
#JDBCJAR=/opt/ibm/db2/V11.5/java/db2jcc4.jar

PARTITIONNUM=8
BATCHSIZE=500

JDBCJAR=/usr/share/java/postgresql-jdbc.jar
DBUSER=redhat
DBPASSWORD=redhat123
DBURL=jdbc:postgresql://brunette-inf/persistentdb
```

Run tests:<br>
> ./run.sh 1<br>
> ./run.sh 2<br>
> ./run.sh 3<br>
