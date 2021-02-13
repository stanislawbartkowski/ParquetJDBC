# ParquetJDBC

A simple SPARK/SCALA tool which reads data from Parquet files or directory and insert to JDBC database.

# Prerequisties

* Java 1.8 or later
* JDBC driver, tested with DB2 and PostreSQL
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
