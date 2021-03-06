import Helper.{P, connect, tocols}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, Date, DriverManager, PreparedStatement}


object ParquetJDBC {

  // -i /home/sbartkowski/work/ParquetJDBC/src/test/resource/test1/test.par -p /home/sbartkowski/work/ParquetJDBC/src/test/resource/test1/param.properties -r 8 -s 100

  def main(args: Array[String]): Unit = {
    val par = new Params(args)
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark Parquet")
      .getOrCreate()

    if (par.test) {
      TestConnection.testconnection(spark, par);
      return;
    }


    val parquetDF = spark.read.parquet(par.inputFile)
    val rdd = parquetDF.repartition(par.numofParts).rdd
    parquetDF.printSchema()

    val fields: List[ReadFields.Field] = ReadFields.readList(par.propPath, par.IsProperty, parquetDF).sortBy(_.pos)

    if (par.connect) InsertJDBC.runJob(par, rdd, fields) else ExportText.runJob(par, rdd, fields)

  }

}
