import Helper.{P, connect}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.sql.Connection

object TestConnection {

  def testconnection(spark: SparkSession, par: Params): Unit = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val p: Path = new Path(par.inputFile);
    P("Testing " + par.inputFile)
    if (!fs.exists(p)) {
      P(" -- does not exists")
    }
    else {
      P(" -- exists")
      P(if (fs.isDirectory(p)) " -- is directory" else " -- is file")
    }

    if (par.connect) {
      P("Testing JDBC connectivity : " + par.url);
      try {
        val con: Connection = connect(par);
        P("-- connected")
        con.close();
      } catch {
        case e: Exception => {
          P("-- failed");
          e.printStackTrace();
        }
      }
    }
    else {
      P(" Export to directory")
      P(" JDBC conenctivity is not tested")
    }
  }

}
