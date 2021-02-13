import org.apache.spark.sql.{Row, SparkSession}

import java.io.{File, FileInputStream, PrintWriter}


import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.util.logging.Logger


object ParquetJDBC {

  // -i /home/sbartkowski/work/ParquetJDBC/src/test/resource/test1/test.par -p /home/sbartkowski/work/ParquetJDBC/src/test/resource/test1/param.properties -r 8 -s 100

  private val log: Logger = Logger.getLogger("ParquetJDBC")

  def L(s: String): Unit = log.info(s);

  def connect(url: String, user: String, password: String): Connection = {
    DriverManager.getConnection(url, user, password)
  }

  def prepare(table: String, conn: Connection, fields: List[ReadFields.Field]): PreparedStatement = {
    var s: String = "";
    for (a <- 1 to fields.length) {
      s = if (a == 1) "?" else s + ",?"
    }
    val query = s"INSERT INTO $table values ($s)"
    conn.setAutoCommit(false)
    conn.prepareStatement(query);
  }

  def insert(st: PreparedStatement, r: Row, fields: List[ReadFields.Field]): Unit = {

    for (elem <- fields) {
      elem.ftype match {
        case ReadFields.FieldType.INT => {
          val intVal = r.getAs[Int](elem.name)
          st.setInt(elem.pos, intVal)
        }
        case ReadFields.FieldType.STRING => {
          val stringVal = r.getAs[String](elem.name)
          st.setString(elem.pos, stringVal)
        }
        case ReadFields.FieldType.DOUBLE => {
          val doubleVal = r.getAs[Double](elem.name)
          st.setDouble(elem.pos, doubleVal)
        }
        case ReadFields.FieldType.DECIMAL => {
          val deciVal = r.getAs[java.math.BigDecimal](elem.name)
          st.setBigDecimal(elem.pos, deciVal)
        }
        case ReadFields.FieldType.DATE => {
          val dateVal = r.getAs[java.sql.Date](elem.name)
          st.setDate(elem.pos, dateVal)
        }
      } // match
    }

    //    st.executeUpdate();
    st.addBatch();
  }

  def main(args: Array[String]): Unit = {
    val par = new Params(args)
    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark Parquet")
      .getOrCreate()
    val parquetDF = spark.read.parquet(par.inputFile)
    val rdd = parquetDF.repartition(par.numofParts).rdd
    parquetDF.printSchema()

    // Params not serializable
    // Break parameters to pieces
    val url = par.url
    val user = par.user
    val password = par.password
    val batchSize = par.batchSize
    val table = par.table

    val fields: List[ReadFields.Field] = ReadFields.readList(par.propPath, par.IsPropert)

    val f: File = new File(par.inputFile)
    if (!f.exists()) {
      println(par.inputFile + " should point to existing file or directory")
      System.exit(4)
    }

    rdd.foreachPartition(f => {
      val con: Connection = connect(url, user, password)
      val st: PreparedStatement = prepare(table, con, fields)
      var i = 0
      L("START: ==============");
      st.clearBatch();
      for (r <- f) {
        insert(st, r, fields)
        // inserttest(st,r, assetCol, dateCol)
        i = i + 1
        if (i % batchSize == 0) {
          st.executeBatch();
          con.commit()
          st.clearBatch();
          L(i + ": commit")
        }
      }
      // last batch if not committed already
      if (i % batchSize != 0) {
        st.executeBatch();
        con.commit()
        L(i + ": last Batch")
      }
      L("END: ===================")
      st.close()
      con.close()
    })

  }

}
