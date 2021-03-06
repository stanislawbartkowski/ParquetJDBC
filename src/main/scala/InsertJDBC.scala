import Helper.{L, connect, tocols}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.TaskContext

import java.io.File
import java.sql.{Connection, Date, PreparedStatement, Types}

object InsertJDBC {

  def prepare(table: String, conn: Connection, fields: List[ReadFields.Field]): PreparedStatement = {

    var s: String = ""
    var col: String = ""
    for (a <- 1 to fields.length) {
      s = if (a == 1) "?" else s + ",?"
      val colsql: String = fields(a - 1).sqlcol
      col = if (a == 1) colsql else col + "," + colsql
    }
    val query = s"INSERT INTO  $table ($col) values ($s)"
    L(query)
    conn.setAutoCommit(false)
    conn.prepareStatement(query);
  }

  class insertDB(st: PreparedStatement) extends InsertData {

    override def insertInt(pos: Int, data: Int, nullv: Boolean): Unit = if (nullv) st.setNull(pos, Types.INTEGER) else st.setInt(pos, data)

    override def insertString(pos: Int, data: String, nullv: Boolean): Unit = st.setString(pos, if (nullv) null else data)

    override def insertDouble(pos: Int, data: Double, nullv: Boolean): Unit = if (nullv) st.setNull(pos,Types.DOUBLE) else st.setDouble(pos, data)

    override def insertBigDecimal(pos: Int, data: java.math.BigDecimal, nullv: Boolean): Unit = if (nullv) st.setNull(pos, Types.BIGINT) else st.setBigDecimal(pos, data)

    override def insertDate(pos: Int, data: Date, nullv: Boolean): Unit = st.setDate(pos, if (nullv) null else data)
  }

  def runJob(par: Params, rdd: RDD[Row], fields: List[ReadFields.Field]) = {
    // Params not serializable
    // Break parameters to pieces
    val url = par.url
    val user = par.user
    val password = par.password
    val batchSize = par.batchSize
    val table = par.table

    rdd.foreachPartition(f => {
      val con: Connection = connect(url, user, password)
      val st: PreparedStatement = prepare(table, con, fields)
      val insertC = new insertDB(st)
      var i = 0
      L("START: ==============");
      st.clearBatch();
      for (r <- f) {
        //        insert(st, r, fields)
        val partid = TaskContext.getPartitionId()
        insertC.insert(r, fields)
        st.addBatch();
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
