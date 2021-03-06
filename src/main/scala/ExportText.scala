import Helper.{L, connect}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Path, Paths}
import java.sql.Date

object ExportText {

  def testDirectory(outdir: String) = {
    var f: File = new File(outdir)
    Helper.L("Touching " + outdir + " directory")

    if (f.exists() && f.isFile) {
      Helper.Severe("Directory expected but regular file found")
      System.exit(4)
    }
    if (!f.exists()) {
      Helper.L("Directory does not exist, will create")
      Files.createDirectories(Paths.get(f.getAbsolutePath))
    }
  }

  class CreateLine(st: StringBuilder, delim: String) extends InsertData {

    override def insertInt(pos: Int, data: Int, nullv: Boolean): Unit = {
      if (pos > 1) st.append(delim); if (!nullv) st.append(data)
    }

    override def insertString(pos: Int, data: String, nullv: Boolean): Unit = {
      if (pos > 1) st.append(delim); if (!nullv) st.append(data)
    }

    override def insertDouble(pos: Int, data: Double, nullv: Boolean): Unit = {
      if (pos > 1) st.append(delim); if (!nullv) st.append(data)
    }

    override def insertBigDecimal(pos: Int, data: java.math.BigDecimal, nullv: Boolean): Unit = {
      if (pos > 1) st.append(delim); if (!nullv) st.append(data)
    }

    override def insertDate(pos: Int, data: Date, nullv: Boolean): Unit = {
      if (pos > 1) st.append(delim); if (!nullv)  st.append(data)
    }
  }


  def runJob(par: Params, rdd: RDD[Row], fields: List[ReadFields.Field]) = {

    val outdir: String = par.outdir
    val delim: String = par.delim
    val outfile : String = par.fileout

    var no: Int = 0

    rdd.foreachPartition(f => {
      testDirectory(outdir)
      val partid = TaskContext.getPartitionId()
      val ff : File = new File(outdir,s"$outfile-$partid")
      val  bw = new BufferedWriter(new FileWriter(ff))

      L("START: ==============");
      val st: StringBuilder = new StringBuilder
      val insertC = new CreateLine(st, delim)
      var i = 0
      for (r <- f) {
        st.clear()
        insertC.insert(r, fields)
//        Helper.P(st.toString())
        bw.write(st.toString())
        bw.newLine()
        i = i + 1
        if (i % 100 == 0) Helper.L(s"$partid $i")
      }
      bw.close()
    })
  }

}