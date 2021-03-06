import org.rogach.scallop.ScallopConf

import java.io.FileInputStream
import java.util.Properties

class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

  def IsProperty(f: String): Boolean = (params contains f) || (paramsout contains f)

  private val urlpar = "url"
  private val urluser = "user"
  private val urlpassword = "password"
  private val tablenamepar = "table"

  private val diroutpar = "dirout"
  private val delimpar = "delim"
  private val fileoutpar = "fileout"

  private val params: Set[String] = Set(urlpar, urluser, urlpassword, tablenamepar)
  private val paramsout: Set[String] = Set(diroutpar, delimpar, fileoutpar)

  banner(
    """
      ParquetJDBC : A Spark/Scala program to load Parquet file or directory to a single JDBC table

      For usage see below:
    """)
  private val opropPath = opt[String]("paramPath", descr = "Property file", required = true, short = 'p')
  private val opart = opt[Int]("numPartitions", descr = "Number of partitions", required = true, short = 'r')
  private val obatch = opt[Int]("batchSize", descr = "Size of batch buffer", required = true, short = 's')
  private val oinput = opt[String]("inputParquet", descr = "Input Parquet file or directory", required = true, short = 'i')
  private val otest = opt[Boolean]("testConn", descr = "Test connection only and exit", short = 't')
  verify()

  val propPath: String = opropPath.getOrElse("")
  val inputFile: String = oinput.getOrElse("")
  val numofParts: Int = opart.getOrElse(-1)
  val batchSize: Int = obatch.getOrElse(-1)
  val test: Boolean = otest.getOrElse(false);

  private val prop = new Properties()

  println(System.getProperty("user.dir"))
  prop.load(new FileInputStream(propPath))

  val connect = prop.getProperty(urlpar) != null

  for (n <- if (connect) params else paramsout)
    if (prop.getProperty(n) == null) {
      println(s"$propPath - parameter $n expected")
      System.exit(4)
    }

  val url = prop.getProperty(urlpar)
  val user = prop.getProperty(urluser)
  val password = prop.getProperty(urlpassword)
  val table = prop.getProperty(tablenamepar)
  val outdir: String = prop.getProperty(diroutpar)
  val delim: String = prop.getProperty(delimpar)
  val fileout: String = prop.getProperty(fileoutpar)
}
