import org.rogach.scallop.ScallopConf

import java.io.FileInputStream
import java.util.Properties

class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

  def IsPropert(f : String) : Boolean = params contains f

  private val urlpar = "url"
  private val urluser = "user"
  private val urlpassword = "password"
  private val tablenamepar = "table"

  private val params : Set[String] = Set(urlpar,urluser,urlpassword,tablenamepar)

  banner(
    """
      ParquetJDBC : A Spark/Scala program to load Parquet file or directory to a single JDBC table

      For usage see below:
    """)
  private val opropPath = opt[String]("paramPath", descr = "Property file", required = true, short = 'p')
  private val opart = opt[Int]("numPartitions", descr = "Number of partitions", required = true, short = 'r')
  private val obatch = opt[Int]("batchSize", descr = "Size of batch buffer", required = true, short = 's')
  private val oinput = opt[String]("inputParquet", descr = "Input Parquet file or directory", required = true, short = 'i')
  verify()

  val propPath: String = opropPath.getOrElse("")
  val inputFile: String = oinput.getOrElse("")
  val numofParts : Int = opart.getOrElse(-1)
  val batchSize : Int = obatch.getOrElse(-1)

  private val prop = new Properties()

  println(System.getProperty("user.dir"))
  prop.load(new FileInputStream(propPath))

  for (n <- params)
    if (prop.getProperty(n) == null) {
      println(s"$propPath - parameter $n expected")
      System.exit(4)
    }

  val url = prop.getProperty(urlpar)
  val user = prop.getProperty(urluser)
  val password = prop.getProperty(urlpassword)
  val table = prop.getProperty(tablenamepar)
}
