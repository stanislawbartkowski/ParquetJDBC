import org.rogach.scallop.ScallopConf
import rest.WarehouseCred

import java.io.FileInputStream
import java.util.Properties

class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

  def IsProperty(f: String): Boolean = (params contains f) || (paramsout contains f) || (paramsrest contains f)

  private val urlpar = "url"
  private val urluser = "user"
  private val urlpassword = "password"
  private val tablenamepar = "table"

  private val diroutpar = "dirout"
  private val delimpar = "delim"
  private val fileoutpar = "fileout"

  private val resturlpar = "resturl"
  private val restuserpar = "restuser"
  private val restpasswordpar = "restpassword"
  private val restschemapar = "restschema"
  private val resttablepar = "resttable"
  private val AWSKEYpar = "AWSKEY"
  private val AWSSECRETKEYpar = "AWSSECRETKEY"
  private val ENDPOINTpar = "ENDPOINT"
  private val BUCKETpar = "BUCKET"

  private val ALLREST=0
  private val TEXTONLY=1
  private val RESTONLY=2

  def isText = jobtype == TEXTONLY || jobtype == ALLREST
  def isRest = jobtype == RESTONLY || jobtype == ALLREST

  private val params: Set[String] = Set(urlpar, urluser, urlpassword, tablenamepar)
  private val paramsout: Set[String] = Set(diroutpar, delimpar, fileoutpar)
  private val paramsrest : Set[String] = Set(resturlpar,restuserpar,restpasswordpar,restschemapar,resttablepar,AWSKEYpar,AWSSECRETKEYpar,ENDPOINTpar,BUCKETpar)

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
  private val ojobtype = opt[Int]("jobtype in case of rest", descr = "0 - text and rest (default), 1 - text only, 2 - rest only", short = 'j')
  verify()

  val propPath: String = opropPath.getOrElse("")
  val inputFile: String = oinput.getOrElse("")
  val numofParts: Int = opart.getOrElse(-1)
  val batchSize: Int = obatch.getOrElse(-1)
  val test: Boolean = otest.getOrElse(false);
  private val jobtype : Int = ojobtype.getOrElse(0)

  private val prop = new Properties()

  println(System.getProperty("user.dir"))
  prop.load(new FileInputStream(propPath))

  val connect = prop.getProperty(urlpar) != null
  val rest = prop.getProperty(resturlpar) != null

  val keys: Set[String] = if (connect) params else if (rest)  paramsrest else paramsout
  for (n <- keys)
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

  private val resturl = prop.getProperty(resturlpar)
  private val restuser = prop.getProperty(restuserpar)
  private val restpassword = prop.getProperty(restpasswordpar)
  val restschema = prop.getProperty(restschemapar)
  val resttable = prop.getProperty(resttablepar)

  private val ENDPOINT = prop.getProperty(ENDPOINTpar)
  private val BUCKET = prop.getProperty(BUCKETpar)
  private val AWSKEY = prop.getProperty(AWSKEYpar)
  private val AWSSECRETKEY = prop.getProperty(AWSSECRETKEYpar)

  val cred = WarehouseCred(restuser,restpassword,resturl,AWSKEY,AWSSECRETKEY,ENDPOINT,BUCKET)

}
