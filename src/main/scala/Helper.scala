import java.sql.{Connection, DriverManager}
import java.util.logging.Logger

object Helper {


  private val log: Logger = Logger.getLogger("ParquetJDBC")

  def L(s: String): Unit = log.info(s);

  def P(s: String): Unit = println(s);

  def Severe(s : String): Unit = log.severe(s)

  def connect(url: String, user: String, password: String): Connection = {
    DriverManager.getConnection(url, user, password)
  }

  def connect(par : Params) : Connection = connect(par.url,par.user,par.password)

  def tocols(e: String): (String, String) = {
    val cols = e.split(",")
    if (cols.length == 1) (e, e)
    else (cols(0), cols(1))
  }

}
