package rest

import java.util.logging.Logger
import javax.ws.rs.core.Response


case class WarehouseException(s: String) extends Exception(s)

case class WarehouseCred(val user: String, val password: String, val url: String, val AWSKEY: String, val AWSSECRETKEY: String, val ENDPOINT: String, val BUCKET: String)


class LoadWarehouse(val cred: WarehouseCred) {

  private val log: Logger = Logger.getLogger("LoadWarehouse")

  private def L(mess: String) = log.info(mess)


  private var token: Option[String] = Option.empty
  private var jobid: Option[String] = Option.empty
  private var sqlid: Option[String] = Option.empty
  private var lastsql: Option[String] = Option.empty

  val sleepSec = 5

  private def throwserror(mess: String) = {
    log.severe(mess)
    throw new WarehouseException(mess)
  }

  private def throwserror(code: Int, mess: String) = {
    val errmess = s"Response code $code $mess"
    log.severe(errmess)
    throw new WarehouseException(errmess)
  }

  def acquireToken(): Unit = {

    L("Requesting access token")
    val (code, j, tokens) = WarehouseRestApi.acquireTokenString(cred)
    if (code != Response.Status.OK.getStatusCode) throwserror(code, "Cannot obtain access token")
    L("Access token obtained")
    token = Option(tokens)
  }

  def LoadJob(path: String, schema: String, table: String, delim: String) = {
    L(s"Launch data loading from $path to $schema.$table")
    val (code, j, id) = WarehouseRestApi.startLoadJob(cred, token.get, path, schema, table, delim)
    if (code != Response.Status.CREATED.getStatusCode) throwserror(code, "Starting the load job failed.")
    jobid = Option(id)
  }

  def LoadWait(mess: String) = {
    L("Waiting for load to be completed")
    var waitforload = true
    while (waitforload) {
      Thread.sleep(sleepSec * 1000)
      L(s"Checking $mess")
      val (code, j, status, jsta) = WarehouseRestApi.getListOfJobs(cred, token.get, jobid.get)
      if (code != Response.Status.OK.getStatusCode) {
        WarehouseRestApi.logerrors(j)
        throwserror(code, "Cannot monitor the load job because of internal error")
      }
      L(s"Status reported $status")
      status match {
        case "Success" => {
          L("Completed with success")
          waitforload = false
        }
        case "Interrupted" | "Load in progress" => {
          L("Still running")
        }
        case "Failed" => {
          WarehouseRestApi.logerrors(jsta)
          throwserror(s"$mess load job reported as failed")
        }
        case _ => {
          WarehouseRestApi.logerrors(jsta)
          throwserror(s"$mess not expected status, cannot complete ($status)")
        }
      } // match
    } // while
  }


  def releaseToken = {
    L("Release the token")
    WarehouseRestApi.releaseToken(cred, token.get)
  }

  def runSQL(sql: String) = {
    L(sql)
    sqlid = Option.empty
    val (code, j, id) = WarehouseRestApi.runSQLCommand(cred, token.get, sql)
    if (code != Response.Status.CREATED.getStatusCode) {
      WarehouseRestApi.logerrors(j)
      throwserror(code, "SQL failed")
    }
    sqlid = Option(id)
    lastsql = Option(sql)
  }

  def waitForSQL() = {
    var waitforcompleted = true
    while (waitforcompleted) {
      L(s"Waiting for result ${lastsql.get}")
      val (code, j, status) = WarehouseRestApi.getListofSQLs(cred, token.get, sqlid.get)
      L(status)
      status match {
        case "completed" => {
          waitforcompleted = false
        }
        case "running" => {
          L("Still running");
          Thread.sleep(sleepSec * 1000)
        }
        case _ => {
          WarehouseRestApi.logerrors(j)
          throwserror(Response.Status.BAD_REQUEST.getStatusCode, "Failure, cannot complete")
        }
      } // match
    } // while
  }

  def deleteJob = {
    L("Remove load job logs")
    val (code, j) = WarehouseRestApi.deleteLoadJob(cred, token.get, jobid.get)
    if (code != Response.Status.OK.getStatusCode) {
      WarehouseRestApi.logerrors(j)
      throwserror(code, "Deleting load job failed")
    }

  }
}


