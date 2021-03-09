package rest

import com.google.common.io.CharStreams
import okhttp3.{HttpUrl, MediaType, OkHttpClient, Request, RequestBody, Response}
import org.apache.spark.sql.execution.streaming.CommitMetadata.format
import org.json4s
import org.json4s.{JArray, JNull, JValue, JsonAST}
import org.json4s.JsonAST.{JObject, JString, JValue}
import org.json4s.jackson.{Json, JsonMethods, parseJson}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{pretty, render}
import org.json4s.jackson.JsonMethods._

import javax.ws.rs.core.Response.Status
import java.io.{InputStream, InputStreamReader}
import java.util.logging.Logger
import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSONObject

object WarehouseRestApi {

  private val log: Logger = Logger.getLogger("WarehouseRestApi")

  def logerrors(j: JValue) = {
    log.info(pretty(render(j)))
  }

  private def execute(client: OkHttpClient, request: Request): Try[Response] =
    Try(client.newCall(request).execute)

  private def runexecute(client: OkHttpClient, request: Request): Response = {
    var response: Response = null
    var i = 0
    val no = 3

    while (response == null) {
      response = execute(client, request) match {
        case Success(response) => response
        case default  => {
          if (i == no) throw new Exception(default.toString)
          i = i+1
          log.severe(default.toString)
          log.info(s"$i max: $no Exception during HTTP call, trying again")
          null
        }
      } // match
    } // while
    response
  }


  private def executeResponse(client: OkHttpClient, request: Request): (Int, JValue) = {
    val response: Response = execute(client, request) match {
      case Success(response) => response
      case Failure(response) => null
    }
    //    }  response = client.newCall(request).execute
    val i: InputStream = response.body().byteStream()
    val reader = new InputStreamReader(i)
    val text = CharStreams.toString(reader)
    (response.code(), if (text == "") JsonAST.JNull else parse(text))
  }

  private def makePostRestCall(cred: WarehouseCred, token: Option[String], rest: String, call: JObject): (Int, JValue) = {
    val d = pretty(render(call))
    val client: OkHttpClient = new OkHttpClient
    val mediaType = MediaType.parse("application/json")
    val body = RequestBody.create(mediaType, d)
    val req: Request.Builder = new Request.Builder().url(s"https://${cred.url}/dbapi/v4/$rest").post(body).addHeader("content-type", "application/json")
    val re = if (token.isEmpty) req else req.addHeader("authorization", s"Bearer ${token.get}")
    val request = re.build
    executeResponse(client, request)
  }

  private def makeGetRestCall(cred: WarehouseCred, token: String, rest: String): (Int, JValue) = {
    val client: OkHttpClient = new OkHttpClient
    val request: Request = new Request.Builder().url(s"https://${cred.url}/dbapi/v4/$rest")
      .get().addHeader("content-type", "application/json").
      addHeader("authorization", s"Bearer $token").build();
    executeResponse(client, request)
  }

  private def makeDeleteRestCall(cred: WarehouseCred, token: String, rest: String): (Int, JValue) = {
    val client = new OkHttpClient
    val request: Request = new Request.Builder()
      .url(s"https://${cred.url}/dbapi/v4/$rest")
      .delete(null)
      .addHeader("content-type", "application/json")
      .addHeader("authorization", s"Bearer $token")
      .build();
    val response: Response = client.newCall(request).execute
    executeResponse(client, request)
  }

  def acquireTokenString(cred: WarehouseCred): (Int, JValue, String) = {

    val json = ("userid" -> cred.user) ~ ("password" -> cred.password)

    //    println(pretty(render(json)))
    val (code, j) = makePostRestCall(cred, Option.empty, "auth/tokens", json)
    val tokenS = (j \ "token").extract[String]
    (code, j, tokenS)
  }

  def releaseToken(cred: WarehouseCred, token: String) = {

    makeDeleteRestCall(cred, token, "auth/token/logout")

  }

  def getListOfJobs(cred: WarehouseCred, token: String, jobid: String): (Int, JValue, String, JValue) = {
    val (code, j) = makeGetRestCall(cred, token, "load_jobs")

    if (code != Status.OK.getStatusCode) {
      logerrors(j)
      return (code, j, "", JNull)
    }

    val list = (j \ "resources").extract[JArray]
    val job = list.arr.find(e => {
      val id = (e \ "id").extract[String]
      id == jobid
    })
    if (job.isEmpty) (Status.NOT_FOUND.getStatusCode, JNull, "", JNull)
    else {
      val j = job.get
      val sta = (j \ "status")
      val st = (sta \ "status").extract[String]
      (code, job.get, st, sta)
    }
  }

  def getListofSQLs(cred: WarehouseCred, token: String, sqlid: String): (Int, JValue, String) = {
    val (code, j) = makeGetRestCall(cred, token, "sql_jobs/" + sqlid)
    val res = j \ "results"
    val status = (j \ "status").extract[String]
    (code, res, status)
  }

  def runSQLCommand(cred: WarehouseCred, token: String, sql: String): (Int, JValue, String) = {
    val json = ("commands" -> sql) ~ ("separator" -> ';') ~ ("stop_on_error" -> "yes")
    val (code, j) = makePostRestCall(cred, Option(token), "sql_jobs", json)
    if (code != Status.CREATED.getStatusCode) {
      logerrors(j)
      (code, j, "")
    }
    else {
      val id = (j \ "id").extract[String]
      (code, j, id)
    }
  }

  def startLoadJob(cred: WarehouseCred, token: String, path: String, schema: String, table: String, delim: String): (Int, JValue, String) = {

    val json = ("load_source" -> "S3") ~
      ("load_action" -> "INSERT") ~
      ("schema" -> schema) ~
      ("table" -> table) ~
      ("auto_create_table" -> ("execute" -> "no")) ~
      ("cloud_source" ->
        ("endpoint" -> cred.ENDPOINT) ~ ("path" -> s"${cred.BUCKET}::$path") ~
          ("auth_id" -> cred.AWSKEY) ~ ("auth_secret" -> cred.AWSSECRETKEY)) ~
      ("file_options" ->
        ("has_header_row" -> "no") ~ ("column_delimiter" -> delim))

    //    println(pretty(render(json)))
    val (code, j) = makePostRestCall(cred, Option(token), "load_jobs", json)
    if (code != Status.CREATED.getStatusCode) {
      logerrors(j)
      (code, j, "")
    }
    else {
      val id = (j \ "id").extract[String]
      (code, j, id)
    }
  }

  def deleteLoadJob(cred: WarehouseCred, token: String, jobid: String): (Int, JValue) = {
    makeDeleteRestCall(cred, token, s"load_jobs/$jobid")
  }
}

