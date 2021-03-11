package rest

object RestTestMain {

  def main(args: Array[String]): Unit = {

    println("Hello")
    //    val path="sbtest/out/export-0"
    val path = "sbtest/l.csv"
    val schema = "STANBTEST"
    val tabledest = "PARQUETTA"
    val table = "PARQUETTA1"
    val delim = "|"

    val url = "XXXXX"
    val user = "XXXX"
    val password = "XXXX"

    val AWSKEY = "XXXX"
    val AWSSECRETKEY = "XXXX"
    val ENDPOINT = "XXXXX"
    val BUCKET = "XXXXXX"

    val cred = WarehouseCred(user, password, url, AWSKEY, AWSSECRETKEY, ENDPOINT, BUCKET)
    val load = SparkLoadToWarehouse(cred,path,schema, table,tabledest,delim)
    load.init
    load.loadJob(0)
    load.close

//    val load = new LoadWarehouse(cred)

//    load.acquireToken()
//    load.runSQL(s"DROP TABLE $schema.$table IF EXISTS")
//    load.waitForSQL

//    load.runSQL(s"CREATE TABLE $schema.$table LIKE $schema.$tabledest ")
//    load.waitForSQL

//    load.runSQL(s"TRUNCATE TABLE $schema.$table IMMEDIATE")
//    load.waitForSQL

    //    load.runSQL(s"SELECT COUNT(*) FROM $schema.$tabledest")
//    load.waitForSQL
//    load.LoadJob(path, schema, table,delim)
//    load.LoadWait

//    load.runSQL(s"INSERT INTO $schema.$tabledest (SELECT * FROM $schema.$table)")
//    load.waitForSQL

//    load.releaseToken

  }
}
