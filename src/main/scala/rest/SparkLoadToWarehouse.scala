package rest

case class SparkLoadToWarehouse(cred: WarehouseCred, inputpath: String, schema : String, table : String, tabledest : String, delim : String) {

  val load = new LoadWarehouse(cred)

  def init = {
    load.acquireToken()
    load.runSQL(s"DROP TABLE $schema.$table IF EXISTS")
    load.waitForSQL

    load.runSQL(s"CREATE TABLE $schema.$table LIKE $schema.$tabledest ")
    load.waitForSQL
  }

  def loadJob = {
    load.runSQL(s"TRUNCATE TABLE $schema.$table IMMEDIATE")
    load.waitForSQL

    load.LoadJob(inputpath, schema, table, delim)
    load.LoadWait(s"$inputpath into $schema.$table")

    load.runSQL(s"INSERT INTO $schema.$tabledest (SELECT * FROM $schema.$table)")
    load.waitForSQL

    load.deleteJob
  }

  def close = load.releaseToken

}


