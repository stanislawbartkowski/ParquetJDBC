import org.apache.spark.sql
import org.apache.spark.sql.types.StructType

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConverters._

object ReadFields {

  object FieldType extends Enumeration {
    type FieldType = Value
    val STRING, INT, DOUBLE, DECIMAL, DATE = Value
  }

  case class Field(rid: Int, rddname: String, pos: Int, sqlcol : String, ftype: FieldType.FieldType)

  def readList(path: String, IsProperty: (String) => Boolean, rdd: sql.DataFrame): List[Field] = {
    val prop = new Properties()
    val sche: StructType = rdd.schema


    prop.load(new FileInputStream(path))
    var l: List[Field] = List();

    prop.propertyNames().asScala.filter(f => !IsProperty(f.toString)).foreach(f => {
      val key: String = f.toString
      val (rname,sname) = Helper.tocols(key)
      val rddinde: Int = sche.fields.indexWhere(p => p.name == rname)
      if (rddinde == -1) {
        Helper.Severe(s"$key does not exist in Parquet schema")
        System.exit(4)
      }
      val vals = prop.get(key).toString.split(",")
      val ftype: FieldType.FieldType = FieldType.withName(vals(1).trim)

      l = l :+ Field(rddinde, rname, vals(0).toInt, sname, ftype)
    }
    )
    l
  }

}
