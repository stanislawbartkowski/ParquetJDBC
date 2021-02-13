import java.io.FileInputStream
import java.util.Properties

import scala.collection.JavaConverters._

object ReadFields {

  object FieldType extends Enumeration {
    type FieldType = Value
    val STRING, INT,DOUBLE, DECIMAL,DATE = Value
  }

  case class Field(name: String, pos: Int, ftype: FieldType.FieldType)

  def readList(path: String, IsProperty: (String) => Boolean): List[Field] = {
    val prop = new Properties()

    prop.load(new FileInputStream(path))
    var l: List[Field] = List();

    prop.propertyNames().asScala.filter(f => !IsProperty(f.toString)).foreach(f => {
      val key: String = f.toString
      val vals = prop.get(key).toString.split(",")
      val ftype: FieldType.FieldType = FieldType.withName(vals(1))

      l = l :+ Field(key, vals(0).toInt, ftype)
    }
    )
    l
  }

}
