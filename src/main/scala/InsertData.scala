import Helper.tocols
import org.apache.spark.sql.Row

import java.sql.Date

trait InsertData {

  def insertInt(pos: Int, data: Int, nullv: Boolean)

  def insertString(pos: Int, data: String, nullv: Boolean)

  def insertDouble(pos: Int, data: Double, nullv: Boolean)

  def insertBigDecimal(pos: Int, Data: java.math.BigDecimal, nullv: Boolean)

  def insertDate(pos: Int, Data: Date, nullv: Boolean)

  def insert(r: Row, fields: List[ReadFields.Field]) = {
    for (elem <- fields) {
      val nullv: Boolean = r.isNullAt(elem.rid)
      elem.ftype match {
        case ReadFields.FieldType.INT => {
          val intVal = r.getAs[Int](elem.rid)
          insertInt(elem.pos, intVal, nullv)
        }
        case ReadFields.FieldType.STRING => {
          val stringVal = r.getAs[String](elem.rid)
          insertString(elem.pos, stringVal, nullv)
        }
        case ReadFields.FieldType.DOUBLE => {
          val doubleVal = r.getAs[Double](elem.rid)
          insertDouble(elem.pos, doubleVal, nullv)
        }
        case ReadFields.FieldType.DECIMAL => {
          val deciVal = r.getAs[java.math.BigDecimal](elem.rid)
          insertBigDecimal(elem.pos, deciVal, nullv)
        }
        case ReadFields.FieldType.DATE => {
          val dateVal = r.getAs[java.sql.Date](elem.rid)
          insertDate(elem.pos, dateVal, nullv)
        }
      } // match
    }

  }

}