package sql.datasource.stxt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String], schema: StructType): BaseRelation = {

    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
    return new LegacyRelation(parameters.get("path").get, schema)(sqlContext)
  }

  def saveAsCsvFile(data: DataFrame, path: String) = {
    val dataCustomRDD = data.rdd.map(row => {
      val values = row.toSeq.map(value => value.toString)
      values.mkString(",")
    })
    dataCustomRDD.saveAsTextFile(path)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {

    saveAsCsvFile(data, parameters.get("path").get)
    createRelation(sqlContext, parameters, data.schema)
  }
}


class LegacyRelation(location: String, userSchema: StructType)
                    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      return this.userSchema
    }
    else {
      return StructType(Seq(StructField("name", StringType, true),
        StructField("age", IntegerType, true)))
    }
  }

  private def castValue(value: String, toType: DataType) = toType match {
    case _: StringType => value
    case _: IntegerType => value.toInt
  }

  override def buildScan(): RDD[Row] = {
    val schemaFields = schema.fields
    val rdd = sqlContext
      .sparkContext
      .wholeTextFiles(location)
      .map(x => x._2)

    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val finalLine = Array(lines(0),lines(1))
      val typedValues = finalLine.zipWithIndex.map {
        case (value, index) => {
          val dataType = schemaFields(index).dataType
          castValue(value, dataType)
        }
      }
      Row.fromSeq(typedValues)
    })
    rows
  }
}

