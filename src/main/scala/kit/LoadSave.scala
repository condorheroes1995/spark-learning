/**
 * Load and save data to/from DataFrames
 */
package kit
import java.util.Properties
import org.apache.spark.sql._

case class LoadSave(spark: SparkSession) {

  def write2JsonByPartition(input: DataFrame, path: String, partitionColumn: String): Unit = {
    input.write.partitionBy(partitionColumn).format("json").save(path)
  }

  def writeAppend(input: DataFrame, path: String): Unit = {
    input.write.mode(SaveMode.Append).save(path)
  }

  def loadJDBC(dbType: String, host: String, port: Int, user: String, password: String, dbName: String,
               tableName: String) : DataFrame = {

    val conditionStr = "useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
    val url = "jdbc:"+dbType+"://" + host + ":" + port + "/" + dbName + "?" + conditionStr
    val prop = new Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    var driverName: String =  dbType match {
      case "mysql" => "com.mysql.jdbc.Driver"
      case "postgresql" => "org.postgresql.Driver"
      case _ => "default"
    }
    prop.setProperty("driver", driverName)

    // one way to read jdbc tables
    spark.read.jdbc(url,tableName,prop)

   // an other way to read jdbc tables
    spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driverName)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName).load()
    }

  def writeJDBC(df: DataFrame,dbType: String, host: String, port: Int, user: String, password: String, dbName: String,
                tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {

    val conditionStr = "useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
    val url = "jdbc:"+dbType+"://" + host + ":" + port + "/" + dbName + "?" + conditionStr
    val prop = new Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)

    var driverName: String =  dbType match {
      case "mysql" => "com.mysql.jdbc.Driver"
      case "postgresql" => "org.postgresql.Driver"
      case _ => "default"
    }
    prop.setProperty("driver", driverName)

//    // one way to write df to jdbc
//    df.write.jdbc(url, tableName, prop)

    // an other way write df to jdbc
    df.write.format("jdbc").mode(mode)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName).save()
  }

  def loadParquet(path: String): DataFrame = {
    // Configure Spark to read binary data as string,
    // note: must be configured on session.
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    // Load parquet data using merge schema (configured through option)
    spark.read
      .option("mergeSchema", "true")
      .format("parquet")
      .load(path)
  }

  def writeParquet(df: DataFrame, path: String) = {
    df.write.format("parquet").save(path)
  }

  def loadHiveTable(tableName: String): DataFrame = {
    spark.read.table(tableName)
  }

  def saveManagedTable(df: DataFrame, tableName: String): Unit = {
    df.write.saveAsTable(tableName)
  }
}
