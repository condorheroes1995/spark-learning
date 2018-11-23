package apis.dataframe

import apis.common.{PandaPlace, RawPanda}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

case class DFOptions(spark: SparkSession) {

  def createFromCaseClassRDD(input: RDD[PandaPlace]) = {

    // Create DataFrame explicitly using session and schema inference

    import spark.implicits._
    val df1 = spark.createDataFrame(input)

    // Create DataFrame using session implicits and schema inference
    val df2 = input.toDF()

    // Create a Row RDD from our RDD of case classes
    val rowRDD = input.map(pm => Row(pm.name,
      pm.pandas.map(pi => Row(pi.id, pi.zip, pi.happy, pi.attributes))))

    val pandasType = ArrayType(StructType(List(
      StructField("id", LongType, true),
      StructField("zip", StringType, true),
      StructField("happy", BooleanType, true),
      StructField("attributes", ArrayType(FloatType), true))))

    // Create DataFrame explicitly with specified schema
    val schema = StructType(List(StructField("name", StringType, true),
      StructField("pandas", pandasType)))

    val df3 = spark.createDataFrame(rowRDD, schema)
  }

  def createFromCaseClassRDD(input: Seq[PandaPlace]) = {
    val rdd = spark.sparkContext.parallelize(input)
    // Create DataFrame explicitly using session and schema inference
    val df1 = spark.createDataFrame(input)
  }

  def createAndPrintSchema() = {
    val damao = RawPanda(1, "M1B 5K7", "giant", true, Array(0.1, 0.1))
    val pandaPlace = PandaPlace("toronto", Array(damao))
    val df = spark.createDataFrame(Seq(pandaPlace))
    df.printSchema()
  }

  def createFromLocal(input: Seq[PandaPlace]) = {
    spark.createDataFrame(input)
  }

  def collectDF(df: DataFrame) = {
    val result: Array[Row] = df.collect()
    result
  }

  /**
    * convert to RDD
    * @param input DataFrame
    * @return RDD
    */
  def toRDD(input: DataFrame): RDD[RawPanda] = {
    val rdd: RDD[Row] = input.rdd
    rdd.map(row => RawPanda(row.getAs[Long](0), row.getAs[String](1),
      row.getAs[String](2), row.getAs[Boolean](3), row.getAs[Array[Double]](4)))
  }

  /**
    * convert to Dataset
    * @param input DataFrame
    * @return Dataset
    */
  def toDataset(input: DataFrame): Dataset[RawPanda] = {
    import spark.implicits._
    val rdd: RDD[Row] = input.rdd
    rdd.map(row => RawPanda(row.getAs[Long](0), row.getAs[String](1),
      row.getAs[String](2), row.getAs[Boolean](3), row.getAs[Array[Double]](4))).toDS()
  }

}
