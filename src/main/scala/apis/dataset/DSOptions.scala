package apis.dataset
import apis.common.{CoffeeShop, RawPanda}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class MiniPandaInfo(zip: String, size: Double)

class MixedDataset(spark: SparkSession) {
  import spark.implicits._

  /**
   * A sample function on a Dataset of RawPandas.
   *
   * This is contrived, since our reduction could also be done with SQL aggregates,
   * but we can see the flexibility of being able to specify arbitrary Scala code.
   */
  def happyPandaSums(ds: Dataset[RawPanda]): Double = {
    ds.toDF().filter($"happy" === true).as[RawPanda].
      select($"attributes"(0).as[Double]).
      reduce((x, y) => x + y)
  }

  /**
   * A sample function on a Dataset of RawPandas.
   * Use the first attribute to deterimine if a panda is squishy.
   */
  def squishyPandas(ds: Dataset[RawPanda]): Dataset[(Long, Boolean)] = {
    ds.select($"id".as[Long], ($"attributes"(0) > 0.5).as[Boolean])
  }

  /**
   * Union happy and sad pandas
   */

  def unionPandas(happyPandas: Dataset[RawPanda], sadPandas: Dataset[RawPanda]) = {
    happyPandas.union(sadPandas)
  }


  /**
   * Functional map + Dataset, sums the positive attributes for the pandas
   */
  def funMap(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.map{rp => rp.attributes.filter(_ > 0).sum}
  }

  def maxPandaSizePerZip(ds: Dataset[RawPanda]): Dataset[(String, Double)] = {
    ds.map(rp => MiniPandaInfo(rp.zip, rp.attributes(2)))
      .groupByKey(mp => mp.zip).agg(max("size").as[Double])
  }

  def maxPandaSizePerZipScala(ds: Dataset[RawPanda]): Dataset[(String, Double)] = {
    ds.groupByKey(rp => rp.zip).mapGroups{ case (g, iter) =>
      (g, iter.map(_.attributes(2)).reduceLeft(Math.max(_, _)))
    }
  }

  /**
   * Illustrate how we make typed queries, using some of the float properties
   * to produce boolean values.
   */
  def typedQueryExample(ds: Dataset[RawPanda]): Dataset[Double] = {
    ds.select($"attributes"(0).as[Double])
  }

  /**
   * Illustrate Dataset joins
   */
  def joinSample(pandas: Dataset[RawPanda], coffeeShops: Dataset[CoffeeShop]):
      Dataset[(RawPanda, CoffeeShop)] = {
    val result: Dataset[(RawPanda, CoffeeShop)] = pandas.joinWith(coffeeShops,
      $"zip" === $"zip")
    result
  }

  /**
   * Illustrate a self join to compare pandas in the same zip code
   */
  def selfJoin(pandas: Dataset[RawPanda]):
      Dataset[(RawPanda, RawPanda)] = {
    val result: Dataset[(RawPanda, RawPanda)] = pandas.joinWith(pandas,
      $"zip" === $"zip")
    result
  }


  /**
   * Illustrate converting an RDD to DS
   */
  def fromRDD(rdd: RDD[RawPanda]): Dataset[RawPanda] = {
    rdd.toDS
  }

  /**
   * Illustrate converting a Dataset to an RDD
   */
  def toRDD(ds: Dataset[RawPanda]): RDD[RawPanda] = {
    ds.rdd
  }

  /**
   * Illustrate converting a Dataset to a DataFrame
   */
  def toDF(ds: Dataset[RawPanda]): DataFrame = {
    ds.toDF()
  }

  /**
   * Illustrate DataFrame to Dataset. Its important to note that if the schema
   * does not match what is expected by the Dataset this fails fast.
   */

  def fromDF(df: DataFrame): Dataset[RawPanda] = {
    df.as[RawPanda]
  }
}
