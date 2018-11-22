package wordcount


import kit.{LoadSave, SparkSessionHolder}
import org.apache.spark.rdd._

object WordCount {
  // bad idea: uses group by key
  def badIdea(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val grouped = wordPairs.groupByKey()
    val wordCounts = grouped.mapValues(_.sum)
    wordCounts
  }

  // good idea: doesn't use group by key
  def simpleWordCount(rdd: RDD[String]): RDD[(String, Int)] = {
    val words = rdd.flatMap(_.split(" "))
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }

  /**
    * Come up with word counts but filter out the illegal tokens and stop words
    */
  def withStopWordsFiltered(rdd : RDD[String], illegalTokens : Array[Char],
    stopWords : Set[String]): RDD[(String, Int)] = {
    val separators = illegalTokens ++ Array[Char](' ')
    val tokens: RDD[String] = rdd.flatMap(_.split(separators).
      map(_.trim.toLowerCase))
    val words = tokens.filter(token =>
      !stopWords.contains(token) && (token.length > 0) )
    val wordPairs = words.map((_, 1))
    val wordCounts = wordPairs.reduceByKey(_ + _)
    wordCounts
  }

  /**
    * just for test
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sc  = SparkSessionHolder.sparkSession.sparkContext
    var dataPath = "data/other"
    val rdd = sc.textFile(dataPath + "/README.md")
    // call badIdea do word count
    badIdea(rdd).collect().foreach(println(_))
    // call goodIdea do word count
    simpleWordCount(rdd).collect().foreach(println(_))

    /**
      * filter illegal token and stop words
      */
    val illegalToken = Array('[','.','\"',',','[')
    val stopWords = Set("a","the","of")
    withStopWordsFiltered(rdd, illegalToken, stopWords).collect().foreach(println(_))
  }
}
