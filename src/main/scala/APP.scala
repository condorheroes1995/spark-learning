import kit.{LoadSave, SparkSessionHolder}

/**
  * just for test
  */
object APP {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionHolder.sparkSession;
    val loadSave = LoadSave(spark)
    val path = "data/other"

    // read mysql
    loadSave
      .loadJDBC("mysql","192.168.9.221",3306,"root","123456","zzl","zero")
      .show()

    // read postgresql
    loadSave
      .loadJDBC("postgresql","192.168.9.221",8082,"admin","admin","health_db","tbl_reg_people_birth")
      .show()

    val jsonRDD = spark.read.format("json").load(path+"/rawpanda.json")
    jsonRDD.show()
  }
}
