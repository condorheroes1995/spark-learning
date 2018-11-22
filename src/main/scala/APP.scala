import kit.{LoadSave, SparkSessionHolder}

object APP {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionHolder.sparkSession;
    val loadSave = LoadSave(spark)

    loadSave
      .loadJDBC("mysql","192.168.9.221",3306,"root","123456","zzl","zero")
      .show()

    loadSave
      .loadJDBC("postgresql","192.168.9.221",8082,"admin","admin","health_db","tbl_reg_people_birth")
      .show()
  }
}
