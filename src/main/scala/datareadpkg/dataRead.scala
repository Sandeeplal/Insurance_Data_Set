package datareadpkg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object dataRead {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkMe App")
    //  val spark = new SparkSession.Builder().master("local[*]").appName("SparkMe App")
    val sc = new SparkContext(conf)
    val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlc.implicits._
    val path = "D:\\Hadoop\\Projects\\Project for submission\\Project 1\\Project 1_dataset_bank-full.csv"
    //    val basedb = spark.read.format("csv").option("delimiter", ";").option("header", "true").option("inferSchema", "true")
    //      .load(path)
    //    basedb.printSchema()

    val db1 = sc.textFile(path)
    val db = db1.filter(l => !(l.startsWith("\"age;")))
    val formatted_data = db.map(s => s.replace("\"", "")).map(s => s.split(";"))
    val val_df1 = formatted_data.map(p => Table(p(0).toInt, p(1), p(2), p(3), p(4), p(5).toInt, p(6), p(7), p(8), p(9).toInt, p(10), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toInt, p(15), p(16))).toDF()
    val s = val_df1.createOrReplaceTempView("Customer")
    sqlc.sql("Select (Select count(y) from Customer where y='yes')/count(*) from Customer").show()

  }
}

case class Table(age: Int = 0, job: String, marital: String, education: String, default_val: String, balance: Int, housing: String, loan: String, contact: String, day: Int, month: String, duration: Int, campaign: Int, pdays: Int, previous: Int, poutcome: String, y: String)
