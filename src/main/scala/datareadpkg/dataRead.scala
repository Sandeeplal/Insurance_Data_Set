package datareadpkg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object dataRead {

  def main(args: Array[String]): Unit = {

//    Updating this functionality
    val sqlc = new SparkSession.Builder().master("local[*]").appName("SparkApp").enableHiveSupport().getOrCreate()
    val sc = sqlc.sparkContext
    sc.setLogLevel("ERROR")
    import sqlc.implicits._
    val path = "D:\\Hadoop\\Projects\\Project for submission\\Project 1\\Project 1_dataset_bank-full.csv"
    val db1 = sc.textFile(path)
    val db = db1.filter(l => !l.startsWith("\"age;"))
    val formatted_data = db.map(s => s.replace("\"", "")).map(s => s.split(";"))
    val val_df1 = formatted_data.map(p => Table(p(0).toInt, p(1), p(2), p(3), p(4), p(5).toInt, p(6), p(7), p(8), p(9).toInt, p(10), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toInt, p(15), p(16))).toDF()
    val_df1.persist()
    val_df1.createOrReplaceTempView("Customer")
    val success_rate = sqlc.sql("Select (Select count(y) from Customer where y='yes')/count(*) from Customer")
    println("Success_rate = " + success_rate.collectAsList().get(0).getDouble(0))
    val failure_rate = sqlc.sql("Select (Select count(y) from Customer where y='no')/count(*) from Customer")
    println("failure_rate = " + failure_rate.collectAsList().get(0).getDouble(0))
    /// Using data frame methods calcaulat min, max and average; imported sql.functions

    val avg_age = val_df1.select(mean("age"), max("age"), min("age")).collectAsList()
    println("Average age= " + avg_age.get(0).get(0) + "\nmax age = " + avg_age.get(0).get(1) + "\nmin age = " + avg_age.get(0).get(2))

    //4. Check quality of customers by checking average balance, median balance of customers
    val bal = val_df1.select(avg("balance")).collectAsList()
    println("Average of Balance amount in account is" + bal.get(0).get(0))

    //6. Check if marital status mattered for subscription to deposit.
    val martial_stat = sqlc.sql("Select marital as Status_of_people, count(marital) as Count_of_people from(Select marital from Customer where y='yes') Group By marital Order By Count_of_people desc")
    martial_stat.show()

    // 8. Do feature engineering for column—age and find right age effect on campaign
    val age_in_sub = sqlc.sql("Select age, count(age) as No_of_people from(Select age from Customer where y='yes') Group By age Order By No_of_people desc")
    age_in_sub.show()

  }
}


