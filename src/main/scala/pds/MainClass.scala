package pds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MainClass {

  def main(args: Array[String]) {

    val db_url="172.31.249.14:27017"
    val db_name="test"
    val db_user="smartdata"
    val db_pwd="smartdata"
    val db_collection = "personnel"
    val db_collection2 = "homeOC"
    val db_collection3 = "patients"



    Logger.getLogger("org").setLevel(Level.OFF)

    val spark : SparkSession = new ConnectionToMongoDB().Session(db_url,db_name, db_user , db_pwd,db_collection)

    //implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //val conf = new SparkConf().setAppName("TransformationDataSet").setMaster("local")
    //val sc = new SparkContext(conf)

    //DataSet Adult.data
    val file5 = "adult.data"
    val file :String = Hdfs.load(file5)
    TransformDataSet1.tranformData(spark,file)

    //DataSet Objets Connectés
    //val file4 = "HomeC.csv"
    //val file2 :String = Hdfs.load(file4)
    //TransformDataSet2.tranformData(spark,file2)

    //DataSet Objets Connectés
    //val file6 = "Heart_Disease_Data.csv"
    //val file3 :String =  Hdfs.load(file6)
    //TransformDataSet3.tranformData(spark,file3)



  }

}
