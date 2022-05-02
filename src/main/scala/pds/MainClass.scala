package pds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object MainClass {

  def main(args: Array[String]) {

    val Mongo_db_url="172.31.249.13:27017"
    val db_name="test"
    val db_user="smartdata"
    val db_pwd="smartdata"
    val db_collection = "NDpersonnelODS"
    val db_collection2 = "homeOC"
    val db_collection3 = "patients"
    val db_collection4 = "residence"
    val db_collection5 = "temps"

    val Maria_db_url="jdbc:mysql://172.31.249.12:3306/NDdatawarehouse"



    Logger.getLogger("org").setLevel(Level.OFF)

    //val spark1 : SparkSession = new ConnectionToMongoDB().Session(Mongo_db_url,db_name, db_user , db_pwd,db_collection)
    val spark: SparkSession = SparkSession.builder.master("local").appName("MariaDB_test").getOrCreate()

    //Script_Residence.InsertResidence(spark)
    //val connectionProperties = new Properties()
    //connectionProperties.put("user","admin")
    //connectionProperties.put("password","admin")

    val DFhumanRessources = spark.read.format("jdbc")
      .option("url",Maria_db_url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "humanRessources")
      .option("user", "admin")
      .option("password", "admin")
      .load().toDF();

    //DFhumanRessources.show()
    val DFpersonnel = spark.read.format("jdbc")
      .option("url",Maria_db_url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "NDpersonnelDim")
      .option("user", "admin")
      .option("password", "admin")
      .load().toDF();
    DFpersonnel.printSchema()

    DFpersonnel.registerTempTable("workForce")
    val workForce2000 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2000 and endDate > 2000 GROUP BY residenceID ").toDF()
    val workForce2001 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2001 and endDate > 2001 GROUP BY residenceID ").toDF()
    val workForce2002 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2002 and endDate > 2002 GROUP BY residenceID ").toDF()
    val workForce2003 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2003 and endDate > 2003 GROUP BY residenceID ").toDF()
    val workForce2004 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2004 and endDate > 2004 GROUP BY residenceID ").toDF()
    val workForce2005 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2005 and endDate > 2005 GROUP BY residenceID ").toDF()
    val workForce2006 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2006 and endDate > 2006 GROUP BY residenceID ").toDF()
    val workForce2007 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2007 and endDate > 2007 GROUP BY residenceID ").toDF()
    val workForce2008 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2008 and endDate > 2008 GROUP BY residenceID ").toDF()
    val workForce2009 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2009 and endDate > 2009 GROUP BY residenceID ").toDF()
    val workForce2010 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2010 and endDate > 2010 GROUP BY residenceID ").toDF()
    val workForce2011 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2011 and endDate > 2011 GROUP BY residenceID ").toDF()
    val workForce2012 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2012 and endDate > 2012 GROUP BY residenceID ").toDF()
    val workForce2013 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2013 and endDate > 2013 GROUP BY residenceID ").toDF()
    val workForce2014 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2014 and endDate > 2014 GROUP BY residenceID ").toDF()
    val workForce2015 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2015 and endDate > 2015 GROUP BY residenceID ").toDF()
    val workForce2016 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2016 and endDate > 2016 GROUP BY residenceID ").toDF()
    val workForce2017 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2017 and endDate > 2017 GROUP BY residenceID ").toDF()
    val workForce2018 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2018 and endDate > 2018 GROUP BY residenceID ").toDF()
    val workForce2019 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2019 and endDate > 2019 GROUP BY residenceID ").toDF()
    val workForce2020 = spark.sqlContext.sql("SELECT residenceID,COUNT(*) AS wrkF FROM workForce Where startDate <= 2020 and endDate >= 2020 GROUP BY residenceID ").toDF()

    val workForce1 = workForce2000.union(workForce2001)
    val workForce2 = workForce1.union(workForce2002)
    val workForce3 = workForce2.union(workForce2003)
    val workForce4 = workForce3.union(workForce2004)
    val workForce5 = workForce4.union(workForce2005)
    val workForce6 = workForce5.union(workForce2006)
    val workForce7 = workForce6.union(workForce2007)
    val workForce8 = workForce7.union(workForce2008)
    val workForce9 = workForce8.union(workForce2009)
    val workForce10 = workForce9.union(workForce2010)
    val workForce11 = workForce10.union(workForce2011)
    val workForce12 = workForce11.union(workForce2012)
    val workForce13 = workForce12.union(workForce2013)
    val workForce14 = workForce13.union(workForce2014)
    val workForce15 = workForce14.union(workForce2015)
    val workForce16 = workForce15.union(workForce2016)
    val workForce17 = workForce16.union(workForce2017)
    val workForce18 = workForce17.union(workForce2018)
    val workForce19 = workForce18.union(workForce2019)
    val workForce20 = workForce19.union(workForce2020)



    workForce20.show()
    //var ultimate_workforce = workForce20.join(DFhumanRessources, workForce20("residenceID")=== DFhumanRessources("residenceID"))

    var TworkForce = workForce20.join(DFhumanRessources)
    TworkForce.show()
    //ultimate_workforce.show()
    //ultimate_workforce.count()


    //DFpersonnel.show()






    //TransformDataSet1.HoursPerWeek()
    //Hdfs.write("test.txt", "test.txt".getBytes)

    //Scripts Mock

    //Script_Residence.InsertResidence(spark);
    //Script_Temps.InsertDate(spark);

    // Insertion & Transformation DataSets
    //implicit val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    //val conf = new SparkConf().setAppName("TransformationDataSet").setMaster("local")
    //val sc = new SparkContext(conf)

    //DataSet Adult.data
    //val file5 = "adult.data"
    //val file :String = Hdfs.load(file5)
    //TransformDataSet1.tranformData(spark,file5)

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
