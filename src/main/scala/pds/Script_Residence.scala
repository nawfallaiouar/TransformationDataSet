package pds

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object Script_Residence {

  def InsertResidence(spark: SparkSession): Unit = {

    val colums = Seq("name", "size", "region", "country");
    val data = Seq(("White-Rose", "20000m2", "South-West", "United-States"), ("Gracefield-House", "10000m2", "North-West", "United-States"), ("Tremor-Sense", "80000m2", "South-West", "Mexico"), ("Arkham-Asylum", "50000m2", "North-East", "United-States"),("1erNovembre", "10000m2", "Ile-de-France", "France"),("Manoir","5000m2","Ile-de-France","France"),("Jardin","11000m2","Ile-de-France","France"),("Lac-d'argent","10000m2","Alpes","France"),("Mayonnaise","50000m2","Ile-de-France","France"));
    val rdd = spark.sparkContext.parallelize(data);

    import spark.implicits._
    val dfFROM = rdd.toDF("name","size","region","country")
    dfFROM.printSchema()
    dfFROM.show()
    //dfFROM.write.mode("Overwrite").format("Parquet").save("hdfs://172.31.249.250:8020//tmp//test123") write in HDFS
    MongoSpark.save(dfFROM)

  }
}