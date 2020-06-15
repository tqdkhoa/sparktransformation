import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, TimestampType}

object SparkTransformation {

  def main(args: Array[String]): Unit = {

    val sourceContainer = "level2"
    val storageAccountName = "<storage_account_name>"

    val sasToken = "<sas_token_string>"

    val level2Storage = new MyStorageAccount(sourceContainer, storageAccountName)
    var mountPoint = level2Storage.getMountPoint("level2")
    var url = level2Storage.getUrl("level2")
    var config = level2Storage.getConfiguration()
    level2Storage.mountBlobStorageContainerToDBFS(url, mountPoint, config, sasToken)

    val spark = SparkSession.builder()
      .appName("Spark Transforamtion")
      .config("spark.master", "local")
      .getOrCreate()

    //Only be used for access Blob Storage directly
//    spark.sparkContext.hadoopConfiguration.set(
//      "fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//    spark.sparkContext.hadoopConfiguration.set(
//      s"fs.azure.sas.${sourceContainer}.${storageAccountName}.blob.core.windows.net",
//      sasToken
//    )

    val df = spark.read.parquet(mountPoint)

    if(df.schema.contains(StructField("registration_dttm",TimestampType,true))
      && df.schema.contains(StructField("id",IntegerType,true))
      && df.schema.contains(StructField("first_name",StringType,true))
      && df.schema.contains(StructField("last_name",StringType,true))
      && df.schema.contains(StructField("email",StringType,true))
      && df.schema.contains(StructField("gender",StringType,true))
      && df.schema.contains(StructField("cc",StringType,true))
      && df.schema.contains(StructField("country",StringType,true))
      && df.schema.contains(StructField("birthdate",StringType,true))
      && df.schema.contains(StructField("salary",DoubleType,true))
      && df.schema.contains(StructField("title",StringType,true))
      && df.schema.contains(StructField("comments",StringType,true))){
      println("Schema is valid")
    } else{
      println("Schema is invalid")
    }

    // in below solution, the column name in the schema is the same, just change to have the alias
//    val tag = "this column has been modified"
//    val metadata = new sql.types.MetadataBuilder().putString("tag", tag).build()
//    val newColumn = df.col(old_colname).as(new_colname, metadata)
//    val parq_rn_col_DF = df.withColumn(old_colname, newColumn)

    val renamedDF = df.withColumnRenamed("cc", "cc_mod")
    val distinctDF = renamedDF.distinct()

    val destinationContainer = "level3"
    val level3Storage = new MyStorageAccount(destinationContainer, storageAccountName)
    mountPoint = level3Storage.getMountPoint("level3")
    url = level3Storage.getUrl("level3")
    config = level3Storage.getConfiguration()
    level3Storage.mountBlobStorageContainerToDBFS(url, mountPoint, config, sasToken)

    println(s"Destination Dir ${destinationContainer}")
    distinctDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .save(mountPoint)

    spark.stop()
  }
}
