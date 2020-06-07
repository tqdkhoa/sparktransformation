import com.azure.storage.blob.models.BlobStorageException
import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.BlobErrorCode
import com.azure.storage.blob.models.BlobItem

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, Metadata, StringType, StructField, StructType, TimestampType}

object SparkTransformation {

  def main(args: Array[String]): Unit = {

    val sasToken = "?sv=2019-10-10&ss=b&srt=sco&sp=rwdlacx&se=2020-06-06T23:24:06Z&st=2020-06-06T15:24:06Z&spr=https,http&sig=bVJZQ2jh077u2MasyqAqwsJ3R1zYo2qDyrEhJ4a5Nw4%3D"
    val serviceClient = new BlobServiceClientBuilder()
      .endpoint("https://tqdkhoaexcercise1.blob.core.windows.net")
      .sasToken(sasToken)
      .buildClient()

    /**
     * Create the Spark Context
     */
    val sc = SparkSession.builder()
      .appName("Spark Transforamtion")
      .config("spark.master", "local")
      .getOrCreate()

    /**
     * Read the expertSchema
     */
    val expectedSchema = StructType(Array(
      StructField("registration_dttm", TimestampType, false),
      StructField("id", IntegerType, false),
      StructField("first_name", StringType, false),
      StructField("last_name", StringType, false),
      StructField("email", StringType, true),
      StructField("gender", StringType, false),
      StructField("ip_address", StringType, false),
      StructField("cc", StringType, true),
      StructField("country", StringType, true),
      StructField("birthdate", StringType, true),
      StructField("salary", DoubleType, false),
      StructField("title", StringType, true),
      StructField("comments", StringType, true),
    ))

    try {
      val containerClient = serviceClient.getBlobContainerClient("level2")
//      containerClient.listBlobs().stream.forEach(blob => println(s"File names:${blob.getName}"))
      containerClient.listBlobs().stream.forEach(
        blob => if(blob.getName.contains("userdata1.parquet")){
          println(s"Reading file ${blob.getName}")
          val parquetFileDF = sc.read.parquet(blob.getName)
          parquetFileDF.createOrReplaceTempView("parquetFile")
          val firstNameDF = sc.sql("SELECT first_name FROM parquetFile")
          firstNameDF.show()
      })

    } catch {
      case ex: BlobStorageException => if (!ex.getErrorCode.equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) throw ex
    }

    sc.stop()
  }
}
