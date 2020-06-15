import com.databricks.dbutils_v1.DBUtilsHolder._

class MyStorageAccount(containerName: String, storageAccountName: String) {

  /*
  * Get SAS Token based on KeyVault
  */
  def getSasToken(keyVaultScope: String, secret: String): String = {
    val sasToken = dbutils.secrets.get(scope = keyVaultScope, key = secret)
    sasToken
  }

  /*
  * Get Mount Point in DBFS
  */
  def getMountPoint(directoryName: String): String ={
    val mountPoint = s"/mnt/${directoryName}"
    mountPoint
  }

  /*
  * Get URL in DBFS
  */
  def getUrl(directoryName: String): String = {
    val url = s"wasbs://" + containerName + "@" + storageAccountName + s".blob.core.windows.net/"
    url
  }

  /*
   * Get Configuration in DBFS
   */
  def getConfiguration(): String = {
    val config = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
    config
  }

  def mountBlobStorageContainerToDBFS(url: String, mountPoint: String, config: String,
                                      sasToken: String):Unit={
        try{
          println("Mounting container to DBFS...")
          dbutils.fs.mount(
            source = url,
            mountPoint = mountPoint,
            extraConfigs = Map(config -> sasToken))
        }
        catch{
          case e: java.rmi.RemoteException => {
            println("The container has already been mounted")
          }
        }
  }
}
