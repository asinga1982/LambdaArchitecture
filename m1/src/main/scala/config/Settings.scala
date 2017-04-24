package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("clickstream")

    // Reads values from application.conf file
    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val pages = weblogGen.getInt("pages")
    lazy val visitors = weblogGen.getInt("visitors")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numFile = weblogGen.getInt("nbr_files")
    lazy val hdfsPath = weblogGen.getString("hdfs_Path")
  }
}
