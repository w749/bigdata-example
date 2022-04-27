package org.mlamp.hdfs

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.huawei.security.LoginUtil

object HdfsLogin {
  private val LOG = LogFactory.getLog(HdfsLogin.getClass)
  private val PATH_TO_HDFS_SITE_XML = classOf[HdfsLogin].getClassLoader.getResource("hdfs-site.xml").getPath
  private val PATH_TO_CORE_SITE_XML = classOf[HdfsLogin].getClassLoader.getResource("core-site.xml").getPath
  private val PRNCIPAL_NAME = "hiveuser"
  private val PATH_TO_KEYTAB = classOf[HdfsLogin].getClassLoader.getResource("user.keytab").getPath
  private val PATH_TO_KRB5_CONF = classOf[HdfsLogin].getClassLoader.getResource("krb5.conf").getPath
  private var conf: Configuration = _
  private var fSystem: FileSystem = _

  def apply(): FileSystem = {
    confLoad()
    authentication()
    instanceBuild()
    this.fSystem
  }

  /**
   * build HDFS instance
   */
  private def instanceBuild(): Unit = { // get filesystem
    // 一般情况下，FileSystem对象JVM里唯一，是线程安全的，这个实例可以一直用，不需要立马close。
    // 注意：
    // 若需要长期占用一个FileSystem对象的场景，可以给这个线程专门new一个FileSystem对象，但要注意资源管理，别导致泄露。
    // 在此之前，需要先给conf加上：
    // conf.setBoolean("fs.hdfs.impl.disable.cache",
    // true);//表示重新new一个连接实例，不用缓存中的对象。
    fSystem = FileSystem.get(conf)
  }

  /**
   * Add configuration file if the application run on the linux ,then need
   * make the path of the core-site.xml and hdfs-site.xml to in the linux
   * client file
   */
  private def confLoad(): Unit = {
    System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF)
    conf = new Configuration()
    // conf file
    conf.addResource(new Path(PATH_TO_CORE_SITE_XML))
    conf.addResource(new Path(PATH_TO_HDFS_SITE_XML))

    LOG.info(this.getClass + ": Load configs complete")
  }

  /**
   * kerberos security authentication if the application running on Linux,need
   * the path of the krb5.conf and keytab to edit to absolute path in Linux.
   * make the keytab and principal in example to current user's keytab and
   * username
   */
  private def authentication(): Unit = { // security mode
    if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
      System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF)
      LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf)
    }
    LOG.info(this.getClass + ": Login kerberos complete")
  }
}

class HdfsLogin
