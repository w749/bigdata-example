package org.mlamp.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

private object HBaseConnection {
  def apply(hbaseSite: String): Connection = {
    val conf = HBaseConfiguration.create()
    conf.addResource(hbaseSite)
    ConnectionFactory.createConnection(conf)
  }
}
private class HBaseConnection
