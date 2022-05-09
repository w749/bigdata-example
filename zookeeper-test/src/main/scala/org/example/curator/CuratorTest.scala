package org.example.curator

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.data.{ACL, Id}
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.slf4j.LoggerFactory

import java.util
import java.util.Properties
import scala.collection.JavaConverters.asScalaBufferConverter

object CuratorTest {
  private lazy val LOG = LoggerFactory.getLogger(this.getClass)
  private val properties: Properties = Config("application.properties")
  def main(args: Array[String]): Unit = {
    val framework = getCuratorConnection()
    framework.start()
//    deletePrefixNode(framework, "/rmstore/ZKRMStateRoot/RMAppRoot", "application")
    setAcl(framework)
    framework.close()
  }

  /**
   * 获取 CuratorFramework 连接
   */
  def getCuratorConnection(): CuratorFramework = {
    CuratorFrameworkFactory.newClient(
      properties.getProperty("connectString"),
      Integer.parseInt(properties.getProperty("sessionTimeoutMs")),
      Integer.parseInt(properties.getProperty("connectionTimeoutMs")),
      new RetryNTimes(Integer.parseInt(properties.getProperty("retryCount")),
        Integer.parseInt(properties.getProperty("elapsedTimeMs")))
    )
  }

  /**
   * 创建永久节点、递归创建节点、创建临时序列节点等基本操作
   */
  def nodeTest(framework: CuratorFramework): Unit = {
    // 创建永久节点、递归创建节点、创建临时序列节点
    framework.create().forPath("/test15")
    framework.create().creatingParentsIfNeeded().forPath("/test11/sub1/sub3")
    framework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/test8")
    val child1 = framework.getChildren.forPath("/").asScala.toList
    println(child1)

    // 存入数据
    framework.setData().forPath("/test15", "abc".getBytes())

    // 获取数据
    val data = framework.getData.forPath("/test15")
    val acls = framework.getACL.forPath("/test15").asScala.toList
    println("/test15 data: "+ new String(data) + "\nacls: " +  acls)

    // 删除节点、递归删除节点
    framework.delete().forPath("/test10")
    framework.delete().deletingChildrenIfNeeded().forPath("/test11")
    val child2 = framework.getChildren.forPath("/").asScala.toList
    println(child2)
  }

  /**
   * 根据需要设置ACL
   */
  def setAcl(framework: CuratorFramework): Unit = {
    val aclsLst = new util.ArrayList[ACL]
    val user = new Id("world", "anyone")
    val user1 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin1:123"))
    val user2 = new Id("digest", DigestAuthenticationProvider.generateDigest("admin2:123"))
    aclsLst.add(new ACL(ZooDefs.Perms.READ, user))
    aclsLst.add(new ACL(ZooDefs.Perms.READ, user1))
    aclsLst.add(new ACL(ZooDefs.Perms.ADMIN, user2))
    aclsLst.add(new ACL(ZooDefs.Perms.WRITE, user2))
    aclsLst.add(new ACL(ZooDefs.Perms.DELETE, user2))

    framework.setACL().withACL(aclsLst).forPath("/test")
  }

  /**
   * 删除指定node下指定前缀的所有子node
   */
  def deletePrefixNode(framework: CuratorFramework, path: String, prefix: String): Unit = {
    val list = framework.getChildren.forPath(path).asScala.toList
    val nodeSize = list.size
    var delSize = 0
    for (node <- list) {
      if (node.startsWith(prefix)) {
        framework.delete().deletingChildrenIfNeeded().forPath(path + "/" + node)
        delSize += 1
        println("delete " + path + "/" + node)
//        LOG.info("delete " + path + "/" + node)
      }
    }
    println("total children: " + nodeSize + " and deleted: " + delSize)
//    LOG.info("total children: " + nodeSize + " and deleted: " + delSize)
  }
}
