package com.atgenomix.connectedreads.test.base

import org.apache.spark._
import org.bdgenomics.utils.misc.Logging
import org.scalatest._

trait SparkContextSpec extends BeforeAndAfterAll with Logging {
  this: Suite =>

  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.ui.enabled", "false")

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }
}
