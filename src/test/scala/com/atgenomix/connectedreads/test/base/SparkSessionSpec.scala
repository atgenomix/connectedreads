package com.atgenomix.connectedreads.test.base

import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.misc.Logging
import org.scalatest._

trait SparkSessionSpec extends BeforeAndAfterAll with Logging {
  this: Suite =>

  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName
  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .config("spark.network.timeout", "1800s") // Default timeout for all network interactions.
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }
}
