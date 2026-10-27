package com.aerospike.chronon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SanitySpec extends AnyFlatSpec with Matchers {
  "Default AerospikeConfig" should "be self-consistent (max lookback <= TTL)" in {
    val conf = AerospikeConfig.fromUserConf(Map(
      AerospikeConfig.HostsKey     -> "localhost",
      AerospikeConfig.NamespaceKey -> "test"
    ))
    conf.batchTtl shouldBe TtlSetting.Seconds(AerospikeConfig.DefaultTtlSeconds.toInt)
    conf.streamingMaxLookbackDays.toLong * 86400L should be <= AerospikeConfig.DefaultTtlSeconds
  }

  "Aerospike Java client" should "be present on the classpath" in {
    val clientClass = Class.forName("com.aerospike.client.AerospikeClient")
    clientClass should not be null
  }
}
