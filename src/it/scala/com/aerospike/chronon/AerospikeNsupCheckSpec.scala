package com.aerospike.chronon

import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import com.aerospike.client.{Info, Key}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process.Process

/** Section 10 (comment 9): the nsup fail-fast against a `NSUP_PERIOD=0`
  * server (the `nsup0` compose service on port 3100, see docker-compose.yml).
  * Without `allow-ttl-without-nsup`, TTL-bearing writes would be rejected by
  * the server with code 22 (FAIL_FORBIDDEN) — the store now fails the client
  * build instead. */
class AerospikeNsupCheckSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private val timeout = 30.seconds
  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private def put(dataset: String, key: String, value: String, ts: Long): PutRequest =
    PutRequest(bytes(key), bytes(value), dataset, Some(ts))
  private def get(dataset: String, key: String): GetRequest = GetRequest(bytes(key), dataset, None)

  private def nsup0Conf(extra: Map[String, String] = Map.empty): Map[String, String] =
    testUserConf + (AerospikeConfig.PortKey -> nsup0Port.toString) ++ extra

  private def asinfo(cmd: String): Unit =
    Process(s"docker exec $nsup0Container asinfo -v $cmd").! shouldBe 0

  "client build" should "fail fast with a positive TTL against nsup-period=0" in {
    withNsup0Server {
      val ex = intercept[IllegalStateException](newStore(nsup0Conf()))
      ex.getMessage should include("nsup-period")
      ex.getMessage should include("allow-ttl-without-nsup")
    }
  }

  it should "build and round-trip with both TTLs 0 (never expire)" in {
    withNsup0Server {
      val ds = "nsup0_gb_BATCH"
      val store = newStore(nsup0Conf(Map(AerospikeConfig.TtlSecondsKey -> "0")))
      Await.result(store.multiPut(Seq(put(ds, "k1", "v1", 100L))), timeout).head shouldBe true
      val record = AerospikeClientHolder.clientFor(store.config).get(
        null,
        new Key(store.config.namespace,
                AerospikeDataModel.physicalSetName(ds),
                AerospikeDataModel.recordKey(ds, bytes("k1"), store.config.keyPrefix)))
      record.expiration should be <= 0 // never expire (0 or -1 depending on client version)
      Await.result(store.multiGet(Seq(get(ds, "k1"))), timeout).head.values.get
        .map(v => new String(v.bytes, UTF_8)) should contain("v1")
    }
  }

  it should "fail fast when a second store on the same client asks for a positive TTL" in {
    withNsup0Server {
      newStore(nsup0Conf(Map(AerospikeConfig.TtlSecondsKey -> "0")))
      val ex = intercept[IllegalStateException](
        newStore(nsup0Conf(Map(
          AerospikeConfig.TtlSecondsKey               -> "3600",
          AerospikeConfig.StreamingMaxLookbackDaysKey -> "0"))))
      ex.getMessage should include("nsup-period")
      ex.getMessage should include("allow-ttl-without-nsup")
    }
  }

  it should "leave no event loops or connections behind when the check fails" in {
    withNsup0Server {
      val probe = newStore(nsup0Conf(Map(AerospikeConfig.TtlSecondsKey -> "0")))
      def loopThreads: Int =
        Thread.getAllStackTraces.keySet.asScala.count(_.getName.startsWith("aerospike-nio"))
      def conns(): Int =
        Info.request(null, AerospikeClientHolder.clientFor(probe.config).getNodes.head, "statistics")
          .split(";")
          .collectFirst { case kv if kv.startsWith("client_connections=") => kv.stripPrefix("client_connections=").toInt }
          .getOrElse(-1)
      val loops0 = loopThreads
      // let the probe's own pool settle before taking the base reading —
      // the tend/info sockets churn for a second after a build
      var conns0 = -1
      var stable = 0
      val settleDeadline = System.nanoTime() + 10.seconds.toNanos
      while (stable < 5 && System.nanoTime() < settleDeadline) {
        Thread.sleep(200)
        val now = conns()
        if (now == conns0) stable += 1 else { conns0 = now; stable = 0 }
      }
      val ex = intercept[IllegalStateException](
        newStore(nsup0Conf(Map(
          AerospikeConfig.TtlSecondsKey               -> "3600",
          AerospikeConfig.StreamingMaxLookbackDaysKey -> "0",
          AerospikeConfig.ReadTotalTimeoutKey         -> "1401")))) // fresh cache key -> real build attempt
      ex.getMessage should include("nsup-period")
      val deadline = System.nanoTime() + 10.seconds.toNanos
      while ((loopThreads != loops0 || conns() != conns0) && System.nanoTime() < deadline) Thread.sleep(200)
      info(s"failed nsup build: loop threads ${loopThreads - loops0}, connection delta ${conns() - conns0}")
      loopThreads shouldBe loops0
      conns() shouldBe conns0
    }
  }

  it should "build with a positive TTL once allow-ttl-without-nsup is set" in {
    withNsup0Server {
      asinfo("set-config:context=namespace;id=test;allow-ttl-without-nsup=true")
      val ds = "nsup0_gb_STREAMING"
      val store = newStore(nsup0Conf(Map(
        AerospikeConfig.TtlSecondsKey             -> "3600",
        AerospikeConfig.StreamingMaxLookbackDaysKey -> "0")))
      Await.result(store.multiPut(Seq(put(ds, "k1", "v1", System.currentTimeMillis()))), timeout).head shouldBe true
    }
  }
}
