package com.aerospike.chronon

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.sys.process.Process
import scala.util.Try

/** Brings the docker-compose Aerospike rig up once per suite and exposes the
  * shared `userConf` the store under test is built from. */
trait AerospikeDockerSpec extends BeforeAndAfterAll { this: Suite =>

  val aerospikeHost: String = sys.env.getOrElse("AEROSPIKE_HOST", "localhost")
  val aerospikePort: Int    = sys.env.get("AEROSPIKE_PORT").map(_.toInt).getOrElse(3000)
  val aerospikeNamespace    = "test"

  def testUserConf: Map[String, String] = Map(
    AerospikeConfig.HostsKey                    -> aerospikeHost,
    AerospikeConfig.PortKey                     -> aerospikePort.toString,
    AerospikeConfig.NamespaceKey                -> aerospikeNamespace,
    AerospikeConfig.TtlSecondsKey               -> "86400",
    AerospikeConfig.StreamingMaxLookbackDaysKey -> "1"
  )

  def newStore(conf: Map[String, String] = testUserConf): AerospikeKvStore =
    AerospikeKvStore(conf)

  /** Suites that manage their own containers (e.g. the TLS fixture specs)
    * override to false — the plaintext CE rig is not brought up for them. */
  protected def needsBaseRig: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (needsBaseRig) {
      val up = Process("docker compose up -d --wait", new java.io.File(".").getAbsoluteFile)
      require(up.! == 0, "docker compose up -d --wait failed; is the rig defined in docker-compose.yml?")
    }
    // Suites may share a forked JVM; drop any cached client from a previous container.
    AerospikeClientHolder.clear()
  }

  override def afterAll(): Unit = {
    // stop + rm, not `down`: the rig's network stays up so its deletion cannot
    // churn other stopped containers' network state (chronon-benchmark-aerospike).
    if (needsBaseRig) {
      val compose = new java.io.File(".").getAbsoluteFile
      Try(Process("docker compose stop --timeout 1", compose).!)
      Try(Process("docker compose rm -f", compose).!)
    }
    super.afterAll()
  }

  val nsup0Port: Int           = 3100
  val nsup0Container: String   = "aerospike-chronon-test-nosup"

  val tlsPort: Int                 = 4533
  val mtlsPort: Int                = 4534
  val tlsServerName: String        = "aerospike-tls"
  val tlsContainers: Seq[String]   = Seq("aerospike-chronon-test-tls", "aerospike-chronon-test-tls-mtls")
  val tlsCertsDir: String          = "docker/tls-certs"
  val tlsUser: String              = "chronon-user"
  val tlsPassword: String          = "chronon-pass"
  /** The client cert CN, provisioned as a `nopassword` PKI user. */
  val tlsPkiUser: String           = "chronon-client"

  /** `gen-tls-certs.sh` writes `.complete` last, so an interrupted run (which
    * can leave some stores behind) is regenerated rather than trusted. */
  private def certsReady: Boolean =
    new java.io.File(tlsCertsDir, ".complete").isFile

  /** `asadm`/`asinfo` over TLS inside a fixture container: seed 4333, CA + the
    * (mTLS-required-safe) client cert, admin/admin bootstrap credentials. */
  private def asadmTls(container: String, cmd: String): Seq[String] =
    Seq("docker", "exec", container, "asadm", "--enable", "-e", cmd,
      "-h", s"127.0.0.1:$tlsServerName:4333", "--tls-enable",
      "--tls-cafile", "/opt/aerospike/certs/ca.crt",
      "--tls-certfile", "/opt/aerospike/certs/client.crt",
      "--tls-keyfile", "/opt/aerospike/certs/client.key",
      "-Uadmin", "-Padmin")

  /** Runs one `asadm` provisioning command, failing loudly with asadm's own
    * output — a swallowed failure would otherwise surface as an opaque
    * connect/auth error in an unrelated test. "already exists" is tolerated
    * so a reused container from a killed run doesn't break reruns. */
  private def runAsadm(container: String, cmd: String): Unit = {
    val out = new StringBuilder
    val rc = Process(asadmTls(container, cmd)).!(scala.sys.process.ProcessLogger(
      l => out.append(l).append('\n'), l => out.append(l).append('\n')))
    if (rc != 0 && !out.toString.toLowerCase.contains("already exists")) {
      throw new IllegalStateException(
        s"asadm provisioning failed on $container (exit $rc): '$cmd'\n$out")
    }
  }

  /** Brings up the profile-gated EE TLS fixtures (`tls` on [[tlsPort]], mTLS
    * required on [[mtlsPort]]) for one body and removes them afterwards.
    * Generates throwaway certs + Java stores when absent, and (re)provisions
    * the password and PKI users on every run — containers are ephemeral.
    * Cancels when Docker or the EE image is unavailable, like a missing rig. */
  def withTlsServers[T](body: => T): T = {
    val compose = new java.io.File(".").getAbsoluteFile
    if (!certsReady) {
      val rc = Try(Process("./docker/gen-tls-certs.sh", compose).!).getOrElse(1)
      if (rc != 0 || !certsReady) {
        throw new org.scalatest.exceptions.TestCanceledException("docker/gen-tls-certs.sh failed", 0)
      }
    }
    val up = Try(Process(
      Seq("docker", "compose", "--profile", "tls", "up", "-d", "--wait",
        "aerospike-tls", "aerospike-tls-mtls"), compose).!)
    up.foreach { rc =>
      if (rc != 0) throw new org.scalatest.exceptions.TestCanceledException("docker compose tls up failed", 0)
    }
    if (up.isFailure) throw new org.scalatest.exceptions.TestCanceledException(s"docker unavailable: ${up.failed.get.getMessage}", 0)
    try {
      tlsContainers.foreach { c =>
        Seq(
          s"manage acl create user $tlsUser password $tlsPassword roles read-write",
          s"manage acl create user $tlsPkiUser password nopassword roles read-write"
        ).foreach { cmd => runAsadm(c, cmd) }
      }
      body
    } finally {
      tlsContainers.foreach(c => Try(Process(s"docker rm -f $c").!))
      AerospikeClientHolder.clear()
    }
  }

  /** Runs `body` against the profile-gated `nsup0` server (NSUP_PERIOD=0, port
    * [[nsup0Port]]), removing the container afterwards. Cancels the test when
    * Docker or the service is unavailable, the same way a missing rig cancels
    * the other ITs. */
  def withNsup0Server[T](body: => T): T = {
    val compose = new java.io.File(".").getAbsoluteFile
    val up = Try(Process(Seq("docker", "compose", "--profile", "nsup0", "up", "-d", "--wait", "aerospike-nosup"), compose).!)
    up.foreach { rc =>
      if (rc != 0) throw new org.scalatest.exceptions.TestCanceledException("docker compose nsup0 up failed", 0)
    }
    if (up.isFailure) throw new org.scalatest.exceptions.TestCanceledException(s"docker unavailable: ${up.failed.get.getMessage}", 0)
    try body
    finally {
      Try(Process(s"docker rm -f $nsup0Container").!)
      AerospikeClientHolder.clear()
    }
  }
}
