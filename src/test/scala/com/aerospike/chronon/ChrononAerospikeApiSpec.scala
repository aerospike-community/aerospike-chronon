package com.aerospike.chronon

import ai.chronon.online.{ExternalSourceHandler, ExternalSourceRegistry, GroupByServingInfoParsed, LoggableResponse, Mutation, StreamDecoder}
import ai.chronon.online.Fetcher.{Request, Response}
import ai.chronon.api.{ExternalPart, ExternalSource, ExternalSourceFactoryConfig, MetaData, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import scala.concurrent.Future
import scala.util.Success

class RecordingConsumer extends Consumer[LoggableResponse] {
  RecordingConsumer.instantiated.incrementAndGet()
  override def accept(t: LoggableResponse): Unit = RecordingConsumer.seen.add(t)
}
object RecordingConsumer {
  val instantiated = new AtomicInteger(0)
  val seen = new ConcurrentLinkedQueue[LoggableResponse]()
}

class ThrowingConsumer extends Consumer[LoggableResponse] {
  throw new RuntimeException("ctor boom")
  override def accept(t: LoggableResponse): Unit = ()
}

class FlakyConsumer extends Consumer[LoggableResponse] {
  if (FlakyConsumer.failInstantiations.get()) throw new RuntimeException("post-roundtrip boom")
  FlakyConsumer.instantiated.incrementAndGet()
  override def accept(t: LoggableResponse): Unit = ()
}
object FlakyConsumer {
  val instantiated = new AtomicInteger(0)
  val failInstantiations = new java.util.concurrent.atomic.AtomicBoolean(false)
}

class NoNoArgCtorConsumer(s: String) extends Consumer[LoggableResponse] {
  override def accept(t: LoggableResponse): Unit = ()
}

class UnitTestDecoder(gbsip: GroupByServingInfoParsed) extends StreamDecoder {
  override def decode(bytes: Array[Byte]): Mutation = Mutation()
  override def schema: StructType = null
}
class WrongCtorDecoder() extends StreamDecoder {
  override def decode(bytes: Array[Byte]): Mutation = Mutation()
  override def schema: StructType = null
}
class NotADecoder

class EchoHandler extends ExternalSourceHandler {
  override def fetch(requests: scala.collection.Seq[Request]): Future[scala.collection.Seq[Response]] =
    Future.successful(requests.map(r => Response(r, Success(r.keys))))
}
class EchoFactory extends ai.chronon.online.ExternalSourceFactory {
  override def createExternalSourceHandler(externalSource: ExternalSource): ExternalSourceHandler = new EchoHandler
}

class ChrononAerospikeApiSpec extends AnyFlatSpec with Matchers {

  "ChrononAerospikeApi" should "expose exactly one public constructor taking Map[String, String]" in {
    val ctors = classOf[ChrononAerospikeApi].getConstructors
    ctors should have length 1
    ctors.head.getParameterTypes.toList shouldBe List(classOf[scala.collection.immutable.Map[_, _]])
  }

  private def conf(extra: (String, String)*): Map[String, String] =
    Map("aerospike.hosts" -> "localhost", "aerospike.namespace" -> "test") ++ extra

  private def roundTrip[T <: Serializable](o: T): T = {
    val bos = new ByteArrayOutputStream()
    new ObjectOutputStream(bos).writeObject(o)
    new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray)).readObject().asInstanceOf[T]
  }

  private def externalPart(name: String, factoryName: String): ExternalPart = {
    val src = new ExternalSource()
    val md = new MetaData(); md.setName(name)
    val fc = new ExternalSourceFactoryConfig(); fc.setFactoryName(factoryName)
    src.setMetadata(md); src.setFactoryConfig(fc)
    val part = new ExternalPart(); part.setSource(src)
    part
  }

  "externalRegistry" should "return the same instance on repeated calls" in {
    val api = new ChrononAerospikeApi(conf())
    api.externalRegistry should be theSameInstanceAs(api.externalRegistry)
  }

  it should "let a factory and its handler be added and seen by a later read" in {
    val api = new ChrononAerospikeApi(conf())
    val registry = api.externalRegistry
    registry.addFactory("echo", new EchoFactory)
    registry.addHandlersByFactory(Some(Seq(externalPart("my_source", "echo"))))
    api.externalRegistry.handlerMap.keySet should contain("my_source")
  }

  it should "leave one handler and raise no error when addHandlersByFactory runs twice" in {
    val api = new ChrononAerospikeApi(conf())
    val registry = api.externalRegistry
    registry.addFactory("echo", new EchoFactory)
    val parts = Some(Seq(externalPart("my_source", "echo")))
    registry.addHandlersByFactory(parts)
    noException should be thrownBy registry.addHandlersByFactory(parts)
    registry.handlerMap.keySet.count(_ == "my_source") shouldBe 1
  }

  it should "be empty but usable after a serialization round trip" in {
    val api = new ChrononAerospikeApi(conf())
    api.externalRegistry.addFactory("echo", new EchoFactory)
    val copy = roundTrip(api)
    copy.externalRegistry.handlerMap.keySet should not contain "my_source"
    copy.externalRegistry.addFactory("echo", new EchoFactory)
    copy.externalRegistry.addHandlersByFactory(Some(Seq(externalPart("late_source", "echo"))))
    copy.externalRegistry.handlerMap.keySet should contain("late_source")
  }

  "log consumer" should "fail construction naming the key and class when the class is missing" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> "com.nope.Missing"))
    }
    ex.getMessage should include("chronon.log.consumer.class")
    ex.getMessage should include("com.nope.Missing")
  }

  it should "fail construction without instantiating a class of the wrong type" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> classOf[java.lang.String].getName))
    }
    ex.getMessage should include("chronon.log.consumer.class")
    ex.getMessage should include(classOf[java.lang.String].getName)
  }

  it should "fail construction with the reason when there is no public no-argument constructor" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> classOf[NoNoArgCtorConsumer].getName))
    }
    ex.getMessage should include("chronon.log.consumer.class")
    ex.getMessage should include("constructor")
  }

  it should "fail construction with the cause attached when the constructor throws" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> classOf[ThrowingConsumer].getName))
    }
    ex.getMessage should include("chronon.log.consumer.class")
    ex.getCause should not be null
  }

  it should "instantiate a valid consumer once and forward every logResponse" in {
    RecordingConsumer.seen.clear()
    RecordingConsumer.instantiated.set(0)
    val api = new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> classOf[RecordingConsumer].getName))
    RecordingConsumer.instantiated.get() shouldBe 1
    val resp = LoggableResponse(Array(1), Array(2), "j", 1L, "h")
    api.logResponse(resp)
    api.logResponse(resp)
    RecordingConsumer.seen should have size 2
    RecordingConsumer.instantiated.get() shouldBe 1
  }

  it should "make logResponse a no-op with no consumer configured" in {
    val api = new ChrononAerospikeApi(conf())
    noException should be thrownBy api.logResponse(LoggableResponse(Array(), Array(), "j", 0L, "h"))
  }

  it should "rethrow a cached resolution failure after serialization without repeating the lookup" in {
    FlakyConsumer.instantiated.set(0)
    FlakyConsumer.failInstantiations.set(false)
    val api = new ChrononAerospikeApi(conf("chronon.log.consumer.class" -> classOf[FlakyConsumer].getName))
    val copy = roundTrip(api)
    FlakyConsumer.failInstantiations.set(true)
    intercept[Exception](copy.logResponse(LoggableResponse(Array(), Array(), "j", 0L, "h")))
    intercept[Exception](copy.logResponse(LoggableResponse(Array(), Array(), "j", 0L, "h")))
    FlakyConsumer.instantiated.get() shouldBe 1 // one resolution attempt total, rethrown both times
    FlakyConsumer.failInstantiations.set(false)
  }

  "stream decoder" should "fail construction naming the key when the class is missing" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.stream.decoder.class" -> "com.nope.Decoder"))
    }
    ex.getMessage should include("chronon.stream.decoder.class")
    ex.getMessage should include("com.nope.Decoder")
  }

  it should "fail construction when the class is not a StreamDecoder" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.stream.decoder.class" -> classOf[NotADecoder].getName))
    }
    ex.getMessage should include("chronon.stream.decoder.class")
  }

  it should "fail construction when the GroupByServingInfoParsed constructor is missing" in {
    val ex = intercept[IllegalArgumentException] {
      new ChrononAerospikeApi(conf("chronon.stream.decoder.class" -> classOf[WrongCtorDecoder].getName))
    }
    ex.getMessage should include("chronon.stream.decoder.class")
    ex.getMessage should include("constructor")
  }

  it should "validate at construction but instantiate per call" in {
    val api = new ChrononAerospikeApi(conf("chronon.stream.decoder.class" -> classOf[UnitTestDecoder].getName))
    api.streamDecoder(null) shouldBe a[UnitTestDecoder]
    api.streamDecoder(null) should not be theSameInstanceAs(api.streamDecoder(null))
  }
}
