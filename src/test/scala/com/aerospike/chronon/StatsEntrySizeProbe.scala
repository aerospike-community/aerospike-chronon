package com.aerospike.chronon

import ai.chronon.{api => Api}
import ai.chronon.aggregator.row.{RowAggregator, StatsGenerator}
import ai.chronon.online.serde.{ArrayRow, AvroConversions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.yahoo.sketches.kll.KllFloatsSketch

import scala.util.Random

/** Opt-in measurement probe (runs only with `STATS_SIZE_PROBE=1`): the encoded
  * size of one stats entry exactly as `SummaryJob`/`TimedKvRdd.valueToBytes`
  * produces it — `StatsGenerator.buildMetrics` -> `buildAggregator` ->
  * `init`/`updateWithReturn`/`normalize` -> `AvroConversions.encodeBytes` with
  * the Chronon value schema. No Spark, no Aerospike. */
class StatsEntrySizeProbe extends AnyFlatSpec with Matchers {

  private val probeEnabled = sys.env.contains("STATS_SIZE_PROBE")

  private case class Shape(name: String, columns: Seq[(String, Api.DataType)]) {
    val numericCount: Int = columns.count { case (_, t) => Api.DataType.isNumeric(t) && t != Api.ByteType }
  }

  // numeric = Double/Long/Int/Float mix; the rest StringType
  private def shape(name: String, total: Int, numeric: Int): Shape = {
    val numTypes = Seq(Api.DoubleType, Api.LongType, Api.IntType, Api.FloatType)
    val cols = (0 until total).map { i =>
      val t = if (i < numeric) numTypes(i % numTypes.size) else Api.StringType
      s"col_$i" -> t
    }
    Shape(name, cols)
  }

  private val shapes = Seq(
    shape("narrow", 10, 7),
    shape("typical", 50, 35),
    shape("wide", 200, 150),
    shape("xwide", 500, 350)
  )

  private def inputType(colType: Api.DataType, expression: StatsGenerator.InputTransform.Value): Api.DataType =
    expression match {
      case StatsGenerator.InputTransform.IsNull => Api.BooleanType
      case StatsGenerator.InputTransform.Raw    => colType
      case StatsGenerator.InputTransform.One    => Api.BooleanType
    }

  private def valueFor(colType: Api.DataType, rng: Random): Any = colType match {
    case Api.DoubleType => rng.nextGaussian()                        // normal(0,1)
    case Api.FloatType  => rng.nextGaussian().toFloat                // normal(0,1)
    case Api.IntType    => Int.box(rng.nextInt(10))                  // uniform 0..9
    case Api.LongType   => Long.box(math.round(math.exp(rng.nextGaussian() * 1.5) * 1000)) // lognormal
    case Api.StringType => null                                      // feeds the null-count metric only
    case _              => null
  }

  "stats entry size" should "measure encoded IR bytes per shape and row count" in {
    assume(probeEnabled, "set STATS_SIZE_PROBE=1 to run the stats entry size probe")

    val runs =
      for {
        s <- shapes
        n <- if (s.name == "narrow") Seq(100, 10000, 100000, 1000000) else Seq(100, 10000, 100000)
      } yield (s, n)

    val seed = 42L
    val colTypeByName = shapes.flatMap(_.columns).toMap

    val out = new java.io.PrintStream(System.out, true, "UTF-8")
    out.println("shape\tcolumns\tnumericCols\tmetrics\tN\tentryBytes\tKiB\tsketchBytes\tentries/1MiB\tentries/128KiB\tdayKiB(24x)\tsegKiB(128x)\tdays/1MiB")

    runs.foreach { case (s, n) =>
      val rng = new Random(seed)

      // 1. StatsGenerator.buildMetrics -> buildAggregator with the selectedDf schema
      val metrics = StatsGenerator.buildMetrics(s.columns)
      val selectedFields = metrics.map { m =>
        val colType = if (m.name == StatsGenerator.totalColumn) Api.BooleanType else colTypeByName(m.name)
        s"${m.name}${m.suffix}" -> inputType(colType, m.expression)
      }
      val selectedSchema = Api.StructType.from("Select", selectedFields.toArray)
      val aggregator: RowAggregator = StatsGenerator.buildAggregator(metrics, selectedSchema)

      // 2. N rows through init/updateWithReturn/normalize (dailySummary path)
      var ir = aggregator.init
      (0 until n).foreach { i =>
        val values: Array[Any] = metrics.map { m =>
          m.expression match {
            case StatsGenerator.InputTransform.IsNull => Boolean.box(false)
            case StatsGenerator.InputTransform.One    => Boolean.box(true)
            case StatsGenerator.InputTransform.Raw    => valueFor(colTypeByName(m.name), rng)
          }
        }.toArray
        ir = aggregator.updateWithReturn(ir, new ArrayRow(values, 0L))
      }
      val normalized = aggregator.normalize(ir)

      // 3. TimedKvRdd.valueToBytes: AvroConversions.encodeBytes(valueZSchema, handler)
      //    valueZSchema = SparkConversions.fromChrononSchema(aggregator.irSchema).toChrononSchema("Value")
      //    — the Spark round-trip yields the same StructType as building it from irSchema.
      //    The handler mirrors GenericRowHandler.func for an Array[Any] row.
      val valueZSchema = Api.StructType.from("Value", aggregator.irSchema)
      val handler: Any => Array[Any] = { case a: Array[Any] => a }
      val valueToBytes = AvroConversions.encodeBytes(valueZSchema, handler)
      val entryBytes = valueToBytes(normalized).length

      // 4. one serialized KLL sketch (k=128) from the normalized IR — the
      //    APPROX_PERCENTILE columns hold `sketch.toByteArray` after normalize
      val percentileIdx = aggregator.irSchema.toSeq.indexWhere(_._1.contains("approx"))
      val sketchBytes = normalized(percentileIdx).asInstanceOf[Array[Byte]].length

      val mib = 1024.0 * 1024.0
      out.println(
        f"${s.name}\t${s.columns.size}\t${s.numericCount}\t${metrics.size}\t$n\t$entryBytes\t${entryBytes / 1024.0}%.1f\t" +
          f"$sketchBytes\t${(mib / entryBytes).toInt}\t${(128 * 1024.0 / entryBytes).toInt}\t" +
          f"${24.0 * entryBytes / 1024.0}%.1f\t${128.0 * entryBytes / 1024.0}%.1f\t${mib / (24.0 * entryBytes)}%.1f")
    }

    out.flush()
    probeEnabled shouldBe true
  }
}
