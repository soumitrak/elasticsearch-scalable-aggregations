package sk.elasticsearch.aggregations

import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid}
import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.util.BytesRef
import org.elasticsearch.common.io.stream.{StreamInput, StreamOutput}
import org.elasticsearch.common.xcontent.ToXContent.Params
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentParser}
import org.elasticsearch.index.fielddata.SortedBinaryDocValues
import org.elasticsearch.search.SearchParseException
import org.elasticsearch.search.aggregations.AggregationStreams.Stream
import org.elasticsearch.search.aggregations.InternalAggregation.{CommonFields, ReduceContext, Type}
import org.elasticsearch.search.aggregations.metrics.{InternalNumericMetricsAggregation, NumericMetricsAggregator}
import org.elasticsearch.search.aggregations.support._
import org.elasticsearch.search.aggregations.{AggregationStreams, Aggregator, AggregatorFactory, InternalAggregation}
import org.elasticsearch.search.internal.SearchContext

import scala.collection.JavaConversions._
import scala.collection.mutable

object InternalHLL {
  val TYPE: Type = new Type("hll")
  val error = 0.01

  def apply() = new InternalHLL()

  val STREAM = new Stream {
    override def readResult(in: StreamInput): InternalAggregation = {
      val tmp = InternalHLL()
      tmp.readFrom(in)
      tmp
    }
  }

  def registerStreams = AggregationStreams.registerStream(STREAM, TYPE.stream)

  def getMonoid = new HyperLogLogMonoid(HyperLogLog.bitsForError(error))
}

sealed case class InternalHLL(var hll: HLL, var nam: String) extends InternalNumericMetricsAggregation.SingleValue(nam) {

  def this() = this(null, null)

  override def `type`(): Type = InternalHLL.TYPE

  override def reduce(reduceContext: ReduceContext): InternalAggregation = {
    val hlls = reduceContext.aggregations().map(_.asInstanceOf[InternalHLL].hll)
    InternalHLL(InternalHLL.getMonoid.sum(hlls), name)
  }

  override def doXContentBody(builder: XContentBuilder, params: Params): XContentBuilder = {
    builder.field(CommonFields.VALUE, hll.estimatedSize)
    builder
  }

  override def writeTo(out: StreamOutput): Unit = {
    out.writeString(nam)
    out.writeByteArray(HyperLogLog.toBytes(hll))
  }

  override def readFrom(in: StreamInput): Unit = {
    nam = in.readString()
    name = nam
    hll = HyperLogLog.fromBytes(in.readByteArray())
  }

  override def value(): Double = hll.estimatedSize
}

class HLLAggregator(nam: String,
                    estimatedBucketsCount: Long,
                    aggregationContext: AggregationContext,
                    parent: Aggregator,
                    valuesSource: Option[ValuesSource])
  extends NumericMetricsAggregator.SingleValue(nam, estimatedBucketsCount, aggregationContext, parent) {

  private var values: SortedBinaryDocValues = _

  private val ord2hll = mutable.Map[Long, HLL]()

  override def shouldCollect(): Boolean = valuesSource.isDefined

  override def buildEmptyAggregation(): InternalAggregation = {
    InternalHLL(InternalHLL.getMonoid.zero, name)
  }

  override def buildAggregation(owningBucketOrdinal: Long): InternalAggregation = {
    ord2hll.get(owningBucketOrdinal) match {
      case Some(hll) => InternalHLL(hll, name)
      case _ => buildEmptyAggregation()
    }
  }

  private def toBytesArray(bytesRef: BytesRef) = {
    val array = Array.ofDim[Byte](bytesRef.length)
    System.arraycopy(bytesRef.bytes, bytesRef.offset, array, 0, bytesRef.length)
    array
  }

  override def collect(docId: Int, bucketOrdinal: Long): Unit = {
    values.setDocument(docId)
    val hllMonoid = InternalHLL.getMonoid
    var result = ord2hll.getOrElse(bucketOrdinal, hllMonoid.zero)
    for (i <- 0 until values.count()) {
      result = hllMonoid.plus(result, hllMonoid.create(toBytesArray(values.valueAt(i))))
    }

    ord2hll += (bucketOrdinal -> result)
  }

  override def setNextReader(reader: AtomicReaderContext): Unit = {
    values = valuesSource.get.bytesValues()
  }

  override def metric(owningBucketOrd: Long): Double = {
    valuesSource.isDefined match {
      case true => ord2hll.get(owningBucketOrd).get.estimatedSize
      case false => Double.NaN
    }
  }
}

class HLLAggregatorFactory(nam: String, config: ValuesSourceConfig[ValuesSource])
  extends ValuesSourceAggregatorFactory[ValuesSource](nam, InternalHLL.TYPE.name(), config) {

  override def createUnmapped(aggregationContext: AggregationContext,
                              parent: Aggregator): Aggregator = {
    new HLLAggregator(name, 0, aggregationContext, parent, None)
  }

  override def create(valuesSource: ValuesSource,
                      expectedBucketsCount: Long,
                      aggregationContext: AggregationContext,
                      parent: Aggregator): Aggregator = {
    new HLLAggregator(name, expectedBucketsCount, aggregationContext, parent, Some(valuesSource))
  }
}

class HLLParser extends Aggregator.Parser {

  override def `type`(): String = InternalHLL.TYPE.name()

  override def parse(aggregationName: String, parser: XContentParser, context: SearchContext): AggregatorFactory = {
    val vsParser = ValuesSourceParser.any(aggregationName, InternalHLL.TYPE, context)
      .formattable(false).build.asInstanceOf[ValuesSourceParser[ValuesSource]]

    var currentFieldName: String = null
    var done = false
    while (!done) {
      val token = parser.nextToken
      if (token != XContentParser.Token.END_OBJECT) {
        if (token eq XContentParser.Token.FIELD_NAME) {
          currentFieldName = parser.currentName
        } else if (vsParser.token(currentFieldName, token, parser)) {
        } else {
          throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].")
        }
      } else {
        done = true
      }
    }

    new HLLAggregatorFactory(aggregationName, vsParser.config())
  }
}
