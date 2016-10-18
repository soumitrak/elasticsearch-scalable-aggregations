package sk.elasticsearch.plugin

import org.elasticsearch.plugins.AbstractPlugin
import org.elasticsearch.search.aggregations.AggregationModule
import sk.elasticsearch.aggregations.{HLLParser, InternalHLL}

class ScalableAggregations extends AbstractPlugin {
  override def description(): String = "Scalable Aggregations"

  override def name(): String = "scalableaggregations"

  def onModule(aggModule: AggregationModule) {
    aggModule.addAggregatorParser(classOf[HLLParser])
    InternalHLL.registerStreams
  }
}
