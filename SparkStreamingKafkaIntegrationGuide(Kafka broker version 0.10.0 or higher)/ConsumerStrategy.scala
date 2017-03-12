package org.apache.spark.streaming.kafka010

import java.{ lang => jl, util => ju }

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging

/**
  * :: Experimental ::
  * Choice of how to create and configure underlying Kafka Consumers on driver and executors.
  * See [[ConsumerStrategies]] to obtain instances.
  * Kafka 0.10 consumers can require additional, sometimes complex, setup after object
  *  instantiation. This interface encapsulates that process, and allows it to be checkpointed.
  * @tparam K type of Kafka message key
  * @tparam V type of Kafka message value
  *
  * 选择如何创建和配置在Driver和Executor上的Kafka Consumer实例，使用ConsumerStrategies获取策略实例
  * Kafka 0.10 consumers 在对象被实例化之后需要额外的配置，有时候比较复杂，这个接口封装了那些过程
  * 并允许实例被checkpointed
  *
  */
@Experimental
abstract class ConsumerStrategy[K, V] {
  /**
    * Kafka <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on executors. Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    *配置参数会在executors上被使用，需要设置bootstrap.servers指定Kafka的broker的主机和端口
    *
    */
  def executorKafkaParams: ju.Map[String, Object]

  /**
    * Must return a fully configured Kafka Consumer, including subscribed or assigned topics.
    * See <a href="http://kafka.apache.org/documentation.html#newconsumerapi">Kafka docs</a>.
    * This consumer will be used on the driver to query for offsets only, not messages.
    * The consumer must be returned in a state that it is safe to call poll(0) on.
    * @param currentOffsets A map from TopicPartition to offset, indicating how far the driver
    * has successfully read.  Will be empty on initial start, possibly non-empty on restart from
    * checkpoint.
    *返回一个必须是fully配置的Kafka Consumer，消费策略可以是subscribed或者assigned
    * consumer将会在driver上使用查询偏移而不是查询消息
    * 返回的consumer状态必须是安全的调用poll(0)方法
    * currentOffsets参数是一个TopicPartition到offset的映射map，可以标示：
    * driver已经成功读取多少信息，第一次启动时候可能是empty，在checkpoint中restart不为空
    *
    */
  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V]
}

/**
  * Subscribe to a collection of topics.
  * @param topics collection of topics to subscribe
  * @param kafkaParams Kafka
  * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  * configuration parameters</a> to be used on driver. The same params will be used on executors,
  * with minor automatic modifications applied.
  *  Requires "bootstrap.servers" to be set
  * with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
  * TopicPartition, the committed offset (if applicable) or kafka param
  * auto.offset.reset will be used.
  *
  * 订阅一组topic消息
  *kafkaParams参数：
  *配置参数将会在driver上使用，并且相同参数也会发送到executor上使用，在executor上可能略微的配置修改
  *通知需要设置bootstrap.servers指定broker
  *
  *offsets参数：
  * offsets伴随着startup开始,如果对于TopicPartition没有指定offset，
  * 提交的offset或auto.offset.reset将会被使用
  *
  *
  */
private case class Subscribe[K, V](
                                    topics: ju.Collection[jl.String],
                                    kafkaParams: ju.Map[String, Object],
                                    offsets: ju.Map[TopicPartition, jl.Long]
                                  ) extends ConsumerStrategy[K, V] with Logging {

  /**
    *继承至ConsumerStrategy的方法，获取配置参数
    *
    * @return
    */
  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  /**
    *
    *  继承至ConsumerStrategy的方法,用于创建Consumer
    * @return
    */
  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.subscribe(topics)
    val toSeek = if (currentOffsets.isEmpty) {
      offsets
    } else {
      currentOffsets
    }
    if (!toSeek.isEmpty) {
      // work around KAFKA-3370 when reset is none
      // poll will throw if no position, i.e. auto offset reset none and no explicit position
      // but cant seek to a position before poll, because poll is what gets subscription partitions
      // So, poll, suppress the first exception, then seek
      val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      val shouldSuppress = aor != null && aor.asInstanceOf[String].toUpperCase == "NONE"
      try {
        consumer.poll(0)
      } catch {
        case x: NoOffsetForPartitionException if shouldSuppress =>
          logWarning("Catching NoOffsetForPartitionException since " +
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " is none.  See KAFKA-3370")
      }
      toSeek.asScala.foreach { case (topicPartition, offset) =>
        consumer.seek(topicPartition, offset)
      }
      // we've called poll, we must pause or next poll may consume messages and set position
      consumer.pause(consumer.assignment())
    }

    consumer
  }
}

/**
  * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
  * The pattern matching will be done periodically against topics existing at the time of check.
  * @param pattern pattern to subscribe to
  * @param kafkaParams Kafka
  * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  * configuration parameters</a> to be used on driver. The same params will be used on executors,
  * with minor automatic modifications applied.
  *  Requires "bootstrap.servers" to be set
  * with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
  * TopicPartition, the committed offset (if applicable) or kafka param
  * auto.offset.reset will be used.
  *
  *  SubscribePattern是订阅所有匹配正则表达式模式的topic的数据
  *  topic模式匹配将会周期性的进行匹配，而不是在检查已存在的topic时候进行模式匹配
  *
  *  kafkaParams参数：
  *配置参数将会在driver上使用，并且相同参数也会发送到executor上使用，在executor上可能略微的配置修改
  *通知需要设置bootstrap.servers指定broker
  *
  *offsets参数：
  * offsets伴随着startup开始,如果对于TopicPartition没有指定offset，
  * 提交的offset或auto.offset.reset将会被使用
  *
  *
  */
private case class SubscribePattern[K, V](
                                           pattern: ju.regex.Pattern,
                                           kafkaParams: ju.Map[String, Object],
                                           offsets: ju.Map[TopicPartition, jl.Long]
                                         ) extends ConsumerStrategy[K, V] with Logging {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.subscribe(pattern, new NoOpConsumerRebalanceListener())
    val toSeek = if (currentOffsets.isEmpty) {
      offsets
    } else {
      currentOffsets
    }
    if (!toSeek.isEmpty) {
      // work around KAFKA-3370 when reset is none, see explanation in Subscribe above
      val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
      val shouldSuppress = aor != null && aor.asInstanceOf[String].toUpperCase == "NONE"
      try {
        consumer.poll(0)
      } catch {
        case x: NoOffsetForPartitionException if shouldSuppress =>
          logWarning("Catching NoOffsetForPartitionException since " +
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + " is none.  See KAFKA-3370")
      }
      toSeek.asScala.foreach { case (topicPartition, offset) =>
        consumer.seek(topicPartition, offset)
      }
      // we've called poll, we must pause or next poll may consume messages and set position
      consumer.pause(consumer.assignment())
    }

    consumer
  }
}

/**
  * Assign a fixed collection of TopicPartitions
  * @param topicPartitions collection of TopicPartitions to assign
  * @param kafkaParams Kafka
  * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
  * configuration parameters</a> to be used on driver. The same params will be used on executors,
  * with minor automatic modifications applied.
  *  Requires "bootstrap.servers" to be set
  * with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
  * TopicPartition, the committed offset (if applicable) or kafka param
  * auto.offset.reset will be used.
  *
  *  订阅一个固定数目的TopicPartitions
  * kafkaParams参数：
  *配置参数将会在driver上使用，并且相同参数也会发送到executor上使用，在executor上可能略微的配置修改
  *通知需要设置bootstrap.servers指定broker
  *
  *offsets参数：
  * offsets伴随着startup开始,如果对于TopicPartition没有指定offset，
  * 提交的offset或auto.offset.reset将会被使用
  *
  *
  *
  */
private case class Assign[K, V](
                                 topicPartitions: ju.Collection[TopicPartition],
                                 kafkaParams: ju.Map[String, Object],
                                 offsets: ju.Map[TopicPartition, jl.Long]
                               ) extends ConsumerStrategy[K, V] {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.assign(topicPartitions)
    val toSeek = if (currentOffsets.isEmpty) {
      offsets
    } else {
      currentOffsets
    }
    if (!toSeek.isEmpty) {
      // this doesn't need a KAFKA-3370 workaround, because partitions are known, no poll needed
      toSeek.asScala.foreach { case (topicPartition, offset) =>
        consumer.seek(topicPartition, offset)
      }
    }

    consumer
  }
}

/**
  * :: Experimental ::
  * object for obtaining instances of [[ConsumerStrategy]]
  *
  * 这个是订阅策略的工厂方法：用于实例化策略的
  *
  *
  *
  */
@Experimental
object ConsumerStrategies {
  /**
    *  :: Experimental ::
    * Subscribe to a collection of topics.
    * @param topics collection of topics to subscribe
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def Subscribe[K, V](
                       topics: Iterable[jl.String],
                       kafkaParams: collection.Map[String, Object],
                       offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](
      new ju.ArrayList(topics.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
  }

  /**
    *  :: Experimental ::
    * Subscribe to a collection of topics.
    * @param topics collection of topics to subscribe
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def Subscribe[K, V](
                       topics: Iterable[jl.String],
                       kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](
      new ju.ArrayList(topics.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
    *  :: Experimental ::
    * Subscribe to a collection of topics.
    * @param topics collection of topics to subscribe
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def Subscribe[K, V](
                       topics: ju.Collection[jl.String],
                       kafkaParams: ju.Map[String, Object],
                       offsets: ju.Map[TopicPartition, jl.Long]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](topics, kafkaParams, offsets)
  }

  /**
    *  :: Experimental ::
    * Subscribe to a collection of topics.
    * @param topics collection of topics to subscribe
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def Subscribe[K, V](
                       topics: ju.Collection[jl.String],
                       kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](topics, kafkaParams, ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /** :: Experimental ::
    * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    * @param pattern pattern to subscribe to
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def SubscribePattern[K, V](
                              pattern: ju.regex.Pattern,
                              kafkaParams: collection.Map[String, Object],
                              offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
    new SubscribePattern[K, V](
      pattern,
      new ju.HashMap[String, Object](kafkaParams.asJava),
      new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
  }

  /** :: Experimental ::
    * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    * @param pattern pattern to subscribe to
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def SubscribePattern[K, V](
                              pattern: ju.regex.Pattern,
                              kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V] = {
    new SubscribePattern[K, V](
      pattern,
      new ju.HashMap[String, Object](kafkaParams.asJava),
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /** :: Experimental ::
    * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    * @param pattern pattern to subscribe to
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def SubscribePattern[K, V](
                              pattern: ju.regex.Pattern,
                              kafkaParams: ju.Map[String, Object],
                              offsets: ju.Map[TopicPartition, jl.Long]): ConsumerStrategy[K, V] = {
    new SubscribePattern[K, V](pattern, kafkaParams, offsets)
  }

  /** :: Experimental ::
    * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
    * The pattern matching will be done periodically against topics existing at the time of check.
    * @param pattern pattern to subscribe to
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def SubscribePattern[K, V](
                              pattern: ju.regex.Pattern,
                              kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V] = {
    new SubscribePattern[K, V](
      pattern,
      kafkaParams,
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
    *  :: Experimental ::
    * Assign a fixed collection of TopicPartitions
    * @param topicPartitions collection of TopicPartitions to assign
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def Assign[K, V](
                    topicPartitions: Iterable[TopicPartition],
                    kafkaParams: collection.Map[String, Object],
                    offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      new ju.ArrayList(topicPartitions.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
  }

  /**
    *  :: Experimental ::
    * Assign a fixed collection of TopicPartitions
    * @param topicPartitions collection of TopicPartitions to assign
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def Assign[K, V](
                    topicPartitions: Iterable[TopicPartition],
                    kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      new ju.ArrayList(topicPartitions.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
    *  :: Experimental ::
    * Assign a fixed collection of TopicPartitions
    * @param topicPartitions collection of TopicPartitions to assign
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
    * TopicPartition, the committed offset (if applicable) or kafka param
    * auto.offset.reset will be used.
    */
  @Experimental
  def Assign[K, V](
                    topicPartitions: ju.Collection[TopicPartition],
                    kafkaParams: ju.Map[String, Object],
                    offsets: ju.Map[TopicPartition, jl.Long]): ConsumerStrategy[K, V] = {
    new Assign[K, V](topicPartitions, kafkaParams, offsets)
  }

  /**
    *  :: Experimental ::
    * Assign a fixed collection of TopicPartitions
    * @param topicPartitions collection of TopicPartitions to assign
    * @param kafkaParams Kafka
    * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
    * configuration parameters</a> to be used on driver. The same params will be used on executors,
    * with minor automatic modifications applied.
    *  Requires "bootstrap.servers" to be set
    * with Kafka broker(s) specified in host1:port1,host2:port2 form.
    */
  @Experimental
  def Assign[K, V](
                    topicPartitions: ju.Collection[TopicPartition],
                    kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      topicPartitions,
      kafkaParams,
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

}

