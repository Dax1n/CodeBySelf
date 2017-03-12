

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy, PreferFixed}


/**
  *  :: Experimental ::
  * Choice of how to schedule consumers for a given TopicPartition on an executor.
  * See [[LocationStrategies]] to obtain instances.
  * Kafka 0.10 consumers prefetch messages, so it's important for performance
  * to keep cached consumers on appropriate executors, not recreate them for every partition.
  * Choice of location is only a preference, not an absolute; partitions may be scheduled elsewhere.
  * 实验性API:
  * 在executor上consumer如何调度给定的TopicPartition，使用LocationStrategies获取调度策略实例
  * Kafka 0.10的消费者可以预取消息，因此对于性能来说在适合的executors上缓存consumers是比较重要的，而不是对每一个分区
  * 进行重新创建。对于分区位置的选择只是一个偏好，并非是绝对的。分区可能被调度到其他位置
  *
  *
  */
@Experimental
sealed abstract class LocationStrategy

/**
  * 使用PreferBrokers策略，必须是你的executors和kafka brokers在相同节点上。
  */
private case object PreferBrokers extends LocationStrategy

/**
  * 大多数情况下使用PreferConsistent需要一贯的将kafka的分区分布到所有的executors上
  */
private case object PreferConsistent extends LocationStrategy

/**
  * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
  * Any TopicPartition not specified in the map will use a consistent location.
  * 默认情况如果分区加载的不均衡的话，可以使用这个策略：放置特定的分区到特定的主机上
  * 任何TopicPartition没有和hosts映射的TopicPartition将会使用consistent location（就是安置到所有executor）
  *
  * 参数Map：就是TopicPartition和主机地址的映射
  */
private case class PreferFixed(hostMap: ju.Map[TopicPartition, String]) extends LocationStrategy

/**
  * :: Experimental :: object to obtain instances of [[LocationStrategy]]
  *
  */
@Experimental
object LocationStrategies {
  /**
    *  :: Experimental ::
    * Use this only if your executors are on the same nodes as your Kafka brokers.
    */
  @Experimental
  def PreferBrokers: LocationStrategy =
  org.apache.spark.streaming.kafka010.PreferBrokers

  /**
    *  :: Experimental ::
    * Use this in most cases, it will consistently distribute partitions across all executors.
    */
  @Experimental
  def PreferConsistent: LocationStrategy =
  org.apache.spark.streaming.kafka010.PreferConsistent

  /**
    *  :: Experimental ::
    * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
    * Any TopicPartition not specified in the map will use a consistent location.
    * 两个方法就是Map类型不一样而已，一个是Scala Map 另一个是Java Map
    *
    */
  @Experimental
  def PreferFixed(hostMap: collection.Map[TopicPartition, String]): LocationStrategy =
  new PreferFixed(new ju.HashMap[TopicPartition, String](hostMap.asJava))

  /**
    *  :: Experimental ::
    * Use this to place particular TopicPartitions on particular hosts if your load is uneven.
    * Any TopicPartition not specified in the map will use a consistent location.
    */
  @Experimental
  def PreferFixed(hostMap: ju.Map[TopicPartition, String]): LocationStrategy =
  new PreferFixed(hostMap)
}

