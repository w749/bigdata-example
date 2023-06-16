package org.example.flink.sink

import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy
import org.apache.flink.util.Preconditions

/**
 * 使用checkpoint规则对文件输出大小、多就没数据关闭文件进行限制
 *   shouldRollOnCheckpoint无法设为false，否则调用org.apache.flink.streaming.api.functions.sink.filesystem.BulkPartWriter#persist()会报错
 *   三个方法其中一个为true就会写入新文件，所以checkpoint时间尽量长一点，避免还未满足其他两个条件就写入新文件从而产生大量小文件
 */
class RollingCheckpointRollingPolicy[IN, OUT] extends CheckpointRollingPolicy[IN, OUT] {
  private val serialVersionUID = 124L
  private var partSize = 1024L * 1024L * 128L
  private var rolloverInterval = 60L * 60L * 1000L
  private var inactivityInterval = 60L * 1000L

  def this(partSize: Long, rolloverInterval: Long, inactivityInterval: Long) {
    this()
    Preconditions.checkArgument(partSize > 0L)
    Preconditions.checkArgument(rolloverInterval > 0L)
    Preconditions.checkArgument(inactivityInterval > 0L)
    this.partSize = partSize
    this.rolloverInterval = rolloverInterval
    this.inactivityInterval = inactivityInterval
  }

  override def shouldRollOnEvent(partFileState: PartFileInfo[OUT], element: IN): Boolean = partFileState.getSize > partSize

  override def shouldRollOnProcessingTime(partFileState: PartFileInfo[OUT], currentTime: Long): Boolean = {
    (currentTime - partFileState.getCreationTime >= rolloverInterval) || (currentTime - partFileState.getLastUpdateTime >= inactivityInterval)
  }
}
