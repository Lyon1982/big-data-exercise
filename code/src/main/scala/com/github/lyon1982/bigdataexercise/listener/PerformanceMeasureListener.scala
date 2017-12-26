package com.github.lyon1982.BigDataExercise.listener

import org.apache.log4j.LogManager
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}

class PerformanceMeasureListener extends SparkListener {

  lazy val logger = LogManager.getLogger("PerformanceMeasureListener")

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.warn(s"Stage #${stageCompleted.stageInfo.stageId} completed, " +
      s"Run Time: ${stageCompleted.stageInfo.taskMetrics.executorRunTime}, " +
      s"CPU Time: ${stageCompleted.stageInfo.taskMetrics.executorCpuTime}, " +
      s"JVM GC Time: ${stageCompleted.stageInfo.taskMetrics.jvmGCTime}, ")
  }

}
