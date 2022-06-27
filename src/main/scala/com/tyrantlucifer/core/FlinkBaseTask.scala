package com.tyrantlucifer.core

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author chao.tian
 * @date 2022/6/14
 */
abstract class FlinkBaseTask(args: Array[String]) extends Logging {

  // 初始化全局配置变量
  implicit var parameters: ParameterTool = _

  // 根据命令行参数 --config-path 解析配置
  parseConfig(args)

  // 初始化任务执行环境
  implicit val env: StreamExecutionEnvironment = initEnv

  /**
   * 解析参数
 *
   * @param args 命令行参数
   */
  private def parseConfig(args: Array[String]): Unit = {
    val commandConfigs = ParameterTool.fromArgs(args)
    val configPath = commandConfigs.get("config-path")
    val parameters = ParameterTool.fromPropertiesFile(configPath)
    this.parameters = parameters
  }

  /**
   * 初始化执行环境
   * @return env
   */
  private def initEnv: StreamExecutionEnvironment = {
    // 实例化流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 每 120s 做一次 checkpoint
    env.enableCheckpointing(parameters.getLong("flink.checkpoint.period"))
    // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 两次 checkpoint 的间隔时间至少为 1 s，默认是 0，立即进行下一次 checkpoint
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(parameters.getLong("flink.checkpoint.pause"));
    // checkpoint 必须在 120s 内结束，否则被丢弃，默认是 10 分钟
    env.getCheckpointConfig.setCheckpointTimeout(parameters.getLong("flink.checkpoint.timeout"))
    // 同一时间只能允许有一个 checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(parameters.getInt("flink.checkpoint.max"))
    // 最多允许 checkpoint 失败 3 次
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(parameters.getInt("flink.checkpoint.failure"))
    // 设置checkpoint的清除策略,当 Flink 任务取消时，保留外部保存的 checkpoint 信息
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置执行环境全局参数
    env.getConfig.setGlobalJobParameters(parameters)
    // 设置checkpoint保存地址
    if (StringUtils.isNotBlank(parameters.get("flink.checkpoint.path"))) {
      env.setStateBackend(new HashMapStateBackend())
      env.getCheckpointConfig.setCheckpointStorage(parameters.get("flink.checkpoint.path"))
    }
    // 返回env
    env
  }

  /**
   * 执行方法，具体业务逻辑实现在这里
   * @param env 执行环境
   * @return job执行结果
   */
  def execute(implicit env: StreamExecutionEnvironment): JobExecutionResult

  /**
   * 开启flink作业入口
   * @return
   */
  def start(): JobExecutionResult = {
    execute
  }
}