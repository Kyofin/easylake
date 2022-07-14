package com.data.easyflow.processor.base

import com.data.easyflow.model.TaskNode
import com.data.easyflow.processor.sink.CsvWrite
import com.data.easyflow.processor.source.{CsvRead, HiveRead, MysqlRead}
import com.data.easyflow.processor.transform.{AggregatorFun, FilterFun, JoinFun, MapFun, SampleFun, SchemaChangeFun, SortFun, UnionFun}
import com.data.easyflow.utils.Config
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._

class ProcessorTaskNode(jobContext: JobContext, processorConfig: Config) extends TaskNode {



  private var input: InputData = _
  /**
   * 节点处理完的df
   */
  private var completedDataFrame: DataFrame = _

  private var processor: Processor = _

  /**
   * 根据配置实例化processor
   */
  private def newInstanceProcessor(): Unit = {
    // 根据plugin实例化
    val plugin = processorConfig.getString("plugin")
    val parameter = processorConfig.getConfiguration("parameter")
    plugin match {
      case "CsvReader" => processor = new CsvRead(jobContext, parameter)
      case "HiveReader" => processor = new HiveRead(jobContext, parameter)
      case "MysqlReader" => processor = new MysqlRead(jobContext, parameter)
      case "CsvWriter" => processor = new CsvWrite(jobContext, parameter)
      case "Sample" => processor = new SampleFun(jobContext, parameter)
      case "Filter" => processor = new FilterFun(jobContext, parameter)
      case "Map" => processor = new MapFun(jobContext, parameter)
      case "Sort" => processor = new SortFun(jobContext, parameter)
      case "Aggregator" => processor = new AggregatorFun(jobContext, parameter)
      case "Join" => processor = new JoinFun(jobContext, parameter)
      case "Union" => processor = new UnionFun(jobContext, parameter)
      case "SchemaChange" => processor = new SchemaChangeFun(jobContext, parameter)
    }


  }

  private def internalProcess(isTryRun: Boolean): Unit = {
    // processor执行前检查参数
    processor.checkParameter()

    // 不同类型processor有不同的处理逻辑
    val pluginType = processorConfig.getString("pluginType")
    pluginType match {
      case "source" =>
        val sourceProcessor = processor.asInstanceOf[SourceProcessor]
        val outPutDF = sourceProcessor.createDataFrame(jobContext.spark)
        completedDataFrame = outPutDF
      case "sink" =>
        val sinkProcessor = processor.asInstanceOf[SinkProcessor]
        val inputDF = input.defaultInput.get
        // 如果是试运行则将输入的df保存起来，用于当前节点的预览结果
        if (isTryRun) {
          completedDataFrame = inputDF
        } else {
          sinkProcessor.save(inputDF)
        }
      case "transform" =>
        val transFormProcessor = processor.asInstanceOf[TransFormProcessor]
        var outPutDF = transFormProcessor.trans2DataFrame(input)
        // repartition
        outPutDF = transFormProcessor.repartitionDataFrame(outPutDF)
        // cache
        outPutDF = transFormProcessor.cacheDataFrame(outPutDF)
        // 声明当前任务节点的输出df
        completedDataFrame = outPutDF
    }
  }

  private def putCompletedDataFame2JobContext(): Unit = {
    jobContext.completedDataFramePool += (getName -> completedDataFrame)
  }

  private def findDependentDataFrames(): Unit = {
    // 根据当前节点的依赖前置任务，找到对应依赖的dataframe
    val dependentDFs = getDepList.map(name => {
      name -> jobContext.completedDataFramePool(name)
    })
    // 判断当前节点是单输入还是多输入还是无输入
    if (dependentDFs.isEmpty){
      input=new InputData(None,None)
    }else if (dependentDFs.size==1){
      input=new InputData(Some(dependentDFs.head._2),None)
    }else if (dependentDFs.size==2) {
      input=new InputData(None,Some(dependentDFs.toMap))
    }
  }


  override def process(): Unit = {
    // 实例化processor
    newInstanceProcessor()
    // 判断是否有依赖，找出依赖的df
    findDependentDataFrames()
    // 根据processor类型进行处理
    internalProcess(jobContext.isTryRun)
    // 存储处理完的df到上下文
    putCompletedDataFame2JobContext()
  }


}
