package com.data.easyflow

import java.io.File

import com.data.easyflow.common.TaskDependType
import com.data.easyflow.graph.DAG
import com.data.easyflow.model.{TaskNode, TaskNodeRelation}
import com.data.easyflow.process.ProcessDag
import com.data.easyflow.processor.base.{JobContext, ProcessorTaskNode}
import com.data.easyflow.processor.sink.CsvWrite
import com.data.easyflow.utils.{Config, DagHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class Engine(val jobContent: String, val isTryRunMode: Boolean, spark: SparkSession) {

  def exec(): String = {

    // 初始化job上下文
    val jobContext = new JobContext(spark)
    // 设置job是否试运行
    jobContext.isTryRun = isTryRunMode

    // 读取config文件初始化Config
    val jobConfig = Config.from(jobContent)
    import scala.collection.JavaConversions._

    var tryRunTaskNodeName: String = null
    //  初始化所有ProcessorTaskNode
    val taskNodeList: java.util.List[TaskNode] = {
      val processorConfigs = jobConfig.getListConfiguration("job.processor")
      processorConfigs.map(c => {
        val name = c.getString("name")
        // 提取含tryRun参数的TaskName
        val isTryRun = c.getBool("tryRun", false)
        if (isTryRun) {
          tryRunTaskNodeName = name
        }
        val preTasks = c.getList("preTasks", classOf[String])
        val taskNode = new ProcessorTaskNode(jobContext, c)
        taskNode.setName(name)
        taskNode.setDepList(preTasks)
        taskNode
      })
    }

    val startNodes: java.util.List[String] = new java.util.ArrayList[String]
    val recoveryNodes: java.util.List[String] = new java.util.ArrayList[String]
    val destTaskNodeList: java.util.List[TaskNode] = DagHelper.generateFlowNodeListByStartNode(taskNodeList, startNodes, recoveryNodes, TaskDependType.TASK_POST)
    val taskNodeRelations: java.util.List[TaskNodeRelation] = DagHelper.generateRelationListByFlowNodes(destTaskNodeList)

    val processDag: ProcessDag = new ProcessDag
    processDag.setEdges(taskNodeRelations)
    processDag.setNodes(destTaskNodeList)

    val dag: DAG[String, TaskNode, TaskNodeRelation] = DagHelper.buildDagGraph(processDag)
    // 获取DAG拓扑排序
    val topologicalSort: java.util.List[String] = dag.topologicalSort
    println(topologicalSort)

    // 按照拓扑排序依次执行

    import scala.collection.JavaConversions._
    import scala.util.control._

    var tryRunResult: String = null

    // 在 breakable 中循环
    val loop = new Breaks;
    loop.breakable {
      for (nodeName <- topologicalSort) {
        val taskNode: TaskNode = dag.getNode(nodeName)
        println(s"执行node >>>>>>>>> ${taskNode.getName} , 该node依赖：${dag.getPreviousNodes(nodeName)}")
        taskNode.process()
        println(jobContext.completedDataFramePool.size)
        if (nodeName == tryRunTaskNodeName && isTryRunMode) {
          val df = jobContext.completedDataFramePool(nodeName)
          tryRunResult = df.collect().mkString(",")
          loop.break()
        }
      }
    }


    tryRunResult
  }
}
