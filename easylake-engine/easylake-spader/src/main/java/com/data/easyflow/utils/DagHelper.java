
package com.data.easyflow.utils;



import com.data.easyflow.common.TaskDependType;
import com.data.easyflow.graph.DAG;
import com.data.easyflow.model.TaskNode;
import com.data.easyflow.model.TaskNodeRelation;
import com.data.easyflow.process.ProcessDag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * dag tools
 */
public class DagHelper {


    private static final Logger logger = LoggerFactory.getLogger(DagHelper.class);


    /**
     * @importangt
     * generate flow node relation list by task node list;
     * Edges that are not in the task Node List will not be added to the result
     * @param taskNodeList taskNodeList
     * @return task node relation list
     */
    public static List<TaskNodeRelation> generateRelationListByFlowNodes(List<TaskNode> taskNodeList) {
        List<TaskNodeRelation> nodeRelationList = new ArrayList<>();
        for (TaskNode taskNode : taskNodeList) {
            String preTasks = taskNode.getPreTasks();
            List<String> preTaskList = JSONUtils.toList(preTasks, String.class);
            if (preTaskList != null) {
                for (String depNodeName : preTaskList) {
                    if (null != findNodeByName(taskNodeList, depNodeName)) {
                        nodeRelationList.add(new TaskNodeRelation(depNodeName, taskNode.getName()));
                    }
                }
            }
        }
        return nodeRelationList;
    }

    /**
     * @importangt
     * generate task nodes needed by dag
     * @param taskNodeList taskNodeList
     * @param startNodeNameList startNodeNameList
     * @param recoveryNodeNameList recoveryNodeNameList
     * @param taskDependType taskDependType
     * @return task node list
     */
    public static List<TaskNode> generateFlowNodeListByStartNode(List<TaskNode> taskNodeList, List<String> startNodeNameList,
                                                                 List<String> recoveryNodeNameList, TaskDependType taskDependType) {
        List<TaskNode> destFlowNodeList = new ArrayList<>();
        List<String> startNodeList = startNodeNameList;

        if(taskDependType != TaskDependType.TASK_POST
                && CollectionUtils.isEmpty(startNodeList)){
            logger.error("start node list is empty! cannot continue run the process ");
            return destFlowNodeList;
        }
        List<TaskNode> destTaskNodeList = new ArrayList<>();
        List<TaskNode> tmpTaskNodeList = new ArrayList<>();
        if (taskDependType == TaskDependType.TASK_POST
                && CollectionUtils.isNotEmpty(recoveryNodeNameList)) {
            startNodeList = recoveryNodeNameList;
        }
        if (CollectionUtils.isEmpty(startNodeList)) {
            // no special designation start nodes
            tmpTaskNodeList = taskNodeList;
        } else {
            // specified start nodes or resume execution
            for (String startNodeName : startNodeList) {
                TaskNode startNode = findNodeByName(taskNodeList, startNodeName);
                List<TaskNode> childNodeList = new ArrayList<>();
                if (startNode == null) {
                    logger.error("start node name [{}] is not in task node list [{}] ",
                        startNodeName,
                        taskNodeList
                    );
                    continue;
                } else if (TaskDependType.TASK_POST == taskDependType) {
                    childNodeList = getFlowNodeListPost(startNode, taskNodeList);
                } else if (TaskDependType.TASK_PRE == taskDependType) {
                    childNodeList = getFlowNodeListPre(startNode, recoveryNodeNameList, taskNodeList);
                } else {
                    childNodeList.add(startNode);
                }
                tmpTaskNodeList.addAll(childNodeList);
            }
        }

        for (TaskNode taskNode : tmpTaskNodeList) {
            if (null == findNodeByName(destTaskNodeList, taskNode.getName())) {
                destTaskNodeList.add(taskNode);
            }
        }
        return destTaskNodeList;
    }

    /**
     * find all nodes that start nodes depend on.
     * @param startNode startNode
     * @param recoveryNodeNameList recoveryNodeNameList
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private static List<TaskNode> getFlowNodeListPre(TaskNode startNode, List<String> recoveryNodeNameList, List<TaskNode> taskNodeList) {

        List<TaskNode> resultList = new ArrayList<>();

        List<String> depList = new ArrayList<>();
        if (null != startNode) {
            depList = startNode.getDepList();
            resultList.add(startNode);
        }
        if (CollectionUtils.isEmpty(depList)) {
            return resultList;
        }
        for (String depNodeName : depList) {
            TaskNode start = findNodeByName(taskNodeList, depNodeName);
            if (recoveryNodeNameList.contains(depNodeName)) {
                resultList.add(start);
            } else {
                resultList.addAll(getFlowNodeListPre(start, recoveryNodeNameList, taskNodeList));
            }
        }
        return resultList;
    }
    /**
     * find node by node name
     * @param nodeDetails nodeDetails
     * @param nodeName nodeName
     * @return task node
     */
    public static TaskNode findNodeByName(List<TaskNode> nodeDetails, String nodeName) {
        for (TaskNode taskNode : nodeDetails) {
            if (taskNode.getName().equals(nodeName)) {
                return taskNode;
            }
        }
        return null;
    }
    /**
     * find all the nodes that depended on the start node
     * @param startNode startNode
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private static List<TaskNode> getFlowNodeListPost(TaskNode startNode, List<TaskNode> taskNodeList) {
        List<TaskNode> resultList = new ArrayList<>();
        for (TaskNode taskNode : taskNodeList) {
            List<String> depList = taskNode.getDepList();
            if (null != depList && null != startNode && depList.contains(startNode.getName())) {
                resultList.addAll(getFlowNodeListPost(taskNode, taskNodeList));
            }
        }
        resultList.add(startNode);
        return resultList;
    }

    /***
     * @importangt
     * build dag graph
     * @param processDag processDag
     * @return dag
     */
    public static DAG<String, TaskNode, TaskNodeRelation> buildDagGraph(ProcessDag processDag) {

        DAG<String,TaskNode,TaskNodeRelation> dag = new DAG<>();

        //add vertex
        if (CollectionUtils.isNotEmpty(processDag.getNodes())){
            for (TaskNode node : processDag.getNodes()){
                dag.addNode(node.getName(),node);
            }
        }

        //add edge
        if (CollectionUtils.isNotEmpty(processDag.getEdges())){
            for (TaskNodeRelation edge : processDag.getEdges()){
                dag.addEdge(edge.getStartNode(),edge.getEndNode());
            }
        }
        return dag;
    }






}
