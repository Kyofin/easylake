
package com.data.easyflow.model;

import java.util.Objects;

public class TaskNodeRelation {

  /**
   * task start node name
   */
  private String startNode;

  /**
   * task end node name
   */
  private String endNode;

  public TaskNodeRelation() {
  }

  public TaskNodeRelation(String startNode, String endNode) {
    this.startNode = startNode;
    this.endNode = endNode;
  }

  public String getStartNode() {
    return startNode;
  }

  public void setStartNode(String startNode) {
    this.startNode = startNode;
  }

  public String getEndNode() {
    return endNode;
  }

  public void setEndNode(String endNode) {
    this.endNode = endNode;
  }


  @Override
  public boolean equals(Object o){
    if (!(o instanceof TaskNodeRelation)) {
      return false;
    }
    TaskNodeRelation relation = (TaskNodeRelation)o;
    return (relation.getStartNode().equals(this.startNode) && relation.getEndNode().equals(this.endNode));
  }

  @Override
  public String toString() {
    return "TaskNodeRelation{" +
            "startNode='" + startNode + '\'' +
            ", endNode='" + endNode + '\'' +
            '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(startNode, endNode);
  }
}
