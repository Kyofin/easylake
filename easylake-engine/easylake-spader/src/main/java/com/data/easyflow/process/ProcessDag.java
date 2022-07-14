
package com.data.easyflow.process;



import com.data.easyflow.model.TaskNode ;
import com.data.easyflow.model.TaskNodeRelation;

import java.util.List;

public class ProcessDag {

  /**
   * DAG edge list
   **/
  private List<TaskNodeRelation> edges;

  /**
   * DAG node list
   */
  private List<TaskNode> nodes;

  /**
   * getter method
   *
   * @return the edges
   * @see ProcessDag#edges
   */
  public List<TaskNodeRelation> getEdges() {
    return edges;
  }

  /**
   * setter method
   *
   * @param edges the edges to set
   * @see ProcessDag#edges
   */
  public void setEdges(List<TaskNodeRelation> edges) {
    this.edges = edges;
  }

  /**
   * getter method
   *
   * @return the nodes
   * @see ProcessDag#nodes
   */
  public List<TaskNode> getNodes() {
    return nodes;
  }

  /**
   * setter method
   *
   * @param nodes the nodes to set
   * @see ProcessDag#nodes
   */
  public void setNodes(List<TaskNode> nodes) {
    this.nodes = nodes;
  }

  @Override
  public String toString() {
    return "ProcessDag{" +
            "edges=" + edges +
            ", nodes=" + nodes +
            '}';
  }
}
