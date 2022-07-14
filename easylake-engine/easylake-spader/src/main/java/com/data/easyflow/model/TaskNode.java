
package com.data.easyflow.model;

import com.data.easyflow.utils.JSONUtils;

import java.util.List;


public  class TaskNode {



  /**
   * task node name
   */
  private String name;


  /**
   * inner dependency information
   */
  private String preTasks;



  /**
   * node dependency list
   */
  private List<String> depList;



  public void process() {

  }



  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }



  public String getPreTasks() {
    return preTasks;
  }



  public List<String> getDepList() {
    return depList;
  }

  public void setDepList(List<String> depList) {
    this.depList = depList;
    this.preTasks = JSONUtils.toJson(depList);
  }


}
