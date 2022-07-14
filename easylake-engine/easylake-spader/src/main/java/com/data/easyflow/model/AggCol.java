package com.data.easyflow.model;

/**
 * @program: easyflow
 * @author: huzekang
 * @create: 2021-11-02 14:40
 **/
public class AggCol {
	private String colName;
	private String aggMethod;

	public AggCol(String colName, String aggMethod) {
		this.colName = colName;
		this.aggMethod = aggMethod;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getAggMethod() {
		return aggMethod;
	}

	public void setAggMethod(String aggMethod) {
		this.aggMethod = aggMethod;
	}
}
