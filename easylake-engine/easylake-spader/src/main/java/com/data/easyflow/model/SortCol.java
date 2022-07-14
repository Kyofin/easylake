package com.data.easyflow.model;

/**
 * @program: easyflow
 * @author: huzekang
 * @create: 2021-11-02 14:33
 **/
public class SortCol {
	private String colName;
	private String sortMethod;

	public SortCol(String colName, String sortMethod) {
		this.colName = colName;
		this.sortMethod = sortMethod;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getSortMethod() {
		return sortMethod;
	}

	public void setSortMethod(String sortMethod) {
		this.sortMethod = sortMethod;
	}
}
