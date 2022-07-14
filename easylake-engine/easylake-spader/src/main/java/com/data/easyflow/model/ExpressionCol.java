package com.data.easyflow.model;

/**
 * @program: easyflow
 * @author: huzekang
 * @create: 2021-11-02 14:22
 **/
public class ExpressionCol {

	private String colName;
	private String expression;

	public ExpressionCol(String colName, String expression) {
		this.colName = colName;
		this.expression = expression;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}
}
