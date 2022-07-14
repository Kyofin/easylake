package com.data.easyflow.model;

/**
 * @program: easyflow
 * @author: huzekang
 * @create: 2021-11-03 09:45
 **/
public class SchemaChangeCol {

	/**
	 * 原始字段名
	 */
	private String originalColName;
	/**
	 * 改变后的字段名
	 */
	private String changedColName;
	/**
	 * 是否保留
	 */
	private boolean isKeep;
	/**
	 * 改变后的字段类型，只允许spark sql支持的类型
	 */
	private String changedColType;

	public String getOriginalColName() {
		return originalColName;
	}

	public void setOriginalColName(String originalColName) {
		this.originalColName = originalColName;
	}

	public String getChangedColName() {
		return changedColName;
	}

	public void setChangedColName(String changedColName) {
		this.changedColName = changedColName;
	}

	public boolean isKeep() {
		return isKeep;
	}

	public void setKeep(boolean keep) {
		isKeep = keep;
	}

	public String getChangedColType() {
		return changedColType;
	}

	public void setChangedColType(String changedColType) {
		this.changedColType = changedColType;
	}
}
