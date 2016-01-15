package com.join.bean;

import java.util.HashMap;

public class Table {
	
	private HashMap<String, Integer> field = new HashMap<String, Integer>();
	private String path;
	private String name;
	
	
	
	public HashMap<String, Integer> getField() {
		return field;
	}
	public void setField(HashMap<String, Integer> field) {
		this.field = field;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	

}
