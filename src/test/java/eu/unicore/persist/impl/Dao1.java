package eu.unicore.persist.impl;

import java.io.Serializable;

import eu.unicore.persist.annotations.Column;
import eu.unicore.persist.annotations.ID;


public class Dao1 implements Serializable{

	private static final long serialVersionUID = 1L;

	private String data;
	
	@Column(name="other")
	private String other="";
	
	@ID
	private String id;

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getOther() {
		return other;
	}

	public void setOther(String other) {
		this.other = other;
	}
	
	
	
}
