package eu.unicore.persist.impl;

import java.io.Serializable;

import eu.unicore.persist.annotations.Column;
import eu.unicore.persist.annotations.ID;


public class Dao2 implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private String id = "the id";
	
	@Column(name="foo")
	private String field="some-content";

	private String data;

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
	
	public String getField() {
		return field;
	}

	public void setField(String extra) {
		this.field = extra;
	}

	@ID
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
}
