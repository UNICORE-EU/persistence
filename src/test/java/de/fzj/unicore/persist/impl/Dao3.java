package de.fzj.unicore.persist.impl;

import java.io.Serializable;

import de.fzj.unicore.persist.annotations.Column;
import de.fzj.unicore.persist.annotations.ID;


public class Dao3 implements Serializable {
	
	private static final long serialVersionUID = 1L;

	@ID
	private String id;
	
	@Column(name="foo")
	private String field="some-content";

	private Integer data;
	
	public Integer getData() {
		return data;
	}

	public void setData(Integer data) {
		this.data = data;
	}
	
	public String getField() {
		return field;
	}

	public void setField(String extra) {
		this.field = extra;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
}
