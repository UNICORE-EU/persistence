package eu.unicore.persist.impl;

import java.io.Serializable;

import eu.unicore.persist.annotations.ID;
import eu.unicore.persist.util.JSON;
import eu.unicore.persist.util.Wrapper;

@JSON(customHandlers=Wrapper.WrapperConverter.class)
public class Dao5 implements Serializable {
	
	private static final long serialVersionUID = 1L;

	@ID
	private String id;

	private Wrapper<Serializable> data;
	
	public Serializable getData() {
		return data!=null? data.get():null;
	}

	public void setData(Serializable data) {
		this.data = new Wrapper<Serializable>(data);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
}
