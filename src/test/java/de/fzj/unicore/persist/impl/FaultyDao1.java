package de.fzj.unicore.persist.impl;

import java.io.Serializable;

import de.fzj.unicore.persist.annotations.ID;


//faulty thing
public class FaultyDao1 implements Serializable {
	
	private static final long serialVersionUID = 1L;

	@ID
	private Object id;
	
	public Object getId() {
		return id;
	}
	
}
