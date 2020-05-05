package de.fzj.unicore.persist.impl;

import java.io.Serializable;

import de.fzj.unicore.persist.annotations.ID;


//faulty thing
public class FaultyDao2 implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private Object id;
	
	@ID
	public Object getId() {
		return id;
	}
	
}
