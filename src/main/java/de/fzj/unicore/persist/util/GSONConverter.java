package de.fzj.unicore.persist.util;

import java.lang.reflect.Type;

public interface GSONConverter {

	public Type getType();
	
	public Object[] getAdapters();
	
	public boolean isHierarchy();
	
}
