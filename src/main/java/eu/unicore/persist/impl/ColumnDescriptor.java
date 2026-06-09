package eu.unicore.persist.impl;

import java.lang.reflect.Method;

/**
 * name and access method for "extra" columns
 * 
 * TODO(?) support non-string types
 *  * 
 * @author schuller
 */
public class ColumnDescriptor {

	private final Method method;

	private final String column;

	public ColumnDescriptor(Method method, String column){
		this.method = method;
		this.column = column;
	}

	public Method getMethod() {
		return method;
	}

	public String getColumn() {
		return column;
	}

}
