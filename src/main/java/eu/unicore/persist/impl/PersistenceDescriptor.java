/*********************************************************************************
 * Copyright (c) 2008 Forschungszentrum Juelich GmbH 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the disclaimer at the end. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 * 
 * (2) Neither the name of Forschungszentrum Juelich GmbH nor the names of its 
 * contributors may be used to endorse or promote products derived from this 
 * software without specific prior written permission.
 * 
 * DISCLAIMER
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *********************************************************************************/

package eu.unicore.persist.impl;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.unicore.persist.PersistenceException;

/**
 * persistence information obtained through introspection
 * of the DAO class
 * 
 * @see ClassScanner
 * @author schuller
 */
public class PersistenceDescriptor {

	public PersistenceDescriptor(){}
	
	private String tableName;
	
	private Method getIdMethod;
	
	private List<ColumnDescriptor>columns;
	
	private Map<String,ColumnDescriptor>columnMap;
	
	public static PersistenceDescriptor get(Class<?>daoClass){
		PersistenceDescriptor pd=new PersistenceDescriptor();
		pd.setTableName(ClassScanner.getTableName(daoClass));
		pd.getIdMethod=ClassScanner.getGetIdMethod(daoClass);
		pd.setColumns(ClassScanner.getColumns(daoClass));
		pd.columnMap=new HashMap<String, ColumnDescriptor>();
		for(ColumnDescriptor c: pd.columns){
			pd.columnMap.put(c.getColumn(), c);
		}
		return pd;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	/**
	 * get the unique ID for the given object
	 * 
	 * @param dao - the target object
	 * @return the unique ID
	 * @throws PersistenceException
	 */
	public String getID(Object dao)throws PersistenceException{
		try{
			return getIdMethod.invoke(dao, (Object[] )null).toString();
		}catch(Exception e){
			throw new PersistenceException(e);
		}
	}

	public List<ColumnDescriptor> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnDescriptor> columns) {
		this.columns = columns;
	}
	
	/**
	 * helper to get the value of a column for an object
	 * @param columnName - the column name
	 * @param dao -  the target oject
	 * @return - the value of the column
	 * @throws PersistenceException
	 */
	public String getColumnValue(String columnName, Object dao)throws PersistenceException{
		try{
			return (String)columnMap.get(columnName).getMethod().invoke(dao, (Object[])null);
		}
		catch(Exception e){
			throw new PersistenceException(e);
		}
	}

}
