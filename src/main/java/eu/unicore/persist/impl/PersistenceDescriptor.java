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
