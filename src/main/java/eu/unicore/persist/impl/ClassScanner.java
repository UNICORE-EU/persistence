package eu.unicore.persist.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import eu.unicore.persist.annotations.Column;
import eu.unicore.persist.annotations.ID;
import eu.unicore.persist.annotations.Table;


/**
 * Helper for evaluating annotations on a persistent class
 * 
 * @see PersistenceDescriptor
 * @author schuller
 */
public class ClassScanner {

	private ClassScanner(){}
	
	/**
	 * figure out the table name to use. If no {@link Table} annotation is
	 * present, the class name is used as default
	 * 
	 * @param daoClass
	 * @return table name
	 */
	public static String getTableName(Class<?>daoClass){
		if(daoClass.getAnnotation(Table.class)!=null){
			return ((Table)daoClass.getAnnotation(Table.class)).name();
		}
		return daoClass.getSimpleName();
	}
	
	/**
	 * retrieve the {@link Method} to use for getting the unique ID
	 * of an entity. This is selected by an {@link ID} annotation,
	 * either on a field (which MUST have an associated getNNN() method!!) 
	 * or on a method. 
	 * Both field and method must be of type {@link String}
	 * 
	 * @param daoClass - the Class of the entity
	 * @return
	 */
	public static Method getGetIdMethod(Class<?>daoClass){
		for(Method m: daoClass.getDeclaredMethods()){
			if(m.getAnnotation(ID.class)!=null){
				if(!String.class.equals(m.getReturnType())){
					throw new IllegalArgumentException("ID method must return java.lang.String");
				}
				return m;
			}
		}
		//else check if a field has the required annotation
		for(Field f: daoClass.getDeclaredFields()){
			if(f.getAnnotation(ID.class)!=null){
				//find getter
				String name="get"+f.getName();
				Method m=findMethod(daoClass, name);
				if(m!=null){
					if(!String.class.equals(m.getReturnType())){
						throw new IllegalArgumentException("ID method must return java.lang.String");
					}
					return m;
				}
			}
		}
		throw new IllegalArgumentException("Class has no useable ID annotation!");
	}
	
	public static List<ColumnDescriptor> getColumns(Class<?>daoClass){
		List<ColumnDescriptor>result=new ArrayList<ColumnDescriptor>();
		for(Method m: daoClass.getDeclaredMethods()){
			if(m.getAnnotation(Column.class)!=null){
				String name=((Column)m.getAnnotation(Column.class)).name();
				ColumnDescriptor cd=new ColumnDescriptor(m,name);
				result.add(cd);
			}
		}
		//else check if a field has the required annotation
		for(Field f: daoClass.getDeclaredFields()){
			if(f.getAnnotation(Column.class)!=null){
				//find getter
				String name="get"+f.getName();
				Method m=findMethod(daoClass, name);
				if(m==null)throw new IllegalArgumentException("Can't find getter method for column "+f.getName());
				String columnName=((Column)f.getAnnotation(Column.class)).name();
				if(columnName==null)columnName=f.getName();
				ColumnDescriptor cd=new ColumnDescriptor(m,columnName);
				result.add(cd);
			}
		}
		return result;
	}
	
	private static Method findMethod(Class<?>daoClass, String name){
		for(Method m: daoClass.getDeclaredMethods()){
			if(m.getName().equalsIgnoreCase(name)){
				return m;
			}
		}
		return null;
	}
	
}
