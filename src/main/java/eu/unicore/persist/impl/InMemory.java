package eu.unicore.persist.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.unicore.persist.PersistenceException;


/**
 * in-memory "persistence" backed by a hash map 
 * 
 * @author schuller
 */
public class InMemory<T> extends Base<T>{

	protected Map<String,T>map;
	
	public InMemory(Class<T>daoClass){
		super(daoClass);
	}
	
	@Override
	public void init() throws PersistenceException, SQLException {
		super.init();
		map = new ConcurrentHashMap<>();
	}

	@Override
	public String getDriverName() {
		return ConcurrentHashMap.class.getName();
	}
	
	@Override
	public void shutdown() {}

	@Override
	public List<String> getIDs() throws PersistenceException {
		List<String> res = new ArrayList<>();
		for(String s: map.keySet())res.add(s);
		return res;
	}

	@Override
	public List<String> getIDs(String column, Object value) throws PersistenceException {
		List<String> res = new ArrayList<>();
		for(String s: map.keySet()){
			T val=map.get(s);
			String cmp=pd.getColumnValue(column, val);
			if(compare(value,cmp))res.add(s);
		}
		return res;
	}
	
	@Override
	public List<String> findIDs(boolean orMode, String column, String... values) throws PersistenceException {
		List<String> res=new ArrayList<>();
		for(String s: map.keySet()){
			T val=map.get(s);
			String cmp=pd.getColumnValue(column, val);
			if(compareWeak(orMode, values,cmp))res.add(s);
		}
		return res;
	}
	
	@Override
	public List<String> findIDs(String column, String... values) throws PersistenceException {
		return findIDs(false, column, values);
	}
	
	@Override
	public Map<String,String> getColumnValues(String column) throws PersistenceException {
		Map<String,String> res = new HashMap<>();
		for(String id: map.keySet()){
			T val=map.get(id);
			String value=pd.getColumnValue(column, val);
			res.put(id,value);
		}
		return res;
	}

	@Override
	public int getRowCount(String column, Object value) throws PersistenceException {
		return getIDs(column,value).size();
	}

	@Override
	public int getRowCount() throws PersistenceException {
		return getIDs().size();
	}

	
	private boolean compare(Object value, String cmp){
		if(value==null)return cmp==null;
		return value.equals(cmp);
	}
	
	private boolean compareWeak(boolean orMode, String[] values, String cmp){
		if(values==null||values.length==0)return cmp==null;
		for(String value: values){
			if(orMode) {
				if(cmp.contains(value))return true;
			}
			else {
				if(!cmp.contains(value))return false;
			}
		}
		return !orMode;
	}
	
	@Override
	protected T _read(String id) throws PersistenceException {
		return map.get(id);
	}

	@Override
	protected void _remove(String id) throws PersistenceException {
		map.remove(id);
	}
	
	@Override
	protected void _removeAll() throws PersistenceException {
		map.clear();
	}

	@Override
	protected void _write(T dao, String id) throws PersistenceException {
		map.put(id, dao);
	}

	@Override
	public void purge(){
		map.clear();
	}

}