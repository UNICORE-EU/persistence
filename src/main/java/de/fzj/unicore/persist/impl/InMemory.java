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

package de.fzj.unicore.persist.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.fzj.unicore.persist.PersistenceException;


/**
 * in-memory "persistence" backed by a hash map 
 * 
 * @author schuller
 */
public class InMemory<T> extends Base<T>{

	protected Map<String,T>map;
	
	public InMemory(){}
	
	public void init() throws PersistenceException {
		super.init();
		map = new ConcurrentHashMap<String, T>();
	}
	public String getDriverName() {
		return ConcurrentHashMap.class.getName();
	}
	
	public void shutdown() {}

	public List<String> getIDs() throws PersistenceException {
		List<String> res=new ArrayList<String>();
		for(String s: map.keySet())res.add(s);
		return res;
	}

	public List<String> getIDs(String column, Object value) throws PersistenceException {
		List<String> res=new ArrayList<String>();
		for(String s: map.keySet()){
			T val=map.get(s);
			String cmp=pd.getColumnValue(column, val);
			if(compare(value,cmp))res.add(s);
		}
		return res;
	}
	
	public List<String> findIDs(boolean orMode, String column, String... values) throws PersistenceException {
		List<String> res=new ArrayList<String>();
		for(String s: map.keySet()){
			T val=map.get(s);
			String cmp=pd.getColumnValue(column, val);
			if(compareWeak(orMode, values,cmp))res.add(s);
		}
		return res;
	}
	
	public List<String> findIDs(String column, String... values) throws PersistenceException {
		return findIDs(false, column, values);
	}
	
	public Map<String,String> getColumnValues(String column) throws PersistenceException {
		Map<String,String> res=new HashMap<String,String>();
		for(String id: map.keySet()){
			T val=map.get(id);
			String value=pd.getColumnValue(column, val);
			res.put(id,value);
		}
		return res;
	}

	public int getRowCount(String column, Object value) throws PersistenceException {
		return getIDs(column,value).size();
	}

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

	public void purge(){
		map.clear();
	}

}