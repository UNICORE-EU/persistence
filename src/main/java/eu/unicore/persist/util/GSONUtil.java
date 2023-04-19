package eu.unicore.persist.util;

import com.google.gson.GsonBuilder;

public class GSONUtil {

	private GSONUtil(){}
	
	/**
	 * register any GSON type converters that the class may expose
	 * @param daoClass
	 * @param builder
	 */
	public static void registerTypeConverters(Class<?> daoClass, GsonBuilder builder){
		JSON json = daoClass.getAnnotation(JSON.class);
		if(json!=null && json.customHandlers().length>0){
			for(Class<?> c: json.customHandlers()){
				try{ 
					GSONConverter conv=(GSONConverter)c.getConstructor().newInstance();
					for(Object adapter: conv.getAdapters()){
						if(conv.isHierarchy()){
							builder.registerTypeHierarchyAdapter((Class<?>)conv.getType(),adapter);
						}
						else{
							builder.registerTypeAdapter(conv.getType(),adapter);
						}
					}
				}catch(Exception ex){
					throw new IllegalStateException(ex);
				}
			}
		}
	}

}
