package eu.unicore.persist.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.unicore.persist.ObjectMarshaller;
import eu.unicore.persist.util.GSONConverter;
import eu.unicore.persist.util.JSON;

public class JSONMarshaller<T> implements ObjectMarshaller<T> {

	private final Gson gson;

	private final Class<T>classOfT;

	public JSONMarshaller(Class<T>classOfT){
		this.classOfT = classOfT;
		this.gson=build();
	}

	private Gson build(){
		GsonBuilder builder = new GsonBuilder();
		registerTypeConverters(classOfT, builder);
		return builder.create();
	}

	@Override
	public T deserialize(InputStream is) {
		return gson.fromJson(new InputStreamReader(is), classOfT);
	}

	@Override
	public T deserialize(byte[] data) {
		return deserialize(new ByteArrayInputStream(data));
	}

	@Override
	public byte[] serialize(T object) {
		return encode(object).getBytes();
	}

	@Override
	public String encode(T input) {
		return gson.toJson(input);
	}

	@Override
	public T decode(String input) {
		return gson.fromJson(input, classOfT);
	}

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
					GSONConverter conv = (GSONConverter)c.getConstructor().newInstance();
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
