package eu.unicore.persist.util;

import java.io.Serializable;
import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * helper class for storing "arbitrary" objects using GSON
 *
 * @author schuller
 */
public class Wrapper<T extends Serializable> {

	private T content;
	
	private String className;
	
	public Wrapper(T target){
		this.content = target;
		if(target!=null){
			className = target.getClass().getName();
		}
	}
	
	public T get(){
		if(className!=null){
			try{
				return content;
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		}
		else{
			return null;
		}
	}
	
	public String toString(){
		return "Wrapper["+String.valueOf(content)+"]";
	}
	
	public static class WrapperConverter implements GSONConverter{

		@Override
		public Type getType() {
			return Wrapper.class;
		}

		@Override
		public Object[] getAdapters() {
			return new Object[]{adapter};
		}

		@Override
		public boolean isHierarchy() {
			return true;
		}
		
	}
	
	private static final WrapperAdapter adapter=new WrapperAdapter();
	
	@SuppressWarnings("rawtypes")
	public static class WrapperAdapter implements JsonDeserializer<Wrapper>{

		@Override
		@SuppressWarnings("unchecked")
		public Wrapper deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context)
						throws JsonParseException {
			JsonElement jsonClassname=json.getAsJsonObject().get("className");
			Serializable target = null;
			if(jsonClassname!=null){
				String className = json.getAsJsonObject().get("className").getAsString();
				try{
					Class<?>clazz = Class.forName(className);
					target =  context.deserialize(json.getAsJsonObject().get("content"),clazz);
				}catch(ClassNotFoundException cne){
					throw new JsonParseException("Unknown model class", cne);
				}
			}
			return new Wrapper(target);
		}

	}

}
