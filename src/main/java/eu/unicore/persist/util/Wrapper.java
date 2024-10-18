package eu.unicore.persist.util;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

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

	private final T content;

	@SuppressWarnings("unused")
	private final String className;

	public Wrapper(T target){
		this.content = target;
		className = target!=null ? target.getClass().getName():null;
	}

	public T get(){
		return content;
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
				for(String p: updates.keySet()) {
					if(className.startsWith(p)) {
						className = className.replace(p, updates.get(p));
					}
				}
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

	public static final Map<String,String> updates = new HashMap<>();

}
