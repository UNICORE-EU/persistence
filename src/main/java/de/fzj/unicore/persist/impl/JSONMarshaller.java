package de.fzj.unicore.persist.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.fzj.unicore.persist.ObjectMarshaller;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.util.GSONUtil;
import eu.unicore.util.Log;

public class JSONMarshaller<T> implements ObjectMarshaller<T> {

	private static final Logger logger = Log.getLogger("unicore.persistence", JSONMarshaller.class);

	private final Gson gson;
	private final Class<T>classOfT;
	
	public JSONMarshaller(Gson gson, Class<T>classOfT){
		this.gson = gson;
		this.classOfT = classOfT;
	}

	public JSONMarshaller(Class<T>classOfT){
		this.classOfT = classOfT;
		this.gson=build();
	}
	
	private Gson build(){
		GsonBuilder builder = new GsonBuilder();
		GSONUtil.registerTypeConverters(classOfT, builder);
		return builder.create();
	}
	@Override
	public T deserialize(InputStream is) throws IOException,
			PersistenceException {
		return gson.fromJson(new InputStreamReader(is), classOfT);
	}

	@Override
	public T deserialize(byte[] data) throws IOException, PersistenceException {
		if(logger.isTraceEnabled()){
			logger.trace("De-serializing to "+classOfT.getName()+":\n"+new String(data));
		}
		return deserialize(new ByteArrayInputStream(data));
	}

	@Override
	public byte[] serialize(T object) throws IOException, PersistenceException {
		return encode(object).getBytes();
	}

	@Override
	public String encode(T input) throws IOException, PersistenceException {
		String json=gson.toJson(input);
		if(logger.isTraceEnabled()){
			logger.trace("Serialized form of "+input+"\n"+json);
		}
		return json;
	}

	@Override
	public T decode(String input) throws IOException, PersistenceException {
		if(logger.isTraceEnabled()){
			logger.trace("De-serializing to "+classOfT.getName()+":\n"+input);
		}
		return gson.fromJson(input, classOfT);
	}

}
