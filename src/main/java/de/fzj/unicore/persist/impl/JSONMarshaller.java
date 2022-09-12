package de.fzj.unicore.persist.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import de.fzj.unicore.persist.ObjectMarshaller;
import de.fzj.unicore.persist.util.GSONUtil;

public class JSONMarshaller<T> implements ObjectMarshaller<T> {

	private final Gson gson;

	private final Class<T>classOfT;

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

}
