package de.fzj.unicore.persist;

import java.io.IOException;
import java.io.InputStream;

public interface ObjectMarshaller<T> {

	public T deserialize(InputStream is)throws IOException, PersistenceException;
	
	public T deserialize(byte[] data)throws IOException, PersistenceException;
	
	public byte[] serialize(T object)throws IOException, PersistenceException;
	
	public String encode(T input)throws IOException, PersistenceException;
	
	public T decode(String input)throws IOException, PersistenceException;
	
}
