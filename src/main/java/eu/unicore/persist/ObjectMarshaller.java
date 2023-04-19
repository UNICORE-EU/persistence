package eu.unicore.persist;

import java.io.InputStream;

public interface ObjectMarshaller<T> {

	public T deserialize(InputStream is);
	
	public T deserialize(byte[] data);
	
	public byte[] serialize(T object);
	
	public String encode(T input);
	
	public T decode(String input);
	
}
