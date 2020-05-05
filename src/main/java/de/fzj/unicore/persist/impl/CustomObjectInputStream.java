package de.fzj.unicore.persist.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Used for deserialization purposes where custom class loaders are needed.
 * Also offers error handling functions when classes cannot be deserialized.
 * 
 * @author schuller
 * @author j.daivandy@fz-juelich.de
 */
public class CustomObjectInputStream extends ObjectInputStream {

	private final ClassLoader classLoader;

	/**
	 * Build an instance of CustomObjectInputStream that uses the specified classloader for deserialization.
	 * @param input - the stream to read object data from
	 * @param classLoader -  custom class loader (can be null)
	 * @param errorHandler - a handler for dealing with problems while reading objects
	 * @throws IOException
	 * @throws SecurityException
	 */
	public CustomObjectInputStream(InputStream input, ClassLoader classLoader) throws IOException, SecurityException {
		super(input);
		this.classLoader = classLoader;
	}


	/**
	 * This method is used for deserializing objects with a custom classloader.
	 */
	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
		if(classLoader!=null){
			return Class.forName(desc.getName(), true, classLoader);
		}
		else{
			return Class.forName(desc.getName());
		}
	}

}