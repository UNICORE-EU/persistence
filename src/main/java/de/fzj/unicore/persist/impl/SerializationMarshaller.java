package de.fzj.unicore.persist.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.Logger;

import de.fzj.unicore.persist.DataVersionException;
import de.fzj.unicore.persist.ObjectMarshaller;
import de.fzj.unicore.persist.PersistenceException;

/**
 * serialization/deserialization using the default Java object serialization
 *
 * @author schuller
 */
public class SerializationMarshaller<T> implements ObjectMarshaller<T> {

	private final Logger logger;
	
	private final String tableName;
	
	private final ClassLoader customClassLoader;
	
	private int contentVersion;

	private final static int MAGIC_VERSION_HEADER=77;
	
	/**
	 * @param tableName
	 * @param contentVersion
	 * @param errorHandler
	 * @param classLoader
	 * @param logger
	 */
	public SerializationMarshaller(String tableName, int contentVersion, ClassLoader classLoader, Logger logger){
		this.logger=logger;
		this.contentVersion=contentVersion;
		this.customClassLoader=classLoader;
		this.tableName=tableName;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(InputStream is) throws IOException, PersistenceException {
		checkVersion(is);
		ObjectInputStream ois=getObjectInputStream(is);
		try{
			return (T)ois.readObject();
		}catch(ClassNotFoundException ce){
			throw new PersistenceException(ce);
		}finally{
			try{ois.close();}catch(IOException e){logger.debug("",e);}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public T deserialize(byte[] data) throws IOException, PersistenceException {
		ByteArrayInputStream is=new ByteArrayInputStream(data);
		checkVersion(is);
		ObjectInputStream ois=getObjectInputStream(is);
		try{
			Object result = ois.readObject();
			return (T)result;
		}catch(ClassNotFoundException ce){
			throw new PersistenceException(ce);
		}finally{
			try{ois.close();}catch(IOException e){logger.debug("",e);}
		}
	}

	@Override
	public byte[] serialize(T object) throws IOException {
		ByteArrayOutputStream bos=new ByteArrayOutputStream();
		bos.write(MAGIC_VERSION_HEADER);
		bos.write(contentVersion);
		ObjectOutputStream oos=new ObjectOutputStream(bos);
		synchronized (object) {
			oos.writeObject(object);
			oos.close();
		}
		if(logger.isDebugEnabled()){
			logger.debug("Serialized size ["+tableName+"] "+bos.size());
		}
		return bos.toByteArray();
	}

	public String encode(T a)throws IOException, PersistenceException {
		return new String(Base64.encodeBase64(serialize(a)));
	}

	public T decode(String base64)throws IOException, PersistenceException {
		return deserialize(Base64.decodeBase64(base64.getBytes()));
	}
	
	private ObjectInputStream getObjectInputStream(InputStream source)throws IOException{
		return new CustomObjectInputStream(source,customClassLoader);
	}
	
	private void checkVersion(InputStream is)throws IOException, DataVersionException{
		//do not check if expected version is "-1"
		if(contentVersion==-1) return;
		
		int magic=is.read();
		if(MAGIC_VERSION_HEADER!=magic)throw new DataVersionException(contentVersion,-1);
		int ver=is.read();
		if(ver!=contentVersion)throw new DataVersionException(contentVersion,ver);
	}

	// testing use
	public void setContentVersion(int contentVersion) {
		this.contentVersion = contentVersion;
	}

}
