package de.fzj.unicore.persist.util;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Properties;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceFactory;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.PersistenceDescriptor;

/**
 * converts a database
 * 
 * @author schuller
 */
@SuppressWarnings("rawtypes")
public class Convert {

	private final Persist input;

	private final Persist output;
	
	@SuppressWarnings("unchecked")
	public Convert(Properties inputConfig, Properties outputConfig)throws Exception{
		Class inClass=Class.forName((String)inputConfig.remove("class"));
		Class inPersistImpl=Class.forName(inputConfig.getProperty("persistence.class"));
		String inTableName=(String)inputConfig.remove("tableName");
		PersistenceDescriptor pdIn=PersistenceDescriptor.get(inClass);
		if(inTableName!=null){
			pdIn.setTableName(inTableName);
		}
		input=PersistenceFactory.get(new PersistenceProperties(inputConfig)).configurePersist(inClass, inPersistImpl, pdIn);
		
		Class outClass=Class.forName((String)outputConfig.remove("class"));
		Class outPersistImpl=Class.forName(outputConfig.getProperty("persistence.class"));
		String outTableName=(String)outputConfig.remove("tableName");
		PersistenceDescriptor pdOut=PersistenceDescriptor.get(outClass);
		if(outTableName!=null){
			pdOut.setTableName(outTableName);
		}
		output=PersistenceFactory.get(new PersistenceProperties(outputConfig)).configurePersist(outClass,outPersistImpl,pdOut);
	}
	
    @SuppressWarnings("unchecked")
	public void convert()throws Exception{
		int errors=0;
		Collection<String>ids=input.getIDs();
		System.out.print("Will convert "+ids.size()+ " entries.");
		//convert
		for(Object s: ids){
			System.out.print("Converting "+s+ " ...");
			try{
				Object in=input.getForUpdate((String)s);
				output.write(in);
				System.out.println("... OK");
			}catch(Exception ex){
				System.out.println("... error: "+ex.getMessage());
				errors++;
			}
		}
		
		System.out.println("Done, "+errors+" errors occured.");

	}
	
	public void shutDown()throws Exception{
		output.shutdown();
		input.shutdown();
	}

	public Persist<?> getInput() {
		return input;
	}

	public Persist<?> getOutput() {
		return output;
	}

	/**
	 * main class, having as arguments:
	 *  1: input configuration file name
	 *  2: output configuration file name
	 */
	public static void main(String[] args)throws Exception {
		System.out.println("*** Convert utility *** ");
		//setup input database
		Properties inputConfig=new Properties();
		inputConfig.load(new FileInputStream(args[0]));
		
		//setup output database
		Properties outputConfig=new Properties();
		outputConfig.load(new FileInputStream(args[1]));
		
		Convert converter=new Convert(inputConfig, outputConfig);
		converter.convert();
		converter.shutDown();
		
	}
	
}
