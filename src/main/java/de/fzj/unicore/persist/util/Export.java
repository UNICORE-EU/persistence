package de.fzj.unicore.persist.util;

import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceFactory;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.PersistImpl;
import de.fzj.unicore.persist.impl.PersistenceDescriptor;

/**
 * Exports a database to JSON<br/>
 * 
 * The output is written to the console (so it can be easily
 * redirected to a file)
 * 
 * @author schuller
 */
@SuppressWarnings("rawtypes")
public class Export {

	private final Persist input;
	private Gson gson;
	private Writer output;
	Class daoClass;
	
	public Export(PersistImpl<?> input)throws Exception{
		this.input = input;
		daoClass=input.getDaoClass();
		completeSetup();
	}
	
	@SuppressWarnings("unchecked")
	public Export(Properties inputConfig)throws Exception{
		daoClass=Class.forName((String)inputConfig.remove("class"));
		Class inPersistImpl=Class.forName(inputConfig.getProperty("persistence.class"));
		String inTableName=(String)inputConfig.remove("tableName");
		PersistenceDescriptor pdIn=PersistenceDescriptor.get(daoClass);
		if(inTableName!=null){
			pdIn.setTableName(inTableName);
		}
		input=PersistenceFactory.get(new PersistenceProperties(inputConfig)).configurePersist(daoClass, inPersistImpl, pdIn);
		completeSetup();
	}
	
	private void completeSetup(){
		GsonBuilder builder = new GsonBuilder();
		GSONUtil.registerTypeConverters(daoClass, builder);
		gson = builder.create();
		output=new OutputStreamWriter(System.out);
	}
	
    @SuppressWarnings("unchecked")
	public void doExport()throws Exception{
    	JsonWriter writer = new JsonWriter(output);
		int errors=0;
		Collection<String>ids=input.getIDs();
		System.out.print("Will export "+ids.size()+ " entries.");
		//convert
		writer.beginArray();
		for(Object s: ids){
			System.out.print("Converting "+s+ " ...");
			try{
				Object in=input.getForUpdate((String)s);
				gson.toJson(in,daoClass,writer);
				System.out.println("... OK");
			}catch(Exception ex){
				System.out.println("... error: "+ex.getMessage());
				errors++;
			}
		}
		writer.endArray();
		writer.close();
		System.out.println("Done, "+errors+" errors occured.");
	}

    public void shutdown() throws PersistenceException, SQLException {
    	input.shutdown();
    }
    
	public Persist<?> getInput() {
		return input;
	}

	/**
	 * main class, having as (optional) argument the input configuration file name
	 * System properties are taken into account as well
	 */ 
	public static void main(String[] args)throws Exception {
		System.out.println("*** Convert utility *** ");
		//setup input database
		Properties inputConfig=new Properties();
		if(args.length>0){
			inputConfig.load(new FileInputStream(args[0]));
		}
		inputConfig.putAll(System.getProperties());
		
		Export export=new Export(inputConfig);
		export.doExport();
		export.shutdown();
	}
	
}
