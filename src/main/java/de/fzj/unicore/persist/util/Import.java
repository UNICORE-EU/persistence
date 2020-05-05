package de.fzj.unicore.persist.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

import de.fzj.unicore.persist.Persist;
import de.fzj.unicore.persist.PersistenceException;
import de.fzj.unicore.persist.PersistenceFactory;
import de.fzj.unicore.persist.PersistenceProperties;
import de.fzj.unicore.persist.impl.PersistenceDescriptor;
import eu.unicore.util.Log;

/**
 * Exports a database to JSON<br/>
 * 
 * The output is written to the console (so it can be easily
 * redirected to a file)
 * 
 * @author schuller
 */
@SuppressWarnings("rawtypes")
public class Import {

	private Gson gson;
	private Reader input;
	private Class daoClass;
	private Persist output;


	public Import(File inFile, Properties outputConfig)throws Exception{
		setup(outputConfig);
		input=new InputStreamReader(new FileInputStream(inFile));
	}

	public Import(String json, Properties outputConfig)throws Exception{
		setup(outputConfig);
		input=new StringReader(json);
	}
	
	@SuppressWarnings("unchecked")
	private void setup(Properties outputConfig)throws Exception{
		daoClass=Class.forName((String)outputConfig.remove("class"));
		Class inPersistImpl=Class.forName(outputConfig.getProperty("persistence.class"));
		String inTableName=(String)outputConfig.remove("tableName");
		PersistenceDescriptor pdIn=PersistenceDescriptor.get(daoClass);
		if(inTableName!=null){
			pdIn.setTableName(inTableName);
		}
		output=PersistenceFactory.get(new PersistenceProperties(outputConfig)).configurePersist(daoClass, inPersistImpl, pdIn);
		GsonBuilder builder = new GsonBuilder();
		GSONUtil.registerTypeConverters(daoClass, builder);
		gson=builder.create();
	}
	
	@SuppressWarnings("unchecked")
	public void doImport()throws Exception{
		int errors=0;
		int count=0;
		JsonReader reader = new JsonReader(input);
		reader.beginArray();
		while(reader.hasNext()){
			Object o = gson.fromJson(reader, daoClass);
			if(o!=null){
				try{
					output.write(o);
					count++;
				}
				catch(Exception ex){
					errors++;
					System.out.println(Log.createFaultMessage("ERROR:",ex));
				}
			}
		}
		reader.endArray();
		reader.close();
		System.out.println("Done, imported "+count+" elements, "+errors+" errors occured.");
	}

    public void shutdown() throws PersistenceException {
    	output.shutdown();
    }
    
	public Persist<?> getOutput() {
		return output;
	}

	/**
	 * main class, having as arguments:
	 *  1: input file 
	 *  2: output configuration file name
	 */
	public static void main(String[] args)throws Exception {
		System.out.println("*** Import utility *** ");
		String fileName=args[0];

		//setup output database
		Properties outputConfig=new Properties();
		outputConfig.load(new FileInputStream(args[1]));

		Import importer=new Import(new File(fileName), outputConfig);
		importer.doImport();
		importer.shutdown();
	}

}
