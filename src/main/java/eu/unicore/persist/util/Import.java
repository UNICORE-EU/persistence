package eu.unicore.persist.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceFactory;
import eu.unicore.persist.PersistenceProperties;

/**
 * Imports a database from a JSON file
 *
 * @author schuller
 */
@SuppressWarnings("rawtypes")
public class Import {

	private final Gson gson;
	private final Reader input;
	private final Class daoClass;
	private final Persist output;

	public Import(File inFile, Properties outputConfig)throws Exception{
		this(new InputStreamReader(new FileInputStream(inFile)), outputConfig);
	}

	public Import(String json, Properties outputConfig)throws Exception{
		this(new StringReader(json), outputConfig);
	}
	
	@SuppressWarnings("unchecked")
	private Import(Reader reader, Properties outputConfig) throws Exception {
		this.input = reader;
		daoClass = Class.forName((String)outputConfig.remove("class"));
		Class inPersistImpl = Class.forName(outputConfig.getProperty("persistence.class"));
		String inTableName=(String)outputConfig.remove("tableName");
		output=PersistenceFactory.get(new PersistenceProperties(outputConfig)).configurePersist(daoClass, inPersistImpl, inTableName);
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
					System.out.println("ERROR: " + ex);
				}
			}
		}
		reader.endArray();
		reader.close();
		System.out.println("Done, imported "+count+" elements, "+errors+" errors occured.");
	}

    public void shutdown() throws PersistenceException, SQLException {
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
