package eu.unicore.persist.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

import eu.unicore.persist.Persist;
import eu.unicore.persist.PersistenceException;
import eu.unicore.persist.PersistenceFactory;
import eu.unicore.persist.PersistenceProperties;
import eu.unicore.persist.impl.PersistImpl;

/**
 * Exports a database to a JSON file
 *
 * @author schuller
 */
@SuppressWarnings("rawtypes")
public class Export {

	private final Persist input;
	private Gson gson;
	private Writer output;
	Class daoClass;

	public Export(PersistImpl<?> input, String outputFile)throws Exception{
		this.input = input;
		daoClass=input.getDaoClass();
		completeSetup(outputFile);
	}

	@SuppressWarnings("unchecked")
	public Export(Properties inputConfig, String outputFile)throws Exception{
		daoClass = Class.forName((String)inputConfig.remove("class"));
		Class inPersistImpl = Class.forName(inputConfig.getProperty("persistence.class"));
		String inTableName=(String)inputConfig.remove("tableName");
		input = PersistenceFactory.get(new PersistenceProperties(inputConfig)).configurePersist(daoClass, inPersistImpl, inTableName);
		completeSetup(outputFile);
	}

	private void completeSetup(String outputFile) throws FileNotFoundException {
		GsonBuilder builder = new GsonBuilder();
		GSONUtil.registerTypeConverters(daoClass, builder);
		gson = builder.create();
		output = new OutputStreamWriter(new FileOutputStream(outputFile));
	}

    @SuppressWarnings("unchecked")
	public void doExport()throws Exception{
    	JsonWriter writer = new JsonWriter(output);
		int errors=0;
		Collection<String>ids=input.getIDs();
		writer.beginArray();
		int n = 0;
		for(String s: ids){
			System.out.print("Converting "+s+ " ...");
			try{
				gson.toJson(input.read(s), daoClass,writer);
				System.out.println("... OK");
				n++;
			}catch(Exception ex){
				System.out.println("... error: "+ex.getMessage());
				errors++;
			}
		}
		writer.endArray();
		writer.close();
		System.out.println("Done, exported "+n+" entries, "+errors+" errors occured.");
	}

    public void shutdown() throws PersistenceException, SQLException {
    	input.shutdown();
    }

	public Persist<?> getInput() {
		return input;
	}

	/**
	 * Arguments:
	 *  1: input configuration file name
	 *  2: output file name
	 */
	public static void main(String[] args)throws Exception {
		System.out.println("*** Convert utility *** ");
		if(args.length<2){
			System.err.println("Require args: inputConfig outputFilename");
			System.exit(1);
		}
		Properties inputConfig=new Properties();
		inputConfig.load(new FileInputStream(args[0]));
		inputConfig.putAll(System.getProperties());
		Export export=new Export(inputConfig, args[1]);
		export.doExport();
		export.shutdown();
	}
}
