package de.fzj.unicore.persist.impl;

import java.sql.SQLException;
import java.util.List;

import de.fzj.unicore.persist.PersistenceException;

/**
 * SQL statements
 * 
 * @author schuller
 */
public abstract class SQL<T> extends Base<T>{

	protected abstract List<String> getSQLCreateTable() throws PersistenceException, SQLException;
	
	protected String getSQLStringType(){
		return "VARCHAR";
	}

	protected String getSQLDropTable(){
		return "DROP TABLE "+pd.getTableName()+";";
	}

	protected String getSQLExists(String id){
		return "SELECT id FROM "+pd.getTableName()+" WHERE id='"+id+"'"+";";
	}

	protected String getSQLRead(){
		return "SELECT data FROM "+pd.getTableName()+" WHERE id=? ;";
	}

	protected String getSQLInsert(){
		if(pd.getColumns().size()>0){
			StringBuffer columns=new StringBuffer();
			StringBuffer columnValues=new StringBuffer();
			for(ColumnDescriptor c: pd.getColumns()){
				columns.append(","+c.getColumn());
				columnValues.append(",?");
			}
			return "INSERT INTO "+pd.getTableName()+" (id,data"+columns.toString()+") " +
			"VALUES (?,?"+columnValues.toString()+");";
		}
		else{
			return "INSERT INTO "+pd.getTableName()+" (id,data) VALUES (?,?);";
		}
	}

	protected String getSQLUpdate(){
		if(pd.getColumns().size()>0){
			StringBuffer columns=new StringBuffer();
			for(ColumnDescriptor c: pd.getColumns()){
				columns.append(","+c.getColumn()+"=?");
			}
			return "UPDATE "+pd.getTableName()+" SET data =?"+columns.toString()+
					" WHERE id=? ;";
		}
		else{
			return "UPDATE "+pd.getTableName()+" SET data=? WHERE id=? ;";
		}
	}

	protected String getSQLDelete(String id){
		return "DELETE FROM "+pd.getTableName()+" WHERE id='"+id+"';";
	}

	protected String getSQLDeleteAll(){
		return "DELETE FROM "+pd.getTableName()+";";
	}

	protected String getSQLSelectAllKeys(){
		return "SELECT ID FROM "+pd.getTableName()+";";
	}

	protected String getSQLSelectKeys(String column, Object value){
		return "SELECT ID FROM "+pd.getTableName()+" WHERE "+column+"=?;";
	}

	protected String getSQLFuzzySelect(String column, int numValues, boolean orMode){
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ID FROM ").append(pd.getTableName());
		sb.append(" WHERE ");
		boolean add = false;
		for(int i=0; i<numValues; i++){
			if(add){
				sb.append( orMode? " OR " : " AND " );
			}
			add = true;
			sb.append(column).append(" LIKE ?");
		}
		sb.append(";");
		return sb.toString();
	}
	
	protected String getSQLSelectColumn(String column){
		return "SELECT ID,"+column+" FROM "+pd.getTableName()+";";
	}

	protected String getSQLRowCount(){
		return "SELECT COUNT(ID) FROM "+pd.getTableName()+";";
	}

	protected String getSQLRowCount(String column, Object value){
		return "SELECT COUNT(ID) FROM "+pd.getTableName()+" WHERE "+column+"='"+value.toString()+
				"';";
	}

	protected String getSQLShutdown(){
		return null;
	}

}
