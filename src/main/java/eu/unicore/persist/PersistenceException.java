package eu.unicore.persist;

/**
 * generic exception thrown by the persistence layer
 *  
 * @author schuller
 */
public class PersistenceException extends Exception {
	
	private static final long serialVersionUID = 1L;
	
	public PersistenceException() {
	}

	public PersistenceException(String message) {
		super(message);
	}

	public PersistenceException(Throwable cause) {
		super(cause);
	}

	public PersistenceException(String message, Throwable cause) {
		super(message, cause);
	}

}
