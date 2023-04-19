package eu.unicore.persist;

public class DataVersionException extends PersistenceException {

	private static final long serialVersionUID = 1L;

	private final int expected;
	
	private final int found;

	public int getExpected() {
		return expected;
	}

	public int getFound() {
		return found;
	}

	public DataVersionException(int expected, int found) {
		super("Unexpected data version found: "+found+", expected "+expected
				+". This commonly occurs when updating to a new server version that is incompatible with the "
				+"version that wrote the data.");
		this.expected = expected;
		this.found = found;
	}
	
}
