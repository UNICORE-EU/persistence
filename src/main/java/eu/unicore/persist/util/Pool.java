package eu.unicore.persist.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * A simple standalone JDBC connection pool manager.
 * <p>
 * The public methods of this class are thread-safe.
 * <p>
 * Home page: <a href="http://www.source-code.biz">www.source-code.biz</a><br>
 * Author: Christian d'Heureuse, Inventec Informatik AG, Zurich, Switzerland<br>
 * Multi-licensed: EPL/LGPL/MPL.
 */
public class Pool {

	private final ConnectionPoolDataSource dataSource;
	private final int maxConnections;
	private final int timeout;
	private final Semaphore semaphore;
	private final Queue<PooledConnection> recycledConnections;
	private int activeConnections;
	private final PoolConnectionEventListener poolConnectionEventListener;
	private boolean isDisposed;

	/**
	 * Constructs a MiniConnectionPoolManager object with a timeout of 60
	 * seconds.
	 * 
	 * @param dataSource
	 *            the data source for the connections.
	 * @param maxConnections
	 *            the maximum number of connections.
	 */
	public Pool(ConnectionPoolDataSource dataSource, int maxConnections) {
		this(dataSource, maxConnections, 60);
	}

	/**
	 * Constructs a MiniConnectionPoolManager object.
	 * 
	 * @param dataSource
	 *            the data source for the connections.
	 * @param maxConnections
	 *            the maximum number of connections.
	 * @param timeout
	 *            the maximum time in seconds to wait for a free connection.
	 */
	public Pool(ConnectionPoolDataSource dataSource, int maxConnections, int timeout) {
		this.dataSource = dataSource;
		this.maxConnections = maxConnections;
		this.timeout = timeout;
		if (maxConnections < 1)
			throw new IllegalArgumentException("Invalid maxConnections value.");
		semaphore = new Semaphore(maxConnections, true);
		recycledConnections = new LinkedList<PooledConnection>();
		poolConnectionEventListener = new PoolConnectionEventListener();
	}

	/**
	 * Closes all unused pooled connections and disposes the pool, after 
	 * which it will become unusable
	 */
	public synchronized void dispose() throws SQLException {
		if (!isDisposed) {
			isDisposed = true;
			SQLException e = null;
			while (!recycledConnections.isEmpty()) {
				try {
					recycledConnections.remove().close();
				} catch (SQLException e2) {
					e = e2;
				}
			}
			if (e!=null)throw e;
		}
	}

	/**
	 * remove all pooled connections, thus ensuring that a call to getConnection() will
	 * create a new connection
	 * @throws SQLException
	 */
	public synchronized void cleanupPooledConnections() throws SQLException {
		if (isDisposed) throw new IllegalStateException("Pool is disposed.");
		SQLException e = null;
		while (!recycledConnections.isEmpty()) {
			try {
				recycledConnections.remove().close();
			} catch (SQLException e2) {
				e = e2;
			}
		}
		if (e!=null)throw e;
	}

	/**
	 * Retrieves a connection from the connection pool. If
	 * <code>maxConnections</code> connections are already in use, the method
	 * waits until a connection becomes available or <code>timeout</code>
	 * seconds elapsed. When the application is finished using the connection,
	 * it must close it in order to return it to the pool.
	 * 
	 * @return a new Connection object.
	 */
	public Connection getConnection() throws SQLException {
		// This routine is unsynchronized, because semaphore.tryAcquire() may
		// block.
		synchronized (this) {
			if (isDisposed)
				throw new IllegalStateException(
						"Connection pool has been disposed.");
		}
		try {
			if (!semaphore.tryAcquire(timeout, TimeUnit.SECONDS))
				throw new SQLException("Timeout while waiting for a free database connection.");
		} catch (InterruptedException e) {
			throw new SQLException(
					"Interrupted while waiting for a database connection.", e);
		}
		boolean ok = false;
		try {
			Connection conn = getConnection2();
			ok = true;
			return conn;
		} finally {
			if (!ok)
				semaphore.release();
		}
	}

	private synchronized Connection getConnection2() throws SQLException {
		if (isDisposed)
			throw new IllegalStateException(
					"Connection pool has been disposed."); // test again with
															// lock
		PooledConnection pconn;
		if (!recycledConnections.isEmpty()) {
			pconn = recycledConnections.remove();
		} else {
			pconn = dataSource.getPooledConnection();
		}
		Connection conn = pconn.getConnection();
		activeConnections++;
		pconn.addConnectionEventListener(poolConnectionEventListener);
		assertInnerState();
		return conn;
	}

	private synchronized void recycleConnection(PooledConnection pconn) {
		if (isDisposed) {
			disposeConnection(pconn);
			return;
		}
		if (activeConnections <= 0)
			throw new AssertionError();
		activeConnections--;
		semaphore.release();
		recycledConnections.add(pconn);
		assertInnerState();
	}

	private synchronized void disposeConnection(PooledConnection pconn) {
		if (activeConnections <= 0)
			throw new AssertionError();
		activeConnections--;
		semaphore.release();
		try {
			pconn.close();
		} catch (Exception e) {}
		assertInnerState();
	}

	private void assertInnerState() {
		if (activeConnections < 0)
			throw new AssertionError();
		if (activeConnections + recycledConnections.size() > maxConnections)
			throw new AssertionError();
		if (activeConnections + semaphore.availablePermits() > maxConnections)
			throw new AssertionError();
	}

	private class PoolConnectionEventListener implements
			ConnectionEventListener {
		public void connectionClosed(ConnectionEvent event) {
			PooledConnection pconn = (PooledConnection) event.getSource();
			pconn.removeConnectionEventListener(this);
			recycleConnection(pconn);
		}

		public void connectionErrorOccurred(ConnectionEvent event) {
			PooledConnection pconn = (PooledConnection) event.getSource();
			pconn.removeConnectionEventListener(this);
			disposeConnection(pconn);
		}
	}

	/**
	 * Returns the number of active (open) connections of this pool. This is the
	 * number of <code>Connection</code> objects that have been issued by
	 * {@link #getConnection()} for which <code>Connection.close()</code> has
	 * not yet been called.
	 * 
	 * @return the number of active connections.
	 **/
	public synchronized int getActiveConnections() {
		return activeConnections;
	}
}
