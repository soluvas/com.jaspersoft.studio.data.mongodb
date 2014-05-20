/*******************************************************************************
 * Copyright (C) 2010 - 2013 Jaspersoft Corporation. All rights reserved.
 * http://www.jaspersoft.com
 * 
 * Unless you have purchased a commercial license agreement from Jaspersoft, 
 * the following license terms apply:
 * 
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Jaspersoft Studio Team - initial API and implementation
 ******************************************************************************/
package com.jaspersoft.mongodb.connection;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

import net.sf.jasperreports.engine.JRException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * 
 * @author Eric Diaz
 * 
 */
public class MongoDbConnection implements Connection {
	private MongoClient client;

	private final String mongoURI;
	private final String username;
	private final String password;
	private String mongoDatabaseName;

	private DB mongoDatabase;

	private static final Logger logger = LoggerFactory.getLogger(MongoDbConnection.class);

	protected static final Set<Integer> AUTH_ERROR_CODES = new HashSet<Integer>(Arrays.asList(new Integer[] {
			16550, // not authorized for query on foo.system.namespaces
			10057, // unauthorized db:admin ns:admin.system.users lock type:1 client:127.0.0.1
			15845, // unauthorized
			13     // MongoDB 2.6.0: not authorized on DB to execute command { count: \"system.namespaces\", query: {}, fields: {} }
	}));
	
	/**
	 * @param mongoURI URI (can include username and password for the connection).
	 * @param username If not {@code null}, will override the username in mongoURI.
	 * @param password If not {@code null}, will override the password in mongoURI.
	 * @throws JRException
	 */
	public MongoDbConnection(String mongoURI, String username, String password)
			throws JRException {
		this.mongoURI = mongoURI;
		this.username = username;
		this.password = password;
		create(mongoURI);
		setDatabase();
	}

	private void create(String mongoURI) throws JRException {
		close();
		
		final MongoClientURI origMongoUri = new MongoClientURI(mongoURI);
		String uriWithoutDbStr = "mongodb://";
		final String theUsername = this.username != null ? this.username : origMongoUri.getUsername();
		if (theUsername != null) {
			// MongoDB passwords are never empty
			final String thePassword = this.password != null ? this.password : String.valueOf(origMongoUri.getPassword());
			try {
				uriWithoutDbStr += URLEncoder.encode(theUsername, "UTF-8") +
						":" + URLEncoder.encode(thePassword, "UTF-8"); 
				uriWithoutDbStr += "@";
			} catch (UnsupportedEncodingException e) {
				throw new JRException("Invalid Mongo URI: " + e, e);
			}
		}
		for (int i = 0; i < origMongoUri.getHosts().size(); i++) {
			if (i > 0) {
				uriWithoutDbStr += ","; 
			}
			uriWithoutDbStr += origMongoUri.getHosts().get(i);
		}
		uriWithoutDbStr += "/";
		
		try {
			logger.debug("Connecting to {} as {} to query database '{}'",
					origMongoUri.getHosts(), theUsername, origMongoUri.getDatabase()); 
			client = new MongoClient(new MongoClientURI(uriWithoutDbStr));
			mongoDatabaseName = origMongoUri.getDatabase();
		} catch (Exception e) {
			logger.error("Cannot create connection", e);
			throw new JRException("Cannot create connection: " + e, e);
		}
	}

	private void setDatabase() throws JRException {
		if (client == null) {
			logger.error("No client");
			return;
		}
		if (mongoDatabase != null) {
			return;
		}
		mongoDatabase = client.getDB(mongoDatabaseName);
	}

	public DB getMongoDatabase() {
		return mongoDatabase;
	}

	public String getMongoURI() {
		return mongoURI;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	@Override
	public void close() {
		mongoDatabaseName = null;
		if (client != null) {
			client.close();
			client = null;
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		if (client != null) {
			return false;
		}
		return true;
	}

	public Mongo getClient() {
		return client;
	}

	public String test() throws JRException {
		if (mongoDatabaseName == null) {
			throw new JRException("Invalid Mongo URI");
		}

		if (mongoDatabase == null) {
			throw new JRException("No mongo database");
		}

		try {
			return "Connection test successful.\n" + "Mongo database name: "
					+ mongoDatabase.getName();
		} catch (Exception e) {
			logger.error("Cannot test connection", e);
			throw new JRException("Cannot test connection: " + e, e);
		}
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return null;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return null;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return false;
	}

	@Override
	public void commit() throws SQLException {
	}

	@Override
	public void rollback() throws SQLException {
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return null;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
	}

	@Override
	public String getCatalog() throws SQLException {
		return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return 0;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return null;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return null;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
	}

	@Override
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return null;
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return null;
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return null;
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return false;
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return null;
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return null;
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return null;
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
}
