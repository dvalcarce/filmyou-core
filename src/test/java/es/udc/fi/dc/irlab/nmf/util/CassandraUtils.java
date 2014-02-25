/**
 * Copyright 2013 Daniel Valcarce Silva
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.udc.fi.dc.irlab.nmf.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraUtils {

    private String host;
    private Cluster cluster;
    private Session blank_session;
    private Session keyspace_session;

    public CassandraUtils(String host, String partitioner) {

	this.host = host;
	cluster = Cluster.builder().addContactPoint(host).build();
    }

    private Cluster getCluster() {
	// Cluster is singleton
	if (cluster == null) {
	    cluster = Cluster.builder().addContactPoints(host).build();
	}
	return cluster;
    }

    private Session getSession(String keyspace) {
	// Session is singleton
	if (keyspace == null) {
	    if (blank_session == null) {
		blank_session = getCluster().connect();
	    }
	    return blank_session;
	}

	if (keyspace_session == null) {
	    keyspace_session = getCluster().connect(keyspace);
	}

	return keyspace_session;
    }

    public void shutdownSessions() {
	if (blank_session != null) {
	    blank_session.close();
	}

	if (keyspace_session != null) {
	    keyspace_session.close();
	}
    }

    private void createKeyspaceIfNeeded(Session session, String keyspace) {
	if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
	    String cqlStatement = String
		    .format("CREATE KEYSPACE %s WITH REPLICATION ="
			    + "{'class' : 'SimpleStrategy', 'replication_factor': 3};",
			    keyspace);

	    session.execute(cqlStatement);
	}
    }

    private void resetTable(Session session, String keyspace, String table)
	    throws InterruptedException {

	// Drop table if already exists
	if (session.getCluster().getMetadata().getKeyspace(keyspace)
		.getTable(table) != null) {
	    String cqlStatement = String.format("DROP TABLE %s", table);

	    session.execute(cqlStatement);
	}

	// Wait for table deletion
	while (session.getCluster().getMetadata().getKeyspace(keyspace)
		.getTable(table) != null) {
	    Thread.sleep(1000);
	}

	// Create table
	String cqlStatement = String
		.format("CREATE TABLE %s (" + "user int," + "movie int,"
			+ "score float," + "PRIMARY KEY (user, movie));", table);

	session.execute(cqlStatement);

    }

    public void insertData(double[][] data, String keyspace, String table)
	    throws InterruptedException {

	Session session = getSession(null);
	createKeyspaceIfNeeded(session, keyspace);

	session = getSession(keyspace);
	resetTable(session, keyspace, table);

	// Insert data
	for (int i = 0; i < data.length; i++) {
	    for (int j = 0; j < data[i].length; j++) {
		if (data[i][j] == 0.0) {
		    continue;
		}
		String cqlStatement = String
			.format("INSERT INTO %s (user, movie, score) VALUES (%d, %d, %f);",
				table, j + 1, i + 1, data[i][j]);
		session.execute(cqlStatement);
	    }
	}

	shutdownSessions();
	getCluster().close();

    }

    public void initializeTable(String keyspace, String table)
	    throws InterruptedException {

	Session session = getSession(null);
	createKeyspaceIfNeeded(session, keyspace);

	session = getSession(keyspace);

	// Drop table if already exists
	if (session.getCluster().getMetadata().getKeyspace(keyspace)
		.getTable(table) != null) {
	    String cqlStatement = String.format("DROP TABLE %s", table);

	    session.execute(cqlStatement);
	}

	// Wait for table deletion
	while (session.getCluster().getMetadata().getKeyspace(keyspace)
		.getTable(table) != null) {
	    Thread.sleep(1000);
	}

	String cqlStatement = String.format("CREATE TABLE %s" + "(user int,"
		+ "relevance float," + "movie int, cluster int,"
		+ "PRIMARY KEY (user, relevance, movie))"
		+ "WITH CLUSTERING ORDER BY" + "(relevance DESC, movie ASC);",
		table);
	session.execute(cqlStatement);

	shutdownSessions();
	getCluster().close();

    }

    public ResultSet selectData(int user, String keyspace, String table)
	    throws InterruptedException {

	Session session = getSession(keyspace);

	String cqlStatement = String.format(
		"SELECT user, movie, relevance FROM %s WHERE user = %d;",
		table, user);
	return session.execute(cqlStatement);

    }

}
