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

package es.udc.fi.dc.irlab.util;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraUtils {

    private final String host;
    private Cluster cluster;
    private Session blank_session;
    private Session keyspace_session;

    public CassandraUtils(final String host, final String partitioner) {

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

    private Session getSession(final String keyspace) {
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

    private void createKeyspaceIfNeeded(final Session session, final String keyspace) {
        if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
            session.execute("CREATE KEYSPACE " + keyspace + " WITH REPLICATION = "
                    + "{'class' : 'SimpleStrategy', 'replication_factor': 3}");
        }
    }

    private void resetTable(final Session session, final String keyspace, final String table)
            throws InterruptedException {

        // Drop table if already exists
        if (session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table) != null) {
            session.execute("DROP TABLE " + table);
        }

        // Wait for table deletion
        while (session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table) != null) {
            Thread.sleep(1000);
        }

        // Create table
        session.execute("CREATE TABLE " + table + " (user int, item int, score float,"
                + "PRIMARY KEY (user, item))");

    }

    public void insertData(final double[][] data, final String keyspace, final String table)
            throws InterruptedException {

        Session session = getSession(null);
        createKeyspaceIfNeeded(session, keyspace);

        session = getSession(keyspace);
        resetTable(session, keyspace, table);

        final PreparedStatement statement = session
                .prepare("INSERT INTO " + table + " (user, item, score) VALUES (?, ?, ?)");
        final BoundStatement boundStatement = new BoundStatement(statement);

        // Insert data
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                if (data[i][j] == 0.0) {
                    continue;
                }
                session.execute(boundStatement.bind(j + 1, i + 1, (float) data[i][j]));
            }
        }

        shutdownSessions();
        getCluster().close();

    }

    public void initializeTable(final String keyspace, final String table)
            throws InterruptedException {

        Session session = getSession(null);
        createKeyspaceIfNeeded(session, keyspace);

        session = getSession(keyspace);

        // Drop table if already exists
        if (session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table) != null) {
            session.execute("DROP TABLE " + table);
        }

        // Wait for table deletion
        while (session.getCluster().getMetadata().getKeyspace(keyspace).getTable(table) != null) {
            Thread.sleep(1000);
        }

        session.execute(
                "CREATE TABLE " + table + " (user int, relevance float, item int, cluster int,"
                        + "PRIMARY KEY (user, relevance, item))"
                        + "WITH CLUSTERING ORDER BY (relevance DESC, item ASC)");

        shutdownSessions();
        getCluster().close();

    }

    public ResultSet selectData(final int user, final String keyspace, final String table)
            throws InterruptedException {

        final Session session = getSession(keyspace);

        return session
                .execute("SELECT user, item, relevance FROM " + table + " WHERE user = " + user);

    }

}
