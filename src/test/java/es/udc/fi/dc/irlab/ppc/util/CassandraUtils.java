package es.udc.fi.dc.irlab.ppc.util;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraUtils {

    private String host;
    private Cluster cluster;

    public CassandraUtils(String host, String partitioner) {

	this.host = host;
	cluster = Cluster.builder().addContactPoint(host).build();
    }

    private Cluster getCluster() {
	if (cluster == null) {
	    cluster = Cluster.builder().addContactPoints(host).build();
	}
	return cluster;
    }

    private void createKeyspaceIfNeeded(Session session, String keyspace) {

	if (session.getCluster().getMetadata().getKeyspace(keyspace) == null) {
	    String cqlStatement = String
		    .format("CREATE KEYSPACE %s WITH REPLICATION ="
			    + "{'class' : 'SimpleStrategy', 'replication_factor': 1};",
			    keyspace);

	    session.execute(cqlStatement);
	}

    }

    private void createTableIfNeeded(Session session, String keyspace,
	    String table) {

	if (session.getCluster().getMetadata().getKeyspace(keyspace)
		.getTable(table) == null) {
	    String cqlStatement = String.format("CREATE TABLE %s ("
		    + "user int," + "movie int," + "score float,"
		    + "PRIMARY KEY (user, movie));", table);

	    session.execute(cqlStatement);
	}

    }

    public void insertData(double[][] data, String keyspace, String table) {
	Session session = getCluster().connect();

	createKeyspaceIfNeeded(session, keyspace);

	session = getCluster().connect(keyspace);

	createTableIfNeeded(session, keyspace, table);

	for (int i = 0; i < data.length; i++) {
	    for (int j = 0; j < data[i].length; j++) {
		String cqlStatement = String
			.format("INSERT INTO %s (user, movie, score) VALUES (%d, %d, %f);",
				table, j+1, i+1, data[i][j]);

		session.execute(cqlStatement);
	    }
	}
    }

}
