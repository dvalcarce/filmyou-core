package es.udc.fi.dc.irlab.util;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class
 * 
 */
public class CassandraSetup {

    private CassandraSetup() {
    };

    /**
     * Update Cassandra settings of jobConf with myConf info.
     * 
     * @param myConf
     *            configuration of the present class
     * @param jobConf
     *            configuration of the job to be launched
     * @return jobConf
     */
    public static Configuration updateConf(final Configuration myConf,
	    final Configuration jobConf) {

	ConfigHelper.setInputRpcPort(jobConf, myConf.get("cassandraPort"));
	ConfigHelper.setInputInitialAddress(jobConf,
		myConf.get("cassandraHost"));
	ConfigHelper.setInputPartitioner(jobConf,
		myConf.get("cassandraPartitioner"));
	ConfigHelper.setInputColumnFamily(jobConf,
		myConf.get("cassandraKeyspace"), myConf.get("cassandraTable"),
		true);
	ConfigHelper.setReadConsistencyLevel(jobConf, "ONE");

	return jobConf;

    }

}
