/**
 * Copyright 2014 Daniel Valcarce Silva
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

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.hadoop.conf.Configuration;

/**
 * Utility class for setting up configuration for Cassandra input/output.
 * 
 */
public class CassandraSetup {

    private CassandraSetup() {

    }

    /**
     * Update Cassandra settings of jobConf with myConf info for reading data
     * from Cassandra.
     * 
     * 
     * @param myConf
     *            configuration of the present class
     * @param jobConf
     *            configuration of the job to be launched
     * @return jobConf
     */
    public static Configuration updateConfForInput(Configuration myConf,
	    Configuration jobConf) {

	String host = myConf.get("cassandraHost");
	String keyspace = myConf.get("cassandraKeyspace");
	String tableIn = myConf.get("cassandraTableIn");
	String port = myConf.get("cassandraPort");
	String partitioner = myConf.get("cassandraPartitioner");

	ConfigHelper.setInputRpcPort(jobConf, port);
	ConfigHelper.setInputInitialAddress(jobConf, host);
	ConfigHelper.setInputPartitioner(jobConf, partitioner);
	ConfigHelper.setInputColumnFamily(jobConf, keyspace, tableIn, true);
	ConfigHelper.setReadConsistencyLevel(jobConf, "ONE");

	return jobConf;

    }

    /**
     * Update Cassandra settings of jobConf with myConf info for writing data to
     * Cassandra.
     * 
     * @param myConf
     *            configuration of the present class
     * @param jobConf
     *            configuration of the job to be launched
     * @return jobConf
     */
    public static Configuration updateConfForOutput(Configuration myConf,
	    Configuration jobConf) {

	String host = myConf.get("cassandraHost");
	String keyspace = myConf.get("cassandraKeyspace");
	String tableOut = myConf.get("cassandraTableOut");
	String port = myConf.get("cassandraPort");
	String partitioner = myConf.get("cassandraPartitioner");
	String ttl = myConf.get("cassandraTTL");

	ConfigHelper.setOutputRpcPort(jobConf, port);
	ConfigHelper.setOutputInitialAddress(jobConf, host);
	ConfigHelper.setOutputPartitioner(jobConf, partitioner);
	ConfigHelper.setOutputColumnFamily(jobConf, keyspace, tableOut);

	String query = String.format(
		"UPDATE %s.%s USING TTL %s SET cluster = ?", keyspace,
		tableOut, ttl);
	CqlConfigHelper.setOutputCql(jobConf, query);

	return jobConf;

    }

}
