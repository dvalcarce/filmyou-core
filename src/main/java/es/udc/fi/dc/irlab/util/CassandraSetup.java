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

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;

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
    public static Configuration updateConfForInput(final Configuration myConf,
            final Configuration jobConf) {

        final String host = myConf.get(RMRecommenderDriver.cassandraHost);
        final String keyspace = myConf.get(RMRecommenderDriver.cassandraKeyspace);
        final String tableIn = myConf.get(RMRecommenderDriver.cassandraTableIn);
        final String port = myConf.get(RMRecommenderDriver.cassandraPort);
        final String partitioner = myConf.get(RMRecommenderDriver.cassandraPartitioner);

        ConfigHelper.setInputRpcPort(jobConf, port);
        ConfigHelper.setInputInitialAddress(jobConf, host);
        ConfigHelper.setInputPartitioner(jobConf, partitioner);
        ConfigHelper.setInputColumnFamily(jobConf, keyspace, tableIn, true);
        ConfigHelper.setReadConsistencyLevel(jobConf, "ONE");

        CqlConfigHelper.setInputCQLPageRowSize(jobConf, "1000");

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
    public static Configuration updateConfForOutput(final Configuration myConf,
            final Configuration jobConf) {

        final String host = myConf.get(RMRecommenderDriver.cassandraHost);
        final String keyspace = myConf.get(RMRecommenderDriver.cassandraKeyspace);
        final String tableOut = myConf.get(RMRecommenderDriver.cassandraTableOut);
        final String port = myConf.get(RMRecommenderDriver.cassandraPort);
        final String partitioner = myConf.get(RMRecommenderDriver.cassandraPartitioner);
        final String ttl = myConf.get(RMRecommenderDriver.cassandraTTL);

        ConfigHelper.setOutputRpcPort(jobConf, port);
        ConfigHelper.setOutputInitialAddress(jobConf, host);
        ConfigHelper.setOutputPartitioner(jobConf, partitioner);
        ConfigHelper.setOutputColumnFamily(jobConf, keyspace, tableOut);

        final String query = String.format("UPDATE %s.%s USING TTL %s SET cluster = ?", keyspace,
                tableOut, ttl);
        CqlConfigHelper.setOutputCql(jobConf, query);

        return jobConf;

    }

}
