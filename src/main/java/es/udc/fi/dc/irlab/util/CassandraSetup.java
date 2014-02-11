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
