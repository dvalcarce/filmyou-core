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

package es.udc.fi.dc.irlab.nmf.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.testdata.ClusteringTestData;
import es.udc.fi.dc.irlab.nmf.testdata.NMFTestData;
import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.NMFIntegrationTest;

/**
 * Integration test for cluster assignment.
 * 
 */
public class TestClusterAssignment extends NMFIntegrationTest {

    @Test
    public void integrationTest() throws Exception {
	int numberOfUsers = ClusteringTestData.numberOfUsers;
	int numberOfClusters = ClusteringTestData.numberOfClusters;

	Configuration conf = buildConf();
	deletePreviousData(conf);

	/* Data initialization */
	Path H = DataInitialization.createMatrix(conf, ClusteringTestData.H,
		baseDirectory, "H", numberOfUsers, numberOfClusters);
	Path clustering = new Path(baseDirectory + "/clustering");

	/* Insert data in Cassandra */
	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(NMFTestData.A, cassandraKeyspace,
		cassandraTable);

	/* Run job */
	conf = buildConf(H, clustering, numberOfUsers, numberOfClusters);
	ToolRunner.run(conf, new ClusterAssignmentJob(), null);

	/* Run asserts */
	compareIntSetData(ClusteringTestData.clustering, baseDirectory,
		clustering);

	deletePreviousData(conf);
    }

}
