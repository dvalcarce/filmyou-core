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

package es.udc.fi.dc.irlab.nmf.ppc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.testdata.PPCTestData;
import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.NMFIntegrationTest;

/**
 * Integration test for ten iterations of PPC algorithm
 * 
 */
public class PPCDriverTest extends NMFIntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	int numberOfUsers = PPCTestData.numberOfUsers;
	int numberOfItems = PPCTestData.numberOfItems;
	int numberOfClusters = PPCTestData.numberOfClusters;
	int numberOfIterations = 10;

	Path H = DataInitialization.createMatrix(PPCTestData.H_init,
		baseDirectory, "H", PPCTestData.numberOfUsers,
		PPCTestData.numberOfClusters);
	Path W = DataInitialization.createMatrix(PPCTestData.W_init,
		baseDirectory, "W", PPCTestData.numberOfItems,
		PPCTestData.numberOfClusters);

	/* Insert data in Cassandra */
	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(PPCTestData.A, cassandraKeyspace,
		cassandraTable);

	/* Run job */
	Configuration conf = buildConf(H, W, numberOfUsers, numberOfItems,
		numberOfClusters, numberOfIterations);

	ToolRunner.run(conf, new PPCDriver(), null);

	/* Run asserts */
	compareVectorData(PPCTestData.H_ten, baseDirectory, H);
	compareVectorData(PPCTestData.W_ten, baseDirectory, W);

	deletePreviousData();

    }

}
