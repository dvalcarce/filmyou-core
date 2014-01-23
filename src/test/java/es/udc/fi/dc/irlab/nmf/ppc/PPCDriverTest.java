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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.ppc.util.PPCTestData;
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

	numberOfIterations = 10;

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
	ToolRunner.run(buildConf(H, W), new PPCDriver(), null);

	/* Run asserts */
	compareData(PPCTestData.W_ten, baseDirectory, W);
	compareData(PPCTestData.H_ten, baseDirectory, H);

	deletePreviousData();

    }
}
