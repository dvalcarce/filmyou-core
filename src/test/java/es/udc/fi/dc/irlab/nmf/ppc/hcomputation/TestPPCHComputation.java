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

package es.udc.fi.dc.irlab.nmf.ppc.hcomputation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.ppc.util.PPCTestData;
import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.NMFIntegrationTest;
import es.udc.fi.dc.irlab.nmf.util.NMFTestData;

/**
 * Integration test for one iteration of HComputation (PPC)
 * 
 */
public class TestPPCHComputation extends NMFIntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	Path H = DataInitialization.createMatrix(PPCTestData.H_init,
		baseDirectory, "H", PPCTestData.numberOfUsers,
		NMFTestData.numberOfClusters);
	Path W = DataInitialization.createMatrix(PPCTestData.W_init,
		baseDirectory, "W", PPCTestData.numberOfItems,
		NMFTestData.numberOfClusters);
	Path H2 = new Path(baseDirectory + "/H2");
	Path W2 = new Path(baseDirectory + "/W2");

	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(PPCTestData.A, cassandraKeyspace,
		cassandraTable);

	numberOfUsers = PPCTestData.numberOfUsers;
	numberOfItems = PPCTestData.numberOfItems;
	numberOfClusters = PPCTestData.numberOfClusters;
	numberOfIterations = 1;

	ToolRunner.run(buildConf(H, W), new PPCComputeHJob(H, W, H2, W2), null);
	compareData(PPCTestData.H_one, baseDirectory, new Path(baseDirectory
		+ "/H2"));

	deletePreviousData();

    }

}
