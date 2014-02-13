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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.testdata.PPCTestData;
import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HDFSUtils;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;

/**
 * Integration test for one iteration of HComputation (PPC)
 * 
 */
public class TestPPCHComputation extends HadoopIntegrationTest {

    @Test
    public void integrationTest() throws Exception {
	int numberOfUsers = PPCTestData.numberOfUsers;
	int numberOfItems = PPCTestData.numberOfItems;
	int numberOfClusters = PPCTestData.numberOfClusters;
	int numberOfIterations = 1;

	Configuration conf = buildConf();
	HDFSUtils.removeData(conf, conf.get("directory"));

	/* Data initialization */
	Path H = DataInitialization.createMatrix(conf, PPCTestData.H_init,
		baseDirectory, "H");
	Path W = DataInitialization.createMatrix(conf, PPCTestData.W_init,
		baseDirectory, "W");
	Path H2 = new Path(baseDirectory + "/H2");
	Path W2 = new Path(baseDirectory + "/W2");

	/* Insert data in Cassandra */
	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(PPCTestData.A, cassandraKeyspace,
		cassandraTableIn);

	/* Run job */
	conf = buildConf(H, W, numberOfUsers, numberOfItems, numberOfClusters,
		numberOfIterations);
	ToolRunner.run(conf, new PPCComputeHJob(H, W, H2, W2), null);

	/* Run asserts */
	compareMatrixData(conf, PPCTestData.H_one, baseDirectory, H2);

	HDFSUtils.removeData(conf, conf.get("directory"));
    }

}
