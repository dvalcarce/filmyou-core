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

package es.udc.fi.dc.irlab.nmf.hcomputation;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.testdata.NMFTestData;
import es.udc.fi.dc.irlab.util.CassandraUtils;
import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Integration test for one iteration of HComputation (NMF)
 * 
 */
public class TestCassandraHComputation extends HadoopIntegrationTest {

	@Test
	public void integrationTest() throws Exception {
		int numberOfUsers = NMFTestData.numberOfUsers;
		int numberOfItems = NMFTestData.numberOfItems;
		int numberOfClusters = NMFTestData.numberOfClusters;
		int numberOfIterations = 1;

		Configuration conf = buildConf();
		HadoopUtils.removeData(conf, conf.get("directory"));

		/* Data initialization */
		Path H = DataInitialization.createDoubleMatrix(conf,
				NMFTestData.H_init, baseDirectory, "H", 1);
		Path W = DataInitialization.createDoubleMatrix(conf,
				NMFTestData.W_init, baseDirectory, "W", 1);
		Path H2 = new Path(baseDirectory + File.separator + "H2");
		Path W2 = new Path(baseDirectory + File.separator + "W2");

		/* Insert data in Cassandra */
		CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
				cassandraPartitioner);
		cassandraUtils.insertData(NMFTestData.A, cassandraKeyspace,
				cassandraTableIn);

		/* Run job */
		conf = buildConf(H, W, numberOfUsers, numberOfItems, numberOfClusters,
				numberOfIterations);
		ToolRunner.run(conf, new ComputeHJob(H, W, H2, W2), null);

		/* Run asserts */
		compareIntVectorData(conf, NMFTestData.H_one, baseDirectory, H2);

		HadoopUtils.removeData(conf, conf.get("directory"));
	}

}
