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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.testdata.PPCTestData;
import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Integration test for one iteration of HComputation (PPC)
 * 
 */
public class TestHDFSPPCHComputation extends HadoopIntegrationTest {

	@Test
	public void integrationTest() throws Exception {
		int numberOfUsers = PPCTestData.numberOfUsers;
		int numberOfItems = PPCTestData.numberOfItems;
		int numberOfClusters = PPCTestData.numberOfClusters;
		int numberOfIterations = 1;

		Configuration conf = buildConf();
		HadoopUtils.removeData(conf, conf.get("directory"));

		/* Data initialization */
		Path H = DataInitialization.createDoubleMatrix(conf,
				PPCTestData.H_init, baseDirectory, "H", 1);
		Path W = DataInitialization.createDoubleMatrix(conf,
				PPCTestData.W_init, baseDirectory, "W", 1);
		Path H2 = new Path(baseDirectory + File.separator + "H2");
		Path W2 = new Path(baseDirectory + File.separator + "W2");
		Path input = DataInitialization.createIntPairFloatFile(conf,
				PPCTestData.A, baseDirectory, "A");

		/* Run job */
		conf = buildConf(H, W, numberOfUsers, numberOfItems, numberOfClusters,
				numberOfIterations);
		conf.setBoolean(RMRecommenderDriver.useCassandraInput, false);
		conf.setBoolean(RMRecommenderDriver.useCassandraOutput, false);
		conf.set(HadoopUtils.inputPathName, input.toString());
		ToolRunner.run(conf, new PPCComputeHJob(H, W, H2, W2), null);

		/* Run asserts */
		compareIntVectorData(conf, PPCTestData.H_one, baseDirectory, H2);

		HadoopUtils.removeData(conf, conf.get("directory"));
	}

}
