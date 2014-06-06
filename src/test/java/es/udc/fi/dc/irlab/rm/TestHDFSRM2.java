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

package es.udc.fi.dc.irlab.rm;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.testdata.ClusteringTestData;
import es.udc.fi.dc.irlab.testdata.RMTestData;
import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Integration test class utility
 * 
 */
public class TestHDFSRM2 extends HadoopIntegrationTest {

	@Test
	public void test() throws Exception {
		Configuration conf = buildConf();

		String baseDirectory = conf.get("directory");
		String directory = conf.get("directory") + "/rm2";

		HadoopUtils.removeData(conf, baseDirectory);

		Path userSum = new Path(directory + File.separator + RM2Job.USER_SUM);
		Path itemColl = new Path(directory + File.separator + RM2Job.ITEMM_COLL);
		DataInitialization.createIntIntFileParent(conf,
				ClusteringTestData.clustering, baseDirectory, "clustering", 1);
		DataInitialization.createIntIntFileParent(conf,
				ClusteringTestData.clusteringCount, baseDirectory,
				"clusteringCount", 0);
		Path input = DataInitialization.createIntPairFloatFile(conf,
				RMTestData.A, baseDirectory, "A");
		Path output = new Path(directory + File.separator + "output");

		/* Run job */
		conf = buildConf("clustering", "clusteringCount",
				RMTestData.numberOfUsers, RMTestData.numberOfItems,
				RMTestData.numberOfClusters);
		conf.setBoolean(RMRecommenderDriver.useCassandraInput, false);
		conf.setBoolean(RMRecommenderDriver.useCassandraOutput, false);
		conf.set(HadoopUtils.inputPathName, input.toString());
		conf.set(HadoopUtils.outputPathName, output.toString());
		ToolRunner.run(conf, new RM2Job(), null);

		// Thread.sleep(20 * 1000);

		/* Run asserts */
		compareIntDoubleData(conf, RMTestData.userSum, baseDirectory, userSum);
		compareMapIntDoubleData(conf, RMTestData.itemColl, baseDirectory,
				itemColl);
		compareIntPairFloatData(conf, RMTestData.recommendations,
				baseDirectory, output);

		HadoopUtils.removeData(conf, baseDirectory);
	}

}
