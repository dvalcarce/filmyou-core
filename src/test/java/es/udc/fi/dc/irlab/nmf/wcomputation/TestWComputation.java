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

package es.udc.fi.dc.irlab.nmf.wcomputation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.IntegrationTest;
import es.udc.fi.dc.irlab.nmf.util.TestData;
import es.udc.fi.dc.irlab.nmf.wcomputation.ComputeWJob;

/**
 * Integration test for one iteration of WComputation
 * 
 */
public class TestWComputation extends IntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	Path H = DataInitialization.createMatrix(TestData.H_init,
		baseDirectory, "H", TestData.numberOfUsers,
		TestData.numberOfClusters);
	Path W = DataInitialization.createMatrix(TestData.W_init,
		baseDirectory, "W", TestData.numberOfItems,
		TestData.numberOfClusters);
	Path H2 = new Path(baseDirectory + "/H2");
	Path W2 = new Path(baseDirectory + "/W2");

	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils
		.insertData(TestData.A, cassandraKeyspace, cassandraTable);

	ToolRunner.run(new Configuration(), new ComputeWJob(H, W, H2, W2),
		buildArgs(H, W));
	compareData(TestData.W_one, baseDirectory, new Path(baseDirectory
		+ "/W2"));

	deletePreviousData();

    }

}