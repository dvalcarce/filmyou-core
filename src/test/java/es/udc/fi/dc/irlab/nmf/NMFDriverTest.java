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

package es.udc.fi.dc.irlab.nmf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.IntegrationTest;
import es.udc.fi.dc.irlab.nmf.util.NMFTestData;

/**
 * Integration test for ten iterations of NMF algorithm
 * 
 */
public class NMFDriverTest extends IntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	numberOfIterations = 10;

	Path H = DataInitialization.createMatrix(NMFTestData.H_init,
		baseDirectory, "H", NMFTestData.numberOfUsers,
		NMFTestData.numberOfClusters);
	Path W = DataInitialization.createMatrix(NMFTestData.W_init,
		baseDirectory, "W", NMFTestData.numberOfItems,
		NMFTestData.numberOfClusters);

	/* Insert data in Cassandra */
	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils
		.insertData(NMFTestData.A, cassandraKeyspace, cassandraTable);

	/* Run job */
	ToolRunner.run(new Configuration(), new NMFDriver(), buildArgs(H, W));

	/* Run asserts */
	compareData(NMFTestData.H_ten, baseDirectory, H);
	compareData(NMFTestData.W_ten, baseDirectory, W);

	deletePreviousData();

    }
}
