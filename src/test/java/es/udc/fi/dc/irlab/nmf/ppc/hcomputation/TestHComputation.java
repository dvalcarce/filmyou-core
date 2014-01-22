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

import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.nmf.util.DataInitialization;
import es.udc.fi.dc.irlab.nmf.util.IntegrationTest;
import es.udc.fi.dc.irlab.nmf.util.NMFTestData;

/**
 * Integration test for one iteration of HComputation (PPC)
 * 
 */
public class TestHComputation extends IntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	Path H = DataInitialization.createMatrix(NMFTestData.H_init,
		baseDirectory, "H", NMFTestData.numberOfUsers,
		NMFTestData.numberOfClusters);
	Path W = DataInitialization.createMatrix(NMFTestData.W_init,
		baseDirectory, "W", NMFTestData.numberOfItems,
		NMFTestData.numberOfClusters);
	Path H2 = new Path(baseDirectory + "/H2");
	Path W2 = new Path(baseDirectory + "/W2");

	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(NMFTestData.A, cassandraKeyspace,
		cassandraTable);

	ToolRunner.run(new Configuration(), new PPCComputeHJob(H, W, H2, W2),
		buildArgs(H, W));
	compareData(NMFTestData.H_one, baseDirectory, new Path(baseDirectory
		+ "/H2"));

	deletePreviousData();

    }

}
