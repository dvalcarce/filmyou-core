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

import es.udc.fi.dc.irlab.rmrecommender.RMRecommenderDriver;
import es.udc.fi.dc.irlab.testdata.NMFTestData;
import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Integration test for ten iterations of NMF algorithm
 *
 */
public class NMFHDFSDriverTest extends HadoopIntegrationTest {

    @Test
    public void integrationTest() throws Exception {
        final int numberOfUsers = NMFTestData.numberOfUsers;
        final int numberOfItems = NMFTestData.numberOfItems;
        final int numberOfClusters = NMFTestData.numberOfClusters;
        final int numberOfIterations = 10;

        Configuration conf = buildConf();
        HadoopUtils.removeData(conf, conf.get("directory"));

        /* Data initialization */
        final Path H = DataInitialization.createDoubleMatrix(conf, NMFTestData.H_init,
                baseDirectory, "H", 1);
        final Path W = DataInitialization.createDoubleMatrix(conf, NMFTestData.W_init,
                baseDirectory, "W", 1);
        final Path input = DataInitialization.createIntPairFloatFile(conf, NMFTestData.A,
                baseDirectory, "A");

        /* Run job */
        conf = buildConf(H, W, numberOfUsers, numberOfItems, numberOfClusters, numberOfIterations);
        conf.setBoolean(RMRecommenderDriver.useCassandraInput, false);
        conf.setBoolean(RMRecommenderDriver.useCassandraOutput, false);
        conf.set(HadoopUtils.inputPathName, input.toString());
        ToolRunner.run(conf, new NMFDriver(), null);

        /* Run asserts */
        compareIntVectorData(conf, NMFTestData.H_ten, baseDirectory, H);
        compareIntVectorData(conf, NMFTestData.W_ten, baseDirectory, W);

        HadoopUtils.removeData(conf, conf.get("directory"));
    }

}
