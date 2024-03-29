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

import es.udc.fi.dc.irlab.testdata.ClusteringTestData;
import es.udc.fi.dc.irlab.testdata.RMTestData;
import es.udc.fi.dc.irlab.util.CassandraUtils;
import es.udc.fi.dc.irlab.util.DataInitialization;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;
import es.udc.fi.dc.irlab.util.HadoopUtils;

/**
 * Integration test class utility
 *
 */
public class TestCassandraRM2 extends HadoopIntegrationTest {

    @Test
    public void test() throws Exception {
        Configuration conf = buildConf();

        final String baseDirectory = conf.get("directory");
        final String directory = conf.get("directory") + "/rm2";

        HadoopUtils.removeData(conf, baseDirectory);

        final Path userSum = new Path(directory + File.separator + RM2Job.USER_SUM);
        final Path itemColl = new Path(directory + File.separator + RM2Job.ITEMM_COLL);
        DataInitialization.createIntIntFileParent(conf, ClusteringTestData.clustering,
                baseDirectory, "clustering", 1);
        DataInitialization.createIntIntFileParent(conf, ClusteringTestData.clusteringCount,
                baseDirectory, "clusteringCount", 0);

        /* Insert data in Cassandra */
        CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost, cassandraPartitioner);
        cassandraUtils.insertData(RMTestData.A, cassandraKeyspace, cassandraTableIn);
        cassandraUtils = new CassandraUtils(cassandraHost, cassandraPartitioner);
        cassandraUtils.initializeTable(cassandraKeyspace, cassandraTableOut);

        /* Run job */
        conf = buildConf("clustering", "clusteringCount", RMTestData.numberOfUsers,
                RMTestData.numberOfItems, RMTestData.numberOfClusters);
        ToolRunner.run(conf, new RM2Job(), null);

        /* Run asserts */
        compareIntDoubleData(conf, RMTestData.userSum, baseDirectory, userSum);
        compareMapIntDoubleData(conf, RMTestData.itemColl, baseDirectory, itemColl);
        compareCassandraData(conf, RMTestData.recommendations, RMTestData.numberOfUsers);

        HadoopUtils.removeData(conf, baseDirectory);
    }

}
