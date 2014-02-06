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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.nmf.util.CassandraUtils;
import es.udc.fi.dc.irlab.util.HDFSUtils;
import es.udc.fi.dc.irlab.util.HadoopIntegrationTest;

/**
 * Integration test class utility
 * 
 */
public class TestRM2 extends HadoopIntegrationTest {

    @Test
    public void test() throws Exception {
	Configuration conf = buildConf();
	HDFSUtils.removeData(conf, conf.get("directory"));

	String directory = conf.get("directory") + "/rm2";
	Path userSum = new Path(directory + "/userSum");
	Path movieSum = new Path(directory + "/movieSum");
	Path totalSum = new Path(directory + "/totalSum");

	/* Insert data in Cassandra */
	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils.insertData(RMTestData.A, cassandraKeyspace,
		cassandraTable);

	/* Run job */
	ToolRunner.run(conf, new RM2Job(), null);

	/* Run asserts */
	compareVectorData(RMTestData.userSum, baseDirectory, userSum);
	compareVectorData(RMTestData.movieSum, baseDirectory, movieSum);
	compareScalarData(RMTestData.totalSum, baseDirectory, totalSum);

	HDFSUtils.removeData(conf, conf.get("directory"));
    }
}