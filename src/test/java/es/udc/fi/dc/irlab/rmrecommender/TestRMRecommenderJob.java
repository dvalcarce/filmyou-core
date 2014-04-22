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

package es.udc.fi.dc.irlab.rmrecommender;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test class for {@link es.udc.fi.dc.irlab.rmrecommender.TestRMRecommenderJob}.
 * 
 */
public class TestRMRecommenderJob {

	/**
	 * Test method for
	 * {@link es.udc.fi.dc.irlab.rmrecommender.TestRMRecommenderJob#parseInput(String[])}
	 * .
	 * 
	 * @throws IOException
	 */
	@Test
	public void testParseInput() throws IOException {
		RMRecommenderDriver job = new RMRecommenderDriver();

		String[] args = { "--cassandraHost", "127.0.0.1",
				"--cassandraKeyspace", "recommendertest",
				"--cassandraPartitioner",
				"org.apache.cassandra.dht.Murmur3Partitioner",
				"--cassandraPort", "9160", "--cassandraTTL", "86400",
				"--cassandraTableIn", "ratings", "--cassandraTableOut",
				"recommendations", "--clustering", "clustering",
				"--clusteringCount", "clusteringCount", "--directory",
				"recommendation", "--lambda", "0.5", "--numberOfClusters",
				"50", "--numberOfItems", "17770", "--numberOfIterations", "1",
				"--numberOfUsers", "480189", "--useCassandra", "false" };

		Configuration conf = job.parseInput(args);
		for (int i = 0; i < args.length; i += 2) {
			assertEquals(args[i + 1], conf.get(args[i].substring(2)));
		}

	}

	/**
	 * Test method for
	 * {@link es.udc.fi.dc.irlab.rmrecommender.TestRMRecommenderJob#parseInput(String[])}
	 * .
	 * 
	 * @throws IOException
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testParseEmptyInput() throws IOException {
		RMRecommenderDriver job = new RMRecommenderDriver();

		String[] args = {};
		job.parseInput(args);

	}

}
