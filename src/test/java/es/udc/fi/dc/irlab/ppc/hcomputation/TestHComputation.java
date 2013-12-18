package es.udc.fi.dc.irlab.ppc.hcomputation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import es.udc.fi.dc.irlab.ppc.util.CassandraUtils;
import es.udc.fi.dc.irlab.ppc.util.IntegrationTest;
import es.udc.fi.dc.irlab.ppc.util.TestData;

/**
 * Integration test
 * 
 */
public class TestHComputation extends IntegrationTest {

    @Test
    public void integrationTest() throws Exception {

	deletePreviousData();

	Path H = createMatrix(TestData.H_init, baseDirectory, "H",
		TestData.numberOfUsers, TestData.numberOfClusters);
	Path W = createMatrix(TestData.W_init, baseDirectory, "W",
		TestData.numberOfItems, TestData.numberOfClusters);

	CassandraUtils cassandraUtils = new CassandraUtils(cassandraHost,
		cassandraPartitioner);
	cassandraUtils
		.insertData(TestData.A, cassandraKeyspace, cassandraTable);

	ToolRunner.run(new Configuration(), new ComputeHJob(H, W), buildArgs());
	compareData(TestData.H_one, new Path(baseDirectory + "/H"));

	deletePreviousData();
    }

}
