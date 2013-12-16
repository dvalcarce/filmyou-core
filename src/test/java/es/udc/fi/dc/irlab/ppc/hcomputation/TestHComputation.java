package es.udc.fi.dc.irlab.ppc.hcomputation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

import es.udc.fi.dc.irlab.ppc.util.CassandraUtils;
import es.udc.fi.dc.irlab.ppc.util.TestData;

/**
 * Integration test
 * 
 */
public class TestHComputation {

    protected String baseDirectory = "integrationTestH";
    protected int numberOfIterations = 1;
    protected int cassandraPort = 9160;
    protected String cassandraHost = "127.0.0.1";
    protected String cassandraPartitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    protected String cassandraKeyspace = "recommendertest";
    protected String cassandraTable = "ratings";

    protected String[] buildArgs() {
	String[] result = new String[] { "--numberOfUsers",
		String.valueOf(TestData.numberOfUsers), "--numberOfItems",
		String.valueOf(TestData.numberOfItems), "--numberOfClusters",
		String.valueOf(TestData.numberOfClusters),
		"--numberOfIterations", String.valueOf(numberOfIterations),
		"--directory", baseDirectory, "--cassandraPort",
		String.valueOf(cassandraPort), "--cassandraHost",
		cassandraHost, "--cassandraKeyspace", cassandraKeyspace,
		"--cassandraTable", cassandraTable, "--cassandraPartitioner",
		cassandraPartitioner };
	return result;

    }

    protected Path createMatrix(double[][] data, String baseDirectory,
	    String filename, int rows, int cols) throws IOException {

	String uri = baseDirectory + "/" + filename;
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);

	SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
		IntWritable.class, VectorWritable.class);

	Vector vector = new DenseVector(cols);
	int i = 1;
	try {
	    for (double[] row : data) {
		vector.assign(row);
		writer.append(new IntWritable(i), new VectorWritable(vector));
		i++;
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	} finally {
	    IOUtils.closeStream(writer);
	}

	return path;

    }

    protected void deletePreviousData() throws IOException {
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(baseDirectory), conf);
	Path path = new Path(baseDirectory);
	fs.delete(path, true);
    }

    protected void compareData(double[][] data, Path path) throws IOException {
	double accuracy = 0.0001;
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(path.toUri(), conf);
	Path mergedFile = new Path(path.toString() + "/merged");

	FileUtil.copyMerge(fs, path, fs, mergedFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		mergedFile, conf)) {

	    IntWritable key = new IntWritable();
	    VectorWritable val = new VectorWritable();

	    if (reader.next(key, val)) {
		for (Vector.Element element : val.get().all()) {
		    assertEquals(data[key.get() - 1][element.index()],
			    element.get(), accuracy);
		}
	    } else {
		throw new RuntimeException(
			"assertEquals->reader.next() failed!");
	    }
	}
    }

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

	compareData(TestData.H_one, new Path(baseDirectory + "/H2"));

	deletePreviousData();
    }

}