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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Emit <i, y_i> from <i, w_i> where y_i = w_i·C
 * 
 */
public class W4Mapper extends
	Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

    private Matrix C;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
	if (localFiles.length == 0) {
	    throw new FileNotFoundException(getClass()
		    + ": Distributed cache file not found.");
	}
	Path directory = localFiles[0];
	FileSystem fs = FileSystem.get(directory.toUri(), conf);
	Path matrixFile = new Path(directory.toString() + "/C");

	FileUtil.copyMerge(fs, directory, fs, matrixFile, false, conf, null);

	try (SequenceFile.Reader reader = new SequenceFile.Reader(fs,
		matrixFile, conf)) {

	    NullWritable key = NullWritable.get();
	    MatrixWritable val = new MatrixWritable();

	    if (reader.next(key, val)) {
		C = val.get();
	    } else {
		throw new FileNotFoundException(getClass()
			+ ": Invalid distributed cache file.");
	    }

	}
    }

    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
	    throws IOException, InterruptedException {

	Vector vector = value.get();
	Matrix mVector = new DenseMatrix(1, vector.size());

	for (int j = 0; j < vector.size(); j++) {
	    mVector.set(0, j, vector.get(j));
	}

	context.write(key, new VectorWritable(mVector.times(C).viewRow(0)));

    }
}