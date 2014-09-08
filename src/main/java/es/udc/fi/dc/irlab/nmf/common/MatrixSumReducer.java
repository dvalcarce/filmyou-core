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

package es.udc.fi.dc.irlab.nmf.common;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixWritable;

import es.udc.fi.dc.irlab.util.MahoutMathUtils;

/**
 * Emit &lt;i, sum_i(matrix_i)> from &lt;NULL, matrix_i>.
 * 
 */
public class MatrixSumReducer extends
		Reducer<NullWritable, MatrixWritable, NullWritable, MatrixWritable> {

	@Override
	protected void reduce(NullWritable key, Iterable<MatrixWritable> values,
			Context context) throws IOException, InterruptedException {

		Iterator<MatrixWritable> it = values.iterator();
		Matrix output = it.next().get();

		while (it.hasNext()) {
			MahoutMathUtils.matrixAddInPlace(output, it.next().get());
		}

		context.write(NullWritable.get(), new MatrixWritable(output));

	}

}
