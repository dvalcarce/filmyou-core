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

package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.VarLongWritable;

/**
 * Reads ratings data from MySQL database and outputs to Hadoop environment.
 * 
 */
public class BaselineToItemPrefsMySQLMapper extends
		Mapper<LongWritable, MySQLRecord, VarLongWritable, VarLongWritable> {

	public static final String RATING_SHIFT = BaselineToItemPrefsMySQLMapper.class
			+ "shiftRatings";

	private boolean booleanData;
	private float ratingShift;

	@Override
	public void map(LongWritable key, MySQLRecord val, Context context)
			throws IOException, InterruptedException {

		if (booleanData) {
			context.write(new VarLongWritable(), new VarLongWritable(val.item));
		} else {
			float prefValue = val.score + ratingShift;
			context.write(new VarLongWritable(val.user),
					new EntityPrefWritable(val.item, prefValue));
		}

	}

	@Override
	protected void setup(Context context) {
		Configuration jobConf = context.getConfiguration();
		booleanData = jobConf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
		ratingShift = Float.parseFloat(jobConf.get(RATING_SHIFT, "0.0"));
	}

}
