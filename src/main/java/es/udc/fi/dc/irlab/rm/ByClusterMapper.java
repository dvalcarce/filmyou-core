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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.IntPairWritable;

/**
 * Abstract mapper for reading clustering data in distributed cached files.
 */
public abstract class ByClusterMapper<A, B> extends
	Mapper<A, B, IntPairWritable, IntDoubleOrPrefWritable> {

    protected Path clustering;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

	if (localFiles.length != 2) {
	    throw new FileNotFoundException(getClass()
		    + ": Missing distributed cache files.");
	}

	clustering = localFiles[0];
    }

}
