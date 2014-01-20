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
package es.udc.fi.dc.irlab.rmrecommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

public class RMRecommenderJob extends AbstractJob {

    /**
     * Main routine. Launch RMRecommender job.
     * 
     * @param args
     *            command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new RMRecommenderJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
	// if (ToolRunner.run(new Configuration(), new PPCDriver(), args) < 0) {
	// throw new RuntimeException("PPCJob failed!");
	// }
	// if (ToolRunner.run(new Configuration(), new RMJob(), args) < 0) {
	// throw new RuntimeException("RMJob failed!");
	// }
	return 0;
    }

}
