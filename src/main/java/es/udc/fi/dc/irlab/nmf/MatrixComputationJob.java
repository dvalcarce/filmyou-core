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

package es.udc.fi.dc.irlab.nmf;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;

/**
 * Abstraction of a matrix computation job for NMF or PPC.
 * 
 */
public abstract class MatrixComputationJob extends AbstractJob {

    public static final String cname = "mergedC";

    protected String directory;
    protected int iteration;

    protected Path H;
    protected Path W;
    protected Path H2;
    protected Path W2;
    protected Path out1;
    protected Path X;
    protected Path C;
    protected Path Y;

    /**
     * MatrixComputationJob constructor.
     * 
     * @param H
     * @param W
     * @param H2
     * @param W2
     */
    public MatrixComputationJob(Path H, Path W, Path H2, Path W2) {
	super();
	this.H = H;
	this.W = W;
	this.H2 = H2;
	this.W2 = W2;
	iteration = getConf().getInt("iteration", -1);
    }

}
