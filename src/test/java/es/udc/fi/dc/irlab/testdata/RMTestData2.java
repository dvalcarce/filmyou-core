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

package es.udc.fi.dc.irlab.testdata;

/**
 * Contains data for NMF testing purposes.
 * 
 */
public class RMTestData2 {

	public static final int numberOfUsers = 5;
	public static final int numberOfItems = 3;
	public static final int numberOfClusters = 2;

	/**
	 * 3 x 5 ratings matrix (score from 1 to 5, 0 if there is no score).
	 */
	public static final double[][] A = new double[][] { { 5, 0, 0, 1, 2 },
			{ 4, 3, 1, 0, 0 }, { 0, 0, 2, 4, 5 } };

	/**
	 * Sum by user (5 users)
	 */
	public static final double[] userSum = new double[] { 9, 3, 3, 5, 7 };

	/**
	 * Sum by item (3 items)
	 */
	public static final double[] movieSum = new double[] { 8, 8, 11 };

	/**
	 * Global sum
	 */
	public static final double totalSum = 27.0;

	/**
	 * Item probability in collection (3 items)
	 */
	public static final double[] itemColl = new double[] { 0.296296296,
			0.296296296, 0.407407407 };

	/**
	 * User clustering (5 users)
	 */
	public static final int[] clustering = new int[] { 1, 1, 2, 2, 2 };

	public static final int[] clusteringCount = new int[] { 2, 3 };

}
