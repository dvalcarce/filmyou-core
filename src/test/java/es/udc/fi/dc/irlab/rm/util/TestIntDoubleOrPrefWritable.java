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

package es.udc.fi.dc.irlab.rm.util;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test class for {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable}.
 * 
 */
public class TestIntDoubleOrPrefWritable {

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int, int, float)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableIntIntFloat() {
	int userId = 1;
	int itemId = 2;
	float score = 3.0f;

	IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(userId,
		itemId, score);

	assertEquals(userId, test.getUserId());
	assertEquals(itemId, test.getItemId());
	assertEquals(score, test.getScore(), 0.0001);
	assertTrue(test.isPref());

	IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(userId,
		itemId, score);
	assertEquals(test, test2);
	assertEquals(test.hashCode(), test2.hashCode());
    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int, double)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableIntDouble() {
	int key = 1;
	double value = 2.0f;

	IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(key, value);

	assertEquals(key, test.getKey());
	assertEquals(value, test.getValue(), 0.0001);
	assertFalse(test.isPref());

	IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(key, value);
	assertEquals(test, test2);
	assertEquals(test.hashCode(), test2.hashCode());
    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableInt() {
	int key = 1;

	IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(key);

	assertEquals(key, test.getKey());
	assertFalse(test.isPref());

	IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(key);
	assertEquals(test, test2);
	assertEquals(test.hashCode(), test2.hashCode());

    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable#equals(Object)}
     * and {@link es.udc.fi.dc.irlab.rm.util.IntDoubleOrPrefWritable#hashCode()}
     * .
     */
    @Test
    public void testIdentity() {
	int userId = 1;
	int itemId = 2;
	float score = 3.0f;
	int key = 1;
	double value = 2.0f;

	IntDoubleOrPrefWritable test1 = new IntDoubleOrPrefWritable(userId,
		itemId, score);
	IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(userId,
		itemId, score);
	IntDoubleOrPrefWritable test3 = new IntDoubleOrPrefWritable(key, value);

	assertTrue(test1.equals(test2));
	assertFalse(test2.equals(test3));
	assertFalse(test1.equals(test3));
	assertTrue(test1.hashCode() == test2.hashCode());
	assertFalse(test2.hashCode() == test3.hashCode());
	assertFalse(test1.hashCode() == test3.hashCode());
    }

}
