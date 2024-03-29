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

package es.udc.fi.dc.irlab.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test class for {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable}.
 *
 */
public class TestIntDoubleOrPrefWritable {

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int, int, float)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableIntIntFloat() {
        final int userId = 1;
        final int itemId = 2;
        final float score = 3.0f;

        final IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(userId, itemId, score);

        assertEquals(userId, test.getUserId());
        assertEquals(itemId, test.getItemId());
        assertEquals(score, test.getScore(), 0.0001);
        assertTrue(test.isPref());

        final IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(userId, itemId, score);
        assertEquals(test, test2);
        assertEquals(test.hashCode(), test2.hashCode());
    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int, double)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableIntDouble() {
        final int key = 1;
        final double value = 2.0f;

        final IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(key, value);

        assertEquals(key, test.getKey());
        assertEquals(value, test.getValue(), 0.0001);
        assertFalse(test.isPref());

        final IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(key, value);
        assertEquals(test, test2);
        assertEquals(test.hashCode(), test2.hashCode());
    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable#IntDoubleOrPrefWritable(int)}
     * .
     */
    @Test
    public void testIntDoubleOrPrefWritableInt() {
        final int key = 1;

        final IntDoubleOrPrefWritable test = new IntDoubleOrPrefWritable(key);

        assertEquals(key, test.getKey());
        assertFalse(test.isPref());

        final IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(key);
        assertEquals(test, test2);
        assertEquals(test.hashCode(), test2.hashCode());

    }

    /**
     * Test method for
     * {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable#equals(Object)}
     * and {@link es.udc.fi.dc.irlab.util.IntDoubleOrPrefWritable#hashCode()} .
     */
    @Test
    public void testIdentity() {
        final int userId = 1;
        final int itemId = 2;
        final float score = 3.0f;
        final int key = 1;
        final double value = 2.0f;

        final IntDoubleOrPrefWritable test1 = new IntDoubleOrPrefWritable(userId, itemId, score);
        final IntDoubleOrPrefWritable test2 = new IntDoubleOrPrefWritable(userId, itemId, score);
        final IntDoubleOrPrefWritable test3 = new IntDoubleOrPrefWritable(key, value);

        assertTrue(test1.equals(test2));
        assertFalse(test2.equals(test3));
        assertFalse(test1.equals(test3));
        assertTrue(test1.hashCode() == test2.hashCode());
        assertFalse(test2.hashCode() == test3.hashCode());
        assertFalse(test1.hashCode() == test3.hashCode());
    }

}
