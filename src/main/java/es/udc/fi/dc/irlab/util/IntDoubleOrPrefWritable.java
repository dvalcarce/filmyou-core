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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

/**
 * An implementation of an object that it can be a PreferenceWritable or a
 * IntDoublePairWritable.
 *
 */
public final class IntDoubleOrPrefWritable implements Writable {

    private int userId;
    private int itemId;
    private float score;
    private int key;
    private double value;
    private boolean isPref;

    /**
     * Empty constructor.
     */
    public IntDoubleOrPrefWritable() {

    }

    /**
     * Constructor for a PreferenceWritable.
     *
     * @param userID
     * @param itemID
     * @param score
     */
    public IntDoubleOrPrefWritable(final int userID, final int itemID, final float score) {
        this.userId = userID;
        this.itemId = itemID;
        this.score = score;
        this.isPref = true;
    }

    /**
     * Constructor for a IntDoublePairWritable.
     *
     * @param key
     * @param value
     */
    public IntDoubleOrPrefWritable(final int key, final double value) {
        this.key = key;
        this.value = value;
        this.isPref = false;
    }

    /**
     * Constructor for a IntWritable (skipping value info).
     *
     * @param key
     */
    public IntDoubleOrPrefWritable(final int key) {
        this.key = key;
        this.isPref = false;
    }

    public int getUserId() {
        return userId;
    }

    public int getItemId() {
        return itemId;
    }

    public float getScore() {
        return score;
    }

    public int getKey() {
        return key;
    }

    public double getValue() {
        return value;
    }

    public boolean isPref() {
        return isPref;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeBoolean(isPref);
        if (isPref) {
            out.writeInt(userId);
            out.writeInt(itemId);
            out.writeFloat(score);
        } else {
            out.writeInt(key);
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        isPref = in.readBoolean();
        if (isPref) {
            userId = in.readInt();
            itemId = in.readInt();
            score = in.readFloat();
        } else {
            key = in.readInt();
            value = in.readDouble();
        }
    }

    @Override
    public int hashCode() {
        if (isPref) {
            return Ints.hashCode(userId) ^ Ints.hashCode(itemId) ^ Floats.hashCode(score);
        } else {
            return Ints.hashCode(key) ^ Doubles.hashCode(value);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof IntDoubleOrPrefWritable)) {
            return false;
        }
        final IntDoubleOrPrefWritable other = (IntDoubleOrPrefWritable) o;
        if (isPref == other.isPref) {
            if (isPref) {
                return userId == other.getUserId() && itemId == other.getItemId()
                        && score == other.getScore();
            }
            return key == other.getKey() && value == other.getValue();

        }
        return false;
    }

    @Override
    public String toString() {
        if (isPref) {
            return String.format("%d\t%d\t%f", userId, itemId, score);
        }
        return String.format("%d\t%f", key, value);
    }

}
