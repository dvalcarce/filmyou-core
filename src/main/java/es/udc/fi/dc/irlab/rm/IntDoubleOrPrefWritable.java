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

    private int userID;
    private int itemID;
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
    public IntDoubleOrPrefWritable(int userID, int itemID, float score) {
	this.userID = userID;
	this.itemID = itemID;
	this.score = score;
	this.isPref = true;
    }

    /**
     * Constructor for a IntDoublePairWritable.
     * 
     * @param key
     * @param value
     */
    public IntDoubleOrPrefWritable(int key, double value) {
	this.key = key;
	this.value = value;
	this.isPref = false;
    }

    public int getUserID() {
	return userID;
    }

    public int getItemID() {
	return itemID;
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
    public void write(DataOutput out) throws IOException {
	out.writeBoolean(isPref);
	if (isPref) {
	    out.writeInt(userID);
	    out.writeInt(itemID);
	    out.writeFloat(score);
	} else {
	    out.writeInt(key);
	    out.writeDouble(value);
	}
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	isPref = in.readBoolean();
	if (isPref) {
	    userID = in.readInt();
	    itemID = in.readInt();
	    score = in.readFloat();
	} else {
	    key = in.readInt();
	    value = in.readDouble();
	}
    }

    @Override
    public int hashCode() {
	if (isPref) {
	    return Ints.hashCode(userID) ^ Ints.hashCode(itemID)
		    ^ Floats.hashCode(score);
	} else {
	    return Ints.hashCode(key) ^ Doubles.hashCode(value);
	}
    }

    @Override
    public boolean equals(Object o) {
	if (!(o instanceof IntDoubleOrPrefWritable)) {
	    return false;
	}
	IntDoubleOrPrefWritable other = (IntDoubleOrPrefWritable) o;
	if (isPref == other.isPref) {
	    if (isPref) {
		return userID == other.getUserID()
			&& itemID == other.getItemID()
			&& score == other.getScore();
	    }
	    return key == other.getKey() && value == other.getValue();

	}
	return false;
    }

    @Override
    public String toString() {
	if (isPref) {
	    return String.format("%d\t%d\t%f", userID, itemID, score);
	}
	return String.format("%d\t%f", key, value);
    }

}
