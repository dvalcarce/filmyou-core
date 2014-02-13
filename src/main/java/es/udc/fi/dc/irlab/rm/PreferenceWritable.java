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

import org.apache.hadoop.io.WritableComparable;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

/**
 * A WritableComparable implementation of a Preference. A Preference is a tuple
 * defined by an userID, an itemID and a score.
 * 
 */
public final class PreferenceWritable implements
	WritableComparable<PreferenceWritable>, Cloneable {

    private int userID;
    private int itemID;
    private float score;

    public PreferenceWritable() {

    }

    public PreferenceWritable(int userID, int itemID, float score) {
	this.userID = userID;
	this.itemID = itemID;
	this.score = score;
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

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeInt(userID);
	out.writeInt(itemID);
	out.writeFloat(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	userID = in.readInt();
	itemID = in.readInt();
	score = in.readFloat();
    }

    @Override
    public int hashCode() {
	return Ints.hashCode(userID) ^ Ints.hashCode(itemID)
		^ Floats.hashCode(score);
    }

    @Override
    public boolean equals(Object o) {
	if (!(o instanceof PreferenceWritable)) {
	    return false;
	}
	PreferenceWritable other = (PreferenceWritable) o;
	return userID == other.getUserID() && itemID == other.getItemID()
		&& score == other.getScore();
    }

    @Override
    public String toString() {
	return String.format("%d\t%d\t%f", userID, itemID, score);
    }

    @Override
    public PreferenceWritable clone() {
	return new PreferenceWritable(userID, itemID, score);
    }

    @Override
    public int compareTo(PreferenceWritable o) {
	int result;

	result = Ints.compare(userID, o.getUserID());
	if (result != 0) {
	    return result;
	}

	result = Ints.compare(itemID, o.getItemID());
	if (result != 0) {
	    return result;
	}

	result = Floats.compare(score, o.getScore());
	return result;
    }

}
