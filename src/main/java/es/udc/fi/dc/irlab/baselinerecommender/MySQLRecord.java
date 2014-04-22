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

package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * Input record for MySQL.
 * 
 */
public class MySQLRecord implements Writable, DBWritable {

	int user;
	int item;
	float score;

	public MySQLRecord() {

	}

	public MySQLRecord(int user, int item, float score) {
		this.user = user;
		this.item = item;
		this.score = score;
	}

	public void readFields(DataInput in) throws IOException {
		user = in.readInt();
		item = in.readInt();
		score = in.readFloat();
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		user = resultSet.getInt(1);
		item = resultSet.getInt(2);
		score = resultSet.getFloat(3);
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(user);
		out.writeInt(item);
		out.writeFloat(score);
	}

	public void write(PreparedStatement stmt) throws SQLException {
		stmt.setInt(1, user);
		stmt.setInt(2, item);
		stmt.setFloat(3, score);
	}

}
