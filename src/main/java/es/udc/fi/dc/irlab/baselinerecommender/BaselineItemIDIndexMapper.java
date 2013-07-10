package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.hadoop.ToEntityPrefsMapper;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

public final class BaselineItemIDIndexMapper
		extends
		Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, VarIntWritable, VarLongWritable> {
	private boolean transpose;

	@Override
	protected void setup(Context context) {
		Configuration jobConf = context.getConfiguration();
		transpose = jobConf.getBoolean(ToEntityPrefsMapper.TRANSPOSE_USER_ITEM,
				false);
	}

	// @Override
	// protected void map(LongWritable key, Text value, Context context)
	// throws IOException, InterruptedException {
	// String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());
	// long itemID = Long.parseLong(tokens[transpose ? 0 : 1]);
	// int index = TasteHadoopUtils.idToIndex(itemID);
	// context.write(new VarIntWritable(index), new VarLongWritable(itemID));
	// }

	@Override
	protected void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> value,
			Context context) throws IOException, InterruptedException {
		for (ByteBuffer itemIDBytes : value.keySet()) {
			long itemID = itemIDBytes.asLongBuffer().get();
			int index = TasteHadoopUtils.idToIndex(itemID);
			context.write(new VarIntWritable(index),
					new VarLongWritable(itemID));
		}
	}
}
