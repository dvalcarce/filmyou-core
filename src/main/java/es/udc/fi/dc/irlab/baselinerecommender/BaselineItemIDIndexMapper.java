package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.math.VarIntWritable;
import org.apache.mahout.math.VarLongWritable;

public final class BaselineItemIDIndexMapper
	extends
	Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, VarIntWritable, VarLongWritable> {

    @Override
    protected void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	long itemID = (long) keys.get("movie").getInt();
	int index = TasteHadoopUtils.idToIndex(itemID);
	context.write(new VarIntWritable(index), new VarLongWritable(itemID));
    }
}
