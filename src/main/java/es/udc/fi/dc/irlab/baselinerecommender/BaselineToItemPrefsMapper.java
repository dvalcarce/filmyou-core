package es.udc.fi.dc.irlab.baselinerecommender;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.EntityPrefWritable;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.math.VarLongWritable;

public class BaselineToItemPrefsMapper
	extends
	Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, VarLongWritable, VarLongWritable> {

    public static final String RATING_SHIFT = BaselineToItemPrefsMapper.class
	    + "shiftRatings";

    private boolean booleanData;
    private float ratingShift;

    @Override
    protected void setup(Context context) {
	Configuration jobConf = context.getConfiguration();
	booleanData = jobConf.getBoolean(RecommenderJob.BOOLEAN_DATA, false);
	ratingShift = Float.parseFloat(jobConf.get(RATING_SHIFT, "0.0"));
    }

    @Override
    public void map(Map<String, ByteBuffer> keys,
	    Map<String, ByteBuffer> columns, Context context)
	    throws IOException, InterruptedException {

	long userID = (long) keys.get("user").getInt();
	long itemID = (long) keys.get("movie").getInt();

	if (booleanData) {
	    context.write(new VarLongWritable(userID), new VarLongWritable(
		    itemID));
	} else {
	    float prefValue = (float) columns.get("score").getInt()
		    + ratingShift;
	    context.write(new VarLongWritable(userID), new EntityPrefWritable(
		    itemID, prefValue));
	}
    }
}
