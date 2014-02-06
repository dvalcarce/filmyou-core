package es.udc.fi.dc.irlab.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {

    /**
     * Remove data from HDFS
     * 
     * @param conf
     *            Configuration file
     * @param path
     * @throws IOException
     */
    public static void removeData(Configuration conf, String path)
	    throws IOException {
	FileSystem fs = FileSystem.get(URI.create(path), conf);
	fs.delete(new Path(path), true);
    }

}
