package com.github.yantzu.hendeavour.mapred;

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

public class SequenceFileRecordReader<K, V> extends org.apache.hadoop.mapred.SequenceFileRecordReader<K, V> {

	public static final Log LOG = LogFactory.getLog(SequenceFileRecordReader.class);
	  
	private boolean eof = false;
	
	public SequenceFileRecordReader(Configuration conf, FileSplit split) throws IOException {
		super(conf, split);
	}

	public synchronized boolean next(K key, V value) throws IOException {
		if (eof) {
			return false;
		}
		try {
			return super.next(key, value);
		} catch (EOFException eofException) {
			LOG.warn("End Of File", eofException);
			this.eof = true;
			return false;
		}
	}

	
	protected synchronized boolean next(K key) throws IOException {
		if (eof) {
			return false;
		}
		try {
			return super.next(key);
		} catch (EOFException eofException) {
			LOG.warn("End Of File", eofException);
			this.eof = true;
			return false;
		}
	}

	
	protected synchronized void getCurrentValue(V value) throws IOException {
		try {
			super.getCurrentValue(value);
		} catch (EOFException eofException) {
			LOG.warn("End Of File", eofException);
			this.eof = true;
		}
	}
}
