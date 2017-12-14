package com.github.yantzu.hendeavour.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

public class NullRecordReader implements RecordReader<NullWritable, NullWritable> {

	@Override
	public boolean next(NullWritable key, NullWritable value) throws IOException {
		return false;
	}

	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	@Override
	public NullWritable createValue() {
		return NullWritable.get();
	}

	@Override
	public long getPos() throws IOException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public float getProgress() throws IOException {
		return 100;
	}

}
