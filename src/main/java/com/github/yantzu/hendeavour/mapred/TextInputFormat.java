package com.github.yantzu.hendeavour.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class TextInputFormat extends org.apache.hadoop.mapred.TextInputFormat {
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		return FileInputFormats.unlimit((FileSplit[]) super.getSplits(job, numSplits));
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		LOG.info("Split Length:" + split.getLength());

		return super.getRecordReader(split, job, reporter);
	}
}
