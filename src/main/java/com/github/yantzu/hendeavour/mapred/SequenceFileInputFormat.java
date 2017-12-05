package com.github.yantzu.hendeavour.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SequenceFileInputFormat<K, V> extends org.apache.hadoop.mapred.SequenceFileInputFormat<K, V> {

	public static final Log LOG = LogFactory.getLog(SequenceFileInputFormat.class);

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		return FileInputFormats.unlimit((FileSplit[]) super.getSplits(job, numSplits));
	}

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		LOG.info("Split Length:" + split.getLength());

		reporter.setStatus(split.toString());

		return new SequenceFileRecordReader<K, V>(job, (FileSplit) split);
	}
}
