package com.github.yantzu.hendeavour.hive.ql.io;

import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class HiveInputFormat<K extends WritableComparable, V extends Writable> extends
		org.apache.hadoop.hive.ql.io.HiveInputFormat<K, V> {

	protected void init(JobConf job) {
		super.init(job);

		for (PartitionDesc partitionDesc : pathToPartitionInfo.values()) {
			Class<? extends InputFormat> targetInputFormatClass = partitionDesc.getInputFileFormatClass();
			if (partitionDesc.getInputFileFormatClass().equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class)) {
				targetInputFormatClass = com.github.yantzu.hendeavour.mapred.SequenceFileInputFormat.class;
			} else if (partitionDesc.getInputFileFormatClass().equals(org.apache.hadoop.mapred.TextInputFormat.class)) {
				targetInputFormatClass = com.github.yantzu.hendeavour.mapred.TextInputFormat.class;
			}
			partitionDesc.setInputFileFormatClass(targetInputFormatClass);
		}
	}

}
