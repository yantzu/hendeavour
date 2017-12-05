package com.github.yantzu.hendeavour.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.SplitLocationInfo;

public abstract class FileInputFormats {

	public static final Log LOG = LogFactory.getLog(FileInputFormats.class);

	protected static FileSplit[] unlimit(FileSplit[] inputSplits) throws IOException {
		LOG.info("Unlimit File Splits - Start");

		if (inputSplits == null) {
			return inputSplits;
		}

		List<FileSplit> outputSplits = new ArrayList<FileSplit>(inputSplits.length);

		Map<Path, FileSplit> lastSplitOfFile = new HashMap<Path, FileSplit>();

		for (FileSplit inputSplit : inputSplits) {
			FileSplit lastSplit = lastSplitOfFile.get(inputSplit.getPath());
			if (lastSplit == null) {
				lastSplitOfFile.put(inputSplit.getPath(), inputSplit);
			} else {
				if (inputSplit.getStart() > lastSplit.getStart()) {
					outputSplits.add(lastSplit);
					lastSplitOfFile.put(inputSplit.getPath(), inputSplit);
				} else {
					outputSplits.add(inputSplit);
				}
			}
		}

		LOG.info("Unlimit " + lastSplitOfFile.size() + " File Split(s)");

		for (FileSplit lastSplit : lastSplitOfFile.values()) {

			long unlimitLength = Long.MAX_VALUE - lastSplit.getStart() - 1;

			if (lastSplit.getLocationInfo() != null && lastSplit.getLocationInfo().length > 0) {
				List<String> hosts = new ArrayList<String>(lastSplit.getLocationInfo().length);
				List<String> inMemoryHost = new ArrayList<String>();
				for (SplitLocationInfo locationInfo : lastSplit.getLocationInfo()) {
					hosts.add(locationInfo.getLocation());
					if (locationInfo.isInMemory()) {
						inMemoryHost.add(locationInfo.getLocation());
					}
				}

				LOG.info("Unlimit LocationInfos File " + lastSplit + " at " + lastSplit.getStart());

				outputSplits.add(new FileSplit(lastSplit.getPath(), lastSplit.getStart(), unlimitLength, hosts
						.toArray(new String[hosts.size()]), inMemoryHost.toArray(new String[inMemoryHost.size()])));
			} else {
				LOG.info("Unlimit Locations File " + lastSplit + " at " + lastSplit.getStart());

				outputSplits.add(new FileSplit(lastSplit.getPath(), lastSplit.getStart(), unlimitLength, lastSplit
						.getLocations()));
			}
		}

		LOG.info("Unlimit File Splits - End");

		return outputSplits.toArray(new FileSplit[outputSplits.size()]);
	}
}
