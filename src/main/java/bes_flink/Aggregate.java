package bes_flink;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class Aggregate<T_IN, T_OUT> {

	/**
	 * This class implements the time-based sliding window Aggregate. The
	 * following assumption holds,
	 * 
	 * Input tuples (T_IN) contain a field <timestampFieldID> of type long
	 * (timestamp)
	 * 
	 * Input tuples (T_IN) contain a field <groupbyFieldID> of type String (if
	 * groupbyFieldID is not "")
	 * 
	 * The group-by parameter of the aggregate operator is defined by exactly
	 * one field
	 * 
	 * The windowSize and windowAdvance parameters have the same time units of
	 * the timestamp field
	 * 
	 * Input tuples' timestamps are non-decreasing!
	 */

	// USER PARAMETERS
	private long windowSize;
	private long windowAdvance;
	private AggregateWindow<T_IN, T_OUT> aggregateWindow;

	// OTHERS
	private long earliestTimestamp;
	private long latestTimestamp;
	private boolean firstTuple = true;
	private HashMap<Long, HashMap<String, AggregateWindow<T_IN, T_OUT>>> windows;

	public Aggregate(long windowSize, long windowAdvance,
			AggregateWindow<T_IN, T_OUT> aggregateWindow) {
		windows = new HashMap<Long, HashMap<String, AggregateWindow<T_IN, T_OUT>>>();
		this.windowSize = windowSize;
		this.windowAdvance = windowAdvance;
		this.aggregateWindow = aggregateWindow;
	}

	public List<T_OUT> processTuple(T_IN t) {

		List<T_OUT> result = new LinkedList<T_OUT>();

		// Take timestamp and make sure it has not decreased
		long timestamp = aggregateWindow.getTimestamp(t);
		if (firstTuple) {
			firstTuple = false;
		} else {
			if (timestamp < latestTimestamp) {
				throw new RuntimeException("Input tuple's timestamp decreased!");
			}
		}
		latestTimestamp = timestamp;

		// Take the group-by
		String groupby = aggregateWindow.getKey(t);

		// Purge stale windows
		List<Long> windowsStart = getWindowsStartTimestamps(timestamp,
				this.windowSize, this.windowAdvance);

		long firstWindowStart = windowsStart.get(0);

		while (earliestTimestamp < firstWindowStart) {
			if (windows.containsKey(earliestTimestamp)) {
				for (Entry<String, AggregateWindow<T_IN, T_OUT>> entry : windows
						.get(earliestTimestamp).entrySet()) {
					String entryGroupby = entry.getKey();
					AggregateWindow<T_IN, T_OUT> window = entry.getValue();
					result.add(window.getAggregatedResult(earliestTimestamp,
							entryGroupby, t));
				}
				windows.remove(earliestTimestamp);
			}
			earliestTimestamp += windowAdvance;
		}

		// Update active windows
		for (Long windowStart : windowsStart) {
			if (!windows.containsKey(windowStart)) {
				windows.put(windowStart,
						new HashMap<String, AggregateWindow<T_IN, T_OUT>>());
			}
			if (!windows.get(windowStart).containsKey(groupby)) {
				AggregateWindow<T_IN, T_OUT> window = (AggregateWindow<T_IN, T_OUT>) aggregateWindow
						.factory();
				window.setup();
				windows.get(windowStart).put(groupby, window);
			}
			windows.get(windowStart).get(groupby).update(t);
		}

		// Done...
		return result;

	}

	public static List<Long> getWindowsStartTimestamps(long timestamp,
			long windowSize, long windowAdvance) {

		LinkedList<Long> result = new LinkedList<Long>();

		long windowStart = (timestamp / windowAdvance) * windowAdvance;
		result.add(windowStart);
		while (windowStart - windowAdvance + windowSize > timestamp
				&& windowStart - windowAdvance >= 0) {
			windowStart -= windowAdvance;
			result.addFirst(windowStart);
		}

		return result;

	}

}
