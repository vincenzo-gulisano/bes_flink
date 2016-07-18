package bes_flink;

public interface AggregateWindow<T_IN, T_OUT> {

	public AggregateWindow<T_IN, T_OUT> factory();

	public void setup();

	public void update(T_IN t);

	public T_OUT getAggregatedResult(long timestamp, String groupby,
			T_IN triggeringTuple);

	public long getTimestamp(T_IN t);

	public String getKey(T_IN t);

	//
	// public int getNumberOfOutFields();

}
