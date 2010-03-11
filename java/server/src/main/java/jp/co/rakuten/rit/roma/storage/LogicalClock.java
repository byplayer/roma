package jp.co.rakuten.rit.roma.storage;

public interface LogicalClock extends Comparable<LogicalClock> {

    LogicalClock incr();

    long getRaw();

    int compareTo(LogicalClock c);
}