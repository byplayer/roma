package jp.co.rakuten.rit.roma.storage;

public interface LogicalClock extends Comparable<LogicalClock> {

    void incr();
    
    long getRaw();
}
