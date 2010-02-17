package jp.co.rakuten.rit.roma.storage;

public class LogicalClockFactory {

    public LogicalClock newLogicalClock(long lc) {
        preLogicalClockInit();
        LogicalClock lclock = initLogicalClock(lc);
        postLogicalClockInit(lclock);
        return lclock;
    }

    public void preLogicalClockInit() {
    }

    public LogicalClock initLogicalClock(long lc) {
        return new LamportClock(lc);
    }

    public void postLogicalClockInit(LogicalClock lclock) {
    }
}
