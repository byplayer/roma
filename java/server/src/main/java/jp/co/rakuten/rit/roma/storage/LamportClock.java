package jp.co.rakuten.rit.roma.storage;

public class LamportClock implements LogicalClock {

    private long raw;

    public LamportClock(long raw) {
        this.raw = raw;
    }

    public long getRaw() {
        return raw;
    }

    public void incr() {
        raw = (raw + 1) & 0xffffffff;
    }

    public int compareTo(LogicalClock c) {
        if (!(c instanceof LamportClock)) {
            throw new IllegalArgumentException();
        }
        LamportClock lc = (LamportClock) c;
        long sub = raw - lc.raw;
        long abs = (sub < 0) ? -sub : sub;
        if (abs < 0x80000000) {
            return (int) (raw - lc.raw);
        } else {
            return (int) (lc.raw - raw);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LamportClock)) {
            return false;
        } else {
            return raw == ((LamportClock) o).raw;
        }
    }

    @Override
    public String toString() {
        return "LamportClock(" + raw + ")";
    }
}