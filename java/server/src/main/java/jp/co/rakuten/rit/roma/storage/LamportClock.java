package jp.co.rakuten.rit.roma.storage;

public class LamportClock implements LogicalClock {

    private long raw;

    public LamportClock(long raw) {
        this.raw = raw;
    }

    public long getRaw() {
        return raw;
    }

    public LogicalClock incr() {
        raw = (raw + 1) & 0xffffffff;
        return this;
    }

    public int compareTo(LogicalClock c) {
        if (!(c instanceof LamportClock)) {
            throw new IllegalArgumentException();
        }
        LamportClock lc = (LamportClock) c;
        long sub = raw - lc.raw;
        if (sub == 0) {
            return 0;
        }
        long abs = (sub < 0) ? -sub : sub;
        if ((abs & 0x80000000) == 0) {
            return (raw - lc.raw > 0) ? 1 : -1;
        } else {
            return (lc.raw - raw > 0) ? 1 : -1;
        }
//        if (abs < 0x80000000) {
//            return (int) (raw - lc.raw);
//        } else {
//            return (int) (lc.raw - raw);
//        }
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