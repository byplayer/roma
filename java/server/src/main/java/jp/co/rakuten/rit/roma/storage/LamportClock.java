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
        raw++;
        // TODO Auto-generated method stub
    }

    public int compareTo(LogicalClock o) {
        if (!(o instanceof LamportClock)) {
            throw new IllegalArgumentException();
        }
        // TODO
        return 0;
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