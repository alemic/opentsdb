// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;

/**
 * Represents a read-only sequence of continuous data points.
 * <p>
 * This class stores a continuous sequence of {@link RowSeq}s in memory.
 */
final class Span implements DataPoints {

  private static final Logger LOG = LoggerFactory.getLogger(Span.class);

  /** The {@link TSDB} instance we belong to. */
  private final TSDB tsdb;

  /** All the rows in this span. */
  private ArrayList<RowSeq> rows = new ArrayList<RowSeq>();

  /** indicates if the sequence should be normalized */
  private boolean normalize;

  /** indicates the normalization interval */
  private long normalize_interval;

  Span(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  private void checkNotEmpty() {
    if (rows.size() == 0) {
      throw new IllegalStateException("empty Span");
    }
  }

  public String metricName() {
    checkNotEmpty();
    return rows.get(0).metricName();
  }

  public Map<String, String> getTags() {
    checkNotEmpty();
    return rows.get(0).getTags();
  }

  public List<String> getAggregatedTags() {
    return Collections.emptyList();
  }

  public int size() {
    int size = 0;
    for (final RowSeq row : rows) {
      size += row.size();
    }
    return size;
  }

  public int aggregatedSize() {
    return 0;
  }

  /**
   * Adds an HBase row to this span, using a row from a scanner.
   * @param row The HBase row to add to this span.
   * @throws IllegalArgumentException if the argument and this span are for
   * two different time series.
   * @throws IllegalArgumentException if the argument represents a row for
   * data points that are older than those already added to this span.
   */
  void addRow(final ArrayList<KeyValue> row) {
    long last_ts = 0;
    if (rows.size() != 0) {
      // Verify that we have the same metric id and tags.
      final byte[] key = row.get(0).key();
      final RowSeq last = rows.get(rows.size() - 1);
      final short metric_width = tsdb.metrics.width();
      final short tags_offset = (short) (metric_width + Const.TIMESTAMP_BYTES);
      final short tags_bytes = (short) (key.length - tags_offset);
      String error = null;
      if (key.length != last.key.length) {
        error = "row key length mismatch";
      } else if (Bytes.memcmp(key, last.key, 0, metric_width) != 0) {
        error = "metric ID mismatch";
      } else if (Bytes.memcmp(key, last.key, tags_offset, tags_bytes) != 0) {
        error = "tags mismatch";
      }
      if (error != null) {
        throw new IllegalArgumentException(error + ". "
            + "This Span's last row key is " + Arrays.toString(last.key)
            + " whereas the row key being added is " + Arrays.toString(key)
            + " and metric_width=" + metric_width);
      }
      last_ts = last.timestamp(last.size() - 1);
      // Optimization: check whether we can put all the data points of `row'
      // into the last RowSeq object we created, instead of making a new
      // RowSeq.  If the time delta between the timestamp encoded in the
      // row key of the last RowSeq we created and the timestamp of the
      // last data point in `row' is small enough, we can merge `row' into
      // the last RowSeq.
      if (RowSeq.canTimeDeltaFit(lastTimestampInRow(metric_width, row)
                                 - last.baseTime())) {
        last.addRow(row);
        return;
      }
    }

    final RowSeq rowseq = new RowSeq(tsdb);
    rowseq.setRow(row);
    if (last_ts >= rowseq.timestamp(0)) {
      LOG.error("New RowSeq added out of order to this Span! Last = " +
                rows.get(rows.size() - 1) + ", new = " + rowseq);
      return;
    }
    rows.add(rowseq);
  }

  /**
   * Package private helper to access the last timestamp in an HBase row.
   * @param metric_width The number of bytes on which metric IDs are stored.
   * @param row The row coming straight out of HBase.
   * @return A strictly positive 32-bit timestamp.
   * @throws IllegalArgumentException if {@code row} doesn't contain any cell.
   */
  static long lastTimestampInRow(final short metric_width,
                                 final ArrayList<KeyValue> row) {
    final int size = row.size();
    if (size < 1) {
      throw new IllegalArgumentException("empty row: " + row);
    }
    final KeyValue lastkv = row.get(size - 1);
    final long base_time = Bytes.getUnsignedInt(lastkv.key(), metric_width);
    final short last_delta = (short)
      (Bytes.getUnsignedShort(lastkv.qualifier()) >>> Const.FLAG_BITS);
    return base_time + last_delta;
  }

  public SeekableView iterator() {
    return normalize ? normalizedSpanIterator() : spanIterator();
  }

  /**
   * Finds the index of the row of the ith data point and the offset in the row.
   * @param i The index of the data point to find.
   * @return two ints packed in a long.  The first int is the index of the row
   * in {@code rows} and the second is offset in that {@link RowSeq} instance.
   */
  private long getIdxOffsetFor(final int i) {
    int idx = 0;
    int offset = 0;
    for (final RowSeq row : rows) {
      final int sz = row.size();
      if (offset + sz > i) {
        break;
      }
      offset += sz;
      idx++;
    }
    return ((long) idx << 32) | (i - offset);
  }

  public long timestamp(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).timestamp(offset);
  }

  public boolean isInteger(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).isInteger(offset);
  }

  public long longValue(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).longValue(offset);
  }

  public double doubleValue(final int i) {
    final long idxoffset = getIdxOffsetFor(i);
    final int idx = (int) (idxoffset >>> 32);
    final int offset = (int) (idxoffset & 0x00000000FFFFFFFF);
    return rows.get(idx).doubleValue(offset);
  }

  public void normalize(final boolean normalize, final long interval) {
    this.normalize = normalize;
    this.normalize_interval = interval;
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Span(")
       .append(rows.size())
       .append(" rows, [");
    for (int i = 0; i < rows.size(); i++) {
      if (i != 0) {
        buf.append(", ");
      }
      buf.append(rows.get(i).toString());
    }
    buf.append("])");
    return buf.toString();
  }

  /**
   * Finds the index of the row in which the given timestamp should be.
   * @param timestamp A strictly positive 32-bit integer.
   * @return A strictly positive index in the {@code rows} array.
   */
  private short seekRow(final long timestamp) {
    short row_index = 0;
    RowSeq row = null;
    final int nrows = rows.size();
    for (int i = 0; i < nrows; i++) {
      row = rows.get(i);
      final int sz = row.size();
      if (row.timestamp(sz - 1) < timestamp) {
        row_index++;  // The last DP in this row is before 'timestamp'.
      } else {
        break;
      }
    }
    if (row_index == nrows) {  // If this timestamp was too large for the
      --row_index;             // last row, return the last row.
    }
    return row_index;
  }

  /** Package private iterator method to access it as a Span.Iterator. */
  SeekableView spanIterator() {
    return normalize ? new Span.NormalizedIterator() : new Span.Iterator();
  }

  /** Iterator for {@link Span}s. */
  final class Iterator implements SeekableView {

    /** Index of the {@link RowSeq} we're currently at, in {@code rows}. */
    private short row_index;

    /** Iterator on the current row. */
    private DataPointsIterator current_row;

    Iterator() {
      current_row = rows.get(0).internalIterator();
    }

    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    public DataPoint next() {
      if (current_row.hasNext()) {
        return current_row.next();
      } else if (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        return current_row.next();
      }
      throw new NoSuchElementException("no more elements");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public void seek(final long timestamp) {
      short row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);
    }

    public String toString() {
      return "Span.Iterator(row_index=" + row_index
        + ", current_row=" + current_row + ", span=" + Span.this + ')';
    }

  }

  Span.NormalizedIterator normalizedSpanIterator()
  {
    return new NormalizedIterator();
  }


  final class NormalizedIterator implements SeekableView, DataPoint {

    private short row_index;
    private DataPointsIterator current_row;

    private final long INTERVAL;

    /** timestamp aligned on INTERVAL */
    private long aligned_timestamp;
    /** the timestamp to return */
    private long current_timestamp;
    /** the value computed at current_timestamp */
    private double current_value;

    private long base_timestamp;
    private double base_value;

    private long next_timestamp;
    private double next_value;

    NormalizedIterator() {
      current_row = rows.get(0).internalIterator();
      INTERVAL = normalize_interval;
    }

    @Override
    public boolean hasNext() {
      return (current_row.hasNext()             // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    private DataPoint _next() {
      if (current_row.hasNext()) {
        return current_row.next();
      } else if (row_index < rows.size() - 1) {
        row_index++;
        current_row = rows.get(row_index).internalIterator();
        return current_row.next();
      }
      throw new NoSuchElementException("no more elements");
    }

    public DataPoint next() {

      final long x0 = base_timestamp;
      final double y0 = base_value;

      long x1 = 0;
      double y1 = 0;

      double amount = 0;
      double interval = 0;

      if (x0 == aligned_timestamp) {
        amount = y0;
        interval = 1;
      } else {

        interval = Math.abs(aligned_timestamp - x0);
        amount = (y0 * interval);

        boolean has_next = true;
        do {

          has_next = false;

          /*
           * we are going to look ahead in order to check if some points match
           * the current time interval. If yes their area will be added to the
           * current.
           */

          x1 = next_timestamp;
          y1 = next_value;

          if (x1 > aligned_timestamp) {
            x1 = aligned_timestamp;
          } else if (x1 < aligned_timestamp && hasNext()) {
            has_next = true;
            final DataPoint dp = _next();
            next_timestamp = dp.timestamp();
            next_value = dp.toDouble();
          }

          // in most of the case x1 > x0
          // but when x1 is aligned and when some
          // datapoints are missing, we can have
          // x1 < x0
          final long distance = Math.abs(x1 - x0);
          interval += distance;
          amount += y1 * distance;

        } while (has_next);
      }

      // values to 'return'
      current_timestamp = aligned_timestamp;
      current_value = amount / interval;

      aligned_timestamp += INTERVAL;
      base_timestamp = next_timestamp;
      base_value = next_value;

      // handle the case we missed a point,
      // To detect it we just need to know if the next
      // aligned point is before the next data point we
      // got. If yes, we missed a point.

      if (aligned_timestamp >= next_timestamp && hasNext()) {
        final DataPoint dp = _next();
        next_timestamp = dp.timestamp();
        next_value = dp.toDouble();
      }

      return this;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long timestamp() {
      return current_timestamp;
    }

    @Override
    public boolean isInteger() {
      return false;
    }

    @Override
    public long longValue() {
      throw new ClassCastException("this value is not a long in " + this);
    }

    @Override
    public double doubleValue() {
      return current_value;
    }

    @Override
    public double toDouble() {
      return current_value;
    }

    @Override
    public void seek(final long timestamp) {
      short row_index = seekRow(timestamp);
      if (row_index != this.row_index) {
        this.row_index = row_index;
        current_row = rows.get(row_index).internalIterator();
      }
      current_row.seek(timestamp);

      // We got synchronized so at this point we have
      // to initialize the first data point
      current_timestamp = aligned_timestamp = timestamp + INTERVAL - timestamp % INTERVAL;
      if (hasNext()) {
        final DataPoint dp = _next();
        base_timestamp = dp.timestamp();
        base_value = dp.toDouble();
      }

      if (hasNext()) {
        final DataPoint dp = _next();
        next_timestamp = dp.timestamp();
        next_value = dp.toDouble();
      }
    }

    @Override
    public String toString() {
      return "Span.NormalizedIterator(row_index=" + row_index
        + ", current_row=" + current_row + ", span=" + Span.this + ')';
    }

  }

  /** Package private iterator method to access it as a DownsamplingIterator. */
  Span.DownsamplingIterator downsampler(final int interval,
                                        final Aggregator downsampler) {
    return new Span.DownsamplingIterator(interval, downsampler);
  }

  /**
   * Iterator that downsamples the data using an {@link Aggregator}.
   * <p>
   * This implementation relies on the fact that the {@link RowSeq}s in this
   * {@link Span} have {@code O(1)} access to individual data points, in order
   * to be efficient.
   */
  final class DownsamplingIterator
    implements SeekableView, DataPoint,
               Aggregator.Longs, Aggregator.Doubles {

    /** Extra bit we set on the timestamp of floating point values. */
    private static final long FLAG_FLOAT = 0x8000000000000000L;

    /** Mask to use in order to get rid of the flag above. */
    private static final long TIME_MASK  = 0x7FFFFFFFFFFFFFFFL;

    /** The "sampling" interval, in seconds. */
    private final int interval;

    /** Function to use to for downsampling. */
    private final Aggregator downsampler;

    /** Index of the {@link RowSeq} we're currently at, in {@code rows}. */
    private int row_index;

    /** The row we're currently at. */
    private RowSeq current_row;

    /** Index of data point we're at, in {@code current_row}. */
    private int pos = -1;

    /**
     * Current timestamp (unsigned 32 bits).
     * The most significant bit is used to store FLAG_FLOAT.
     */
    private long time;

    /** Current value (either an actual long or a double encoded in a long). */
    private long value;

    /**
     * Ctor.
     * @param interval The interval in seconds wanted between each data point.
     * @param downsampler The downsampling function to use.
     * @param iterator The iterator to access the underlying data.
     */
    DownsamplingIterator(final int interval,
                         final Aggregator downsampler) {
      this.interval = interval;
      this.downsampler = downsampler;
      this.current_row = rows.get(0);
    }

    // ------------------ //
    // Iterator interface //
    // ------------------ //

    public boolean hasNext() {
      return (pos < current_row.size() - 1      // more points in this row
              || row_index < rows.size() - 1);  // or more rows
    }

    private boolean moveToNext() {
      if (pos == current_row.size() - 1) {  // Have we finished this row?
        // Yes, move on to the next one.
        if (row_index < rows.size() - 1) {  // Do we have more rows?
          current_row = rows.get(++row_index);
          pos = 0;
          return true;
        } else {  // No more rows, can't go further.
          pos = -42;  // Safeguard to catch bugs earlier.
          return false;
        }
      }
      pos++;
      return true;
    }

    public DataPoint next() {
      if (!hasNext()) {
        throw new NoSuchElementException("no more data points in " + this);
      }

      // Look ahead to see if all the data points that fall within the next
      // interval turn out to be integers.  While we do this, compute the
      // average timestamp of all the datapoints in that interval.
      long newtime = 0;
      final int saved_row_index = row_index;
      final int saved_pos = pos;
      // Since we know hasNext() returned true, we have at least 1 point.
      moveToNext();
      time = current_row.timestamp(pos) + interval;  // end of this interval.
      boolean integer = true;
      int npoints = 0;
      do {
        npoints++;
        newtime += current_row.timestamp(pos);
        //LOG.info("Downsampling @ time " + current_row.timestamp(pos));
        integer &= current_row.isInteger(pos);
      } while (moveToNext() && current_row.timestamp(pos) < time);
      newtime /= npoints;

      // Now that we're done looking ahead, let's go back where we were.
      pos = saved_pos;
      if (row_index != saved_row_index) {
        row_index = saved_row_index;
        current_row = rows.get(row_index);
      }

      // Compute `value'.  This will rely on `time' containing the end time of
      // this interval...
      if (integer) {
        value = downsampler.runLong(this);
      } else {
        value = Double.doubleToRawLongBits(downsampler.runDouble(this));
      }
      // ... so update the time only here.
      time = newtime;
      //LOG.info("Downsampled avg time " + time);
      if (!integer) {
        time |= FLAG_FLOAT;
      }
      return this;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    // ---------------------- //
    // SeekableView interface //
    // ---------------------- //

    public void seek(final long timestamp) {
      row_index = seekRow(timestamp);
      current_row = rows.get(row_index);
      final DataPointsIterator it = current_row.internalIterator();
      it.seek(timestamp);
      pos = it.index();
    }

    // ------------------- //
    // DataPoint interface //
    // ------------------- //

    public long timestamp() {
      return time & TIME_MASK;
    }

    public boolean isInteger() {
      return (time & FLAG_FLOAT) == 0;
    }

    public long longValue() {
      if (isInteger()) {
        return value;
      }
      throw new ClassCastException("this value is not a long in " + this);
    }

    public double doubleValue() {
      if (!isInteger()) {
        return Double.longBitsToDouble(value);
      }
      throw new ClassCastException("this value is not a float in " + this);
    }

    public double toDouble() {
      return isInteger() ? longValue() : doubleValue();
    }

    // -------------------------- //
    // Aggregator.Longs interface //
    // -------------------------- //

    public boolean hasNextValue() {
      if (pos == current_row.size() - 1) {
        if (row_index < rows.size() - 1) {
          //LOG.info("hasNextValue: next row? " + (rows.get(row_index + 1).timestamp(0) < time));
          return rows.get(row_index + 1).timestamp(0) < time;
        } else {
          //LOG.info("hasNextValue: false, this is the end");
          return false;
        }
      }
      //LOG.info("hasNextValue: next point? " + (current_row.timestamp(pos+1) < time));
      return current_row.timestamp(pos + 1) < time;
    }

    public long nextLongValue() {
      if (hasNextValue()) {
        moveToNext();
        return current_row.longValue(pos);
      }
      throw new NoSuchElementException("no more longs in interval of " + this);
    }

    // ---------------------------- //
    // Aggregator.Doubles interface //
    // ---------------------------- //

    public double nextDoubleValue() {
      if (hasNextValue()) {
        moveToNext();
        // Use `toDouble' instead of `doubleValue' because we can get here if
        // there's a mix of integer values and floating point values in the
        // current downsampled interval.
        return current_row.toDouble(pos);
      }
      throw new NoSuchElementException("no more floats in interval of " + this);
    }

    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("Span.DownsamplingIterator(interval=").append(interval)
         .append(", downsampler=").append(downsampler)
         .append(", row_index=").append(row_index)
         .append(", pos=").append(pos)
         .append(", current time=").append(timestamp())
         .append(", current value=");
     if (isInteger()) {
       buf.append("long:").append(longValue());
     } else {
       buf.append("double:").append(doubleValue());
     }
     buf.append(", rows=").append(rows).append(')');
     return buf.toString();
    }

  }

}
