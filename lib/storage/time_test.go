package storage

import (
	"math"
	"testing"
	"time"
)

func TestTimeRangeFromPartition(t *testing.T) {
	for i := range 24 * 30 * 365 {
		testTimeRangeFromPartition(t, time.Now().Add(time.Hour*time.Duration(i)))
	}
}

func testTimeRangeFromPartition(t *testing.T, initialTime time.Time) {
	t.Helper()

	want := initialTime.UTC().Truncate(partitionDuration)
	var tr TimeRange
	tr.fromPartitionTime(initialTime)

	minTime := timestampToTime(tr.MinTimestamp)
	if !minTime.Equal(want) {
		t.Fatalf("unexpected MinTimestamp; got %s; want %s", minTime, want)
	}

	// Verify that the previous millisecond from tr.MinTimestamp belongs to the previous partition.
	tr.MinTimestamp--
	prevTime := timestampToTime(tr.MinTimestamp)
	if !prevTime.Before(minTime) || minTime.Sub(prevTime) > time.Millisecond {
		t.Fatalf("unexpected prevTime; got %s; want 1ms before minTime=%s", prevTime, minTime)
	}

	maxTime := timestampToTime(tr.MaxTimestamp)
	wantMax := want.Add(partitionDuration - time.Millisecond)
	if !maxTime.Equal(wantMax) {
		t.Fatalf("unexpected MaxTimestamp; got %s; want %s", maxTime, wantMax)
	}

	// Verify that the next millisecond from tr.MaxTimestamp belongs to the next partition.
	tr.MaxTimestamp++
	nextTime := timestampToTime(tr.MaxTimestamp)
	wantNext := want.Add(partitionDuration)
	if !nextTime.Equal(wantNext) {
		t.Fatalf("unexpected nextTime; got %s; want %s", nextTime, wantNext)
	}
}

func TestTimeRangeOverlapsWith(t *testing.T) {
	f := func(min1, max1, min2, max2 int64, want bool) {
		tr1 := TimeRange{min1, max1}
		tr2 := TimeRange{min2, max2}
		if got := tr1.overlapsWith(tr2); got != want {
			t.Errorf("unmet time range overlapping expectation: got %t, want %t", got, want)
		}
	}

	f(0, 0, 0, 0, true)
	f(0, 0, 0, 1, true)
	f(0, 1, 0, 0, true)
	f(1, 2, 0, 0, false)
	f(0, 0, 1, 2, false)
	f(1, 2, 0, 3, true)
	f(1, 10, 5, 15, true)
	f(5, 15, 1, 10, true)
}

func TestTimeRangeContains(t *testing.T) {
	f := func(min, max, ts int64, want bool) {
		tr := TimeRange{min, max}
		if got := tr.contains(ts); got != want {
			t.Errorf("unmet ts.contains() expectation: got %t, want %t", got, want)
		}
	}

	f(0, 0, 0, true)
	f(0, 0, 1, false)
	f(0, 0, -1, false)

	f(1, 3, 0, false)
	f(1, 3, 1, true)
	f(1, 3, 2, true)
	f(1, 3, 3, true)
	f(1, 3, 4, false)

	f(0, math.MaxInt64, -1, false)
	f(0, math.MaxInt64, 0, true)
	f(0, math.MaxInt64, 1, true)
	f(0, math.MaxInt64, math.MaxInt64/2, true)
	f(0, math.MaxInt64, math.MaxInt64-1, true)
	f(0, math.MaxInt64, math.MaxInt64, true)
}

func TestTimeRangeDateRange(t *testing.T) {
	f := func(tr TimeRange, wantMinDate, wantMaxDate uint64) {
		t.Helper()

		gotMinDate, gotMaxDate := tr.DateRange()
		if gotMinDate != wantMinDate {
			t.Errorf("unexpected min date: got %d, want %d", gotMinDate, wantMinDate)
		}
		if gotMaxDate != wantMaxDate {
			t.Errorf("unexpected max date: got %d, want %d", gotMaxDate, wantMaxDate)
		}
	}

	var tr TimeRange

	// MinTimestamp is less than MaxTimestamp, the timestamps belong to the
	// different days. Min date must be less than the max date.
	tr = TimeRange{1*msecPerDay + 123, 2*msecPerDay + 456}
	f(tr, 1, 2)

	// MinTimestamp is less than MaxTimestamp and both timestamps belong to the
	// same day. Max date must be the same as min date.
	tr = TimeRange{1*msecPerDay + 123, 1*msecPerDay + 456}
	f(tr, 1, 1)

	// MinTimestamp equals to MaxTimestamp. Max date must be the same as min
	// date.
	tr = TimeRange{1*msecPerDay + 123, 1*msecPerDay + 123}
	f(tr, 1, 1)

	// MinTimestamp is the first millisecond of the day and equals to
	// MaxTimestamp. Min and max dates must be the same.
	tr = TimeRange{1 * msecPerDay, 1 * msecPerDay}
	f(tr, 1, 1)

	// MinTimestamp is greater than MaxTimestamp MaxTimestamp. Max date must be
	// the same as min date.
	tr = TimeRange{2*msecPerDay + 654, 1*msecPerDay + 321}
	f(tr, 2, 2)

	// MaxTimestamp is the last millisecond of the day.
	// Max date should be the next date
	tr = TimeRange{1*msecPerDay + 123, 2 * msecPerDay}
	f(tr, 1, 2)

	// MaxTimestamp is the first millisecond of the day.
	// Max date should be the next date
	tr = TimeRange{1*msecPerDay + 123, 2*msecPerDay + 1}
	f(tr, 1, 2)
}

func TestDateToString(t *testing.T) {
	f := func(date uint64, want string) {
		t.Helper()

		if got := dateToString(date); got != want {
			t.Errorf("dateToString(%d) unexpected return value: got %q, want %q", date, got, want)
		}
	}

	f(globalIndexDate, "[entire retention period]")
	f(1, "1970-01-02")
	f(10, "1970-01-11")
}

func TestTimeRangeString(t *testing.T) {
	f := func(tr TimeRange, want string) {
		t.Helper()

		if got := tr.String(); got != want {
			t.Errorf("TimeRange.String() unexpected return value: got %q, want %q", got, want)
		}
	}

	f(globalIndexTimeRange, "[entire retention period]")
	f(TimeRange{
		MinTimestamp: 0,
		MaxTimestamp: 1,
	}, "[1970-01-01T00:00:00Z..1970-01-01T00:00:00.001Z]")
	f(TimeRange{
		MinTimestamp: 1,
		MaxTimestamp: 2,
	}, "[1970-01-01T00:00:00.001Z..1970-01-01T00:00:00.002Z]")
	f(TimeRange{
		MinTimestamp: time.Date(2024, 9, 6, 0, 0, 0, 000, time.UTC).UnixMilli(),
		MaxTimestamp: time.Date(2024, 9, 7, 0, 0, 0, 000, time.UTC).UnixMilli() - 1,
	}, "[2024-09-06T00:00:00Z..2024-09-06T23:59:59.999Z]")
}

func TestTimeRange_fromPartitionTimestamp(t *testing.T) {
	f := func(ts int64, want TimeRange) {
		var got TimeRange
		got.fromPartitionTimestamp(ts)
		if got != want {
			t.Errorf("unexpected time range: got %v, want %v", &got, &want)
		}
	}

	ts := time.Date(2025, 3, 23, 14, 07, 56, 999_999_999, time.UTC).UnixMilli()
	f(ts, TimeRange{
		MinTimestamp: time.Date(2025, 3, 23, 14, 5, 0, 0, time.UTC).UnixMilli(),
		MaxTimestamp: time.Date(2025, 3, 23, 14, 9, 59, 999_000_000, time.UTC).UnixMilli(),
	})
}

func TestIsFirstHourOfDay(t *testing.T) {
	f := func(tt time.Time, want bool) {
		got := isFirstHourOfDay(uint64(tt.Unix()))
		if got != want {
			t.Fatalf("isFirstHourOfDay(%v) unexpected result: got %t, want %t", tt, got, want)
		}

	}

	firstHourOfDay := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	f(firstHourOfDay, true)
	firstHourOfDay = time.Date(2000, 1, 1, 0, 12, 34, 56789, time.UTC)
	f(firstHourOfDay, true)
	firstHourOfDay = time.Date(2000, 1, 1, 0, 59, 59, 999_999_999, time.UTC)
	f(firstHourOfDay, true)

	secondHourOfDay := time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)
	f(secondHourOfDay, false)

	sixthHourOfDay := time.Date(2000, 1, 1, 5, 0, 0, 0, time.UTC)
	f(sixthHourOfDay, false)

	lastHourOfDay := time.Date(2000, 1, 1, 23, 59, 59, 999_999_999, time.UTC)
	f(lastHourOfDay, false)
}

func TestMaxUnixMilli(t *testing.T) {
	lastFuturePtMaxTime := time.Date(2262, 3, 31, 23, 59, 59, 999_000_000, time.UTC)
	if got, want := lastFuturePtMaxTime.UnixMilli(), int64(maxUnixMilli); got != want {
		t.Fatalf("unexpected maxUnixMilli: got %d, want %d", got, want)
	}
}
