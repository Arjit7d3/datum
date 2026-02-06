package internal

import "time"

type Time struct {
	Year  int64 `json:"year"`
	Month int64 `json:"month"`
	Day   int64 `json:"day"`
	Hour  int64 `json:"hour"`
	Min   int64 `json:"min"`
	Sec   int64 `json:"sec"`
	Ms    int64 `json:"ms"`
}

func NewTimeFromTime(t time.Time) Time {
	return Time{
		Year:  int64(t.Year()),
		Month: int64(t.Month()),
		Day:   int64(t.Day()),
		Hour:  int64(t.Hour()),
		Min:   int64(t.Minute()),
		Sec:   int64(t.Second()),
		Ms:    t.UnixMilli() % 1000,
	}
}

func NewTimeFromUnixMilli(t int64) Time {
	return NewTimeFromTime(time.UnixMilli(t))
}

func NewTime(year int64, month int64, day int64, hour int64, min int64, sec int64, ms int64) Time {
	return Time{
		Year:  year,
		Month: month,
		Day:   day,
		Hour:  hour,
		Min:   min,
		Sec:   sec,
		Ms:    ms,
	}
}

func (t *Time) ToGoTime() time.Time {
	return time.Date(int(t.Year), time.Month(t.Month), int(t.Day), int(t.Hour), int(t.Min), int(t.Sec), int(t.Ms)*1e6, time.UTC)
}

func (t *Time) ToUnixMilli() int64 {
	return t.ToGoTime().UnixMilli()
}
