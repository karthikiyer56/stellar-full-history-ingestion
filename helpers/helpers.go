package helpers

import (
	"fmt"
	"time"
)

func ParseMonth(month string) (int, error) {
	// Parse YYYY-MM format
	t, err := time.Parse("2006-01", month)
	if err != nil {
		return 0, err
	}

	// Convert to YYYYMM integer
	yearMonth := t.Year()*100 + int(t.Month())
	return yearMonth, nil
}

func FormatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	} else if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
