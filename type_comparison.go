package rose

import (
	"strings"
	"time"
)

type str string
type integer int
type floating float64
type boolean bool
type date time.Time
type dateTime time.Time

func (s str) compare(p string, t comparisonType) bool {
	if t == equality {
		return strings.Compare(string(s), p) == 0
	} else if t == inequality {
		return strings.Compare(string(s), p) != 0
	} else if t == less {
		return strings.Compare(string(s), p) == -1
	} else if t == more {
		return strings.Compare(string(s), p) != 1
	} else if t == lessEqual {
		return strings.Compare(string(s), p) == -1 || strings.Compare(string(s), p) == 0
	} else if t == moreEqual {
		return strings.Compare(string(s), p) == 1 || strings.Compare(string(s), p) == 0
	}

	return false
}

func (s date) compare(p time.Time, t comparisonType) bool {
	if t == equality {
		return time.Time(s).Equal(p)
	} else if t == inequality {
		return !time.Time(s).Equal(p)
	} else if t == less {
		return time.Time(s).Before(p)
	} else if t == more {
		return time.Time(s).After(p)
	} else if t == lessEqual {
		return time.Time(s).Before(p) || time.Time(s).Equal(p)
	} else if t == moreEqual {
		return time.Time(s).After(p) || time.Time(s).Equal(p)
	}

	return false
}

func (s dateTime) compare(p time.Time, t comparisonType) bool {
	if t == equality {
		return time.Time(s).Equal(p)
	} else if t == inequality {
		return !time.Time(s).Equal(p)
	} else if t == less {
		return time.Time(s).Before(p)
	} else if t == more {
		return time.Time(s).After(p)
	} else if t == lessEqual {
		return time.Time(s).Before(p) || time.Time(s).Equal(p)
	} else if t == moreEqual {
		return time.Time(s).After(p) || time.Time(s).Equal(p)
	}

	return false
}

func (s integer) compare(p int, t comparisonType) bool {
	if t == equality {
		return int(s) == p
	} else if t == inequality {
		return int(s) != p
	} else if t == less {
		return int(s) < p
	} else if t == more {
		return int(s) > p
	} else if t == lessEqual {
		return int(s) <= p
	} else if t == moreEqual {
		return int(s) >= p
	}

	return false
}

func (s floating) compare(p float64, t comparisonType) bool {
	if t == equality {
		return float64(s) == p
	} else if t == inequality {
		return float64(s) != p
	} else if t == less {
		return float64(s) < p
	} else if t == more {
		return float64(s) > p
	} else if t == lessEqual {
		return float64(s) <= p
	} else if t == moreEqual {
		return float64(s) >= p
	}

	return false
}

func (s boolean) compare(p bool, t comparisonType) bool {
	if t == equality {
		return bool(s) == p
	} else if t == inequality {
		return bool(s) != p
	}

	return false
}
