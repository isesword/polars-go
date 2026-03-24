package polars

import (
	pb "github.com/isesword/polars-go-bridge/proto"
)

// DtYear extracts the year from a Date/Datetime expression.
func (e Expr) DtYear() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtYear{
				DtYear: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtMonth extracts the month from a Date/Datetime expression.
func (e Expr) DtMonth() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtMonth{
				DtMonth: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtDay extracts the day from a Date/Datetime expression.
func (e Expr) DtDay() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtDay{
				DtDay: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtWeekday extracts the ISO weekday from a Date/Datetime expression.
func (e Expr) DtWeekday() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtWeekday{
				DtWeekday: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtHour extracts the hour from a Datetime/Time expression.
func (e Expr) DtHour() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtHour{
				DtHour: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtMinute extracts the minute from a Datetime/Time expression.
func (e Expr) DtMinute() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtMinute{
				DtMinute: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtSecond extracts the second from a Datetime/Time expression.
func (e Expr) DtSecond() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtSecond{
				DtSecond: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtMonthStart rolls backward to the first day of the month.
func (e Expr) DtMonthStart() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtMonthStart{
				DtMonthStart: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// DtMonthEnd rolls forward to the last day of the month.
func (e Expr) DtMonthEnd() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_DtMonthEnd{
				DtMonthEnd: &pb.TemporalUnary{Expr: e.inner},
			},
		},
	}
}

// StrToDate parses a string column into a Date using the provided format.
func (e Expr) StrToDate(format string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrToDate{
				StrToDate: &pb.TemporalParse{
					Expr:   e.inner,
					Format: format,
				},
			},
		},
	}
}

// StrToDatetime parses a string column into a Datetime using the provided format.
func (e Expr) StrToDatetime(format string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrToDatetime{
				StrToDatetime: &pb.TemporalParse{
					Expr:   e.inner,
					Format: format,
				},
			},
		},
	}
}

// StrToTime parses a string column into a Time using the provided format.
func (e Expr) StrToTime(format string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrToTime{
				StrToTime: &pb.TemporalParse{
					Expr:   e.inner,
					Format: format,
				},
			},
		},
	}
}
