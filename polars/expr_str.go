package polars

import (
	pb "github.com/isesword/polars-go/proto"
)

// StrLenBytes 计算字符串字节长度
func (e Expr) StrLenBytes() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrLenBytes{
				StrLenBytes: &pb.StringFunction{Expr: e.inner},
			},
		},
	}
}

// StrLenChars 计算字符串字符长度
func (e Expr) StrLenChars() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrLenChars{
				StrLenChars: &pb.StringFunction{Expr: e.inner},
			},
		},
	}
}

// StrContains 判断字符串是否包含子串/正则
func (e Expr) StrContains(pattern string, literal bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrContains{
				StrContains: &pb.StringContains{
					Expr:    e.inner,
					Pattern: pattern,
					Literal: literal,
				},
			},
		},
	}
}

// StrStartsWith 判断字符串是否以指定前缀开头
func (e Expr) StrStartsWith(prefix string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrStartsWith{
				StrStartsWith: &pb.StringStartsWith{
					Expr:   e.inner,
					Prefix: prefix,
				},
			},
		},
	}
}

// StrEndsWith 判断字符串是否以指定后缀结尾
func (e Expr) StrEndsWith(suffix string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrEndsWith{
				StrEndsWith: &pb.StringEndsWith{
					Expr:   e.inner,
					Suffix: suffix,
				},
			},
		},
	}
}

// StrExtract 使用正则提取分组
func (e Expr) StrExtract(pattern string, groupIndex uint32) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrExtract{
				StrExtract: &pb.StringExtract{
					Expr:       e.inner,
					Pattern:    pattern,
					GroupIndex: groupIndex,
				},
			},
		},
	}
}

// StrReplace 替换第一个匹配
func (e Expr) StrReplace(pattern string, value string, literal bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrReplace{
				StrReplace: &pb.StringReplace{
					Expr:    e.inner,
					Pattern: pattern,
					Value:   value,
					Literal: literal,
				},
			},
		},
	}
}

// StrReplaceAll 替换全部匹配
func (e Expr) StrReplaceAll(pattern string, value string, literal bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrReplaceAll{
				StrReplaceAll: &pb.StringReplace{
					Expr:    e.inner,
					Pattern: pattern,
					Value:   value,
					Literal: literal,
				},
			},
		},
	}
}

// StrToLowercase 转为小写
func (e Expr) StrToLowercase() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrToLowercase{
				StrToLowercase: &pb.StringFunction{Expr: e.inner},
			},
		},
	}
}

// StrToUppercase 转为大写
func (e Expr) StrToUppercase() Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrToUppercase{
				StrToUppercase: &pb.StringFunction{Expr: e.inner},
			},
		},
	}
}

// StrStripChars 修剪指定字符（空字符串表示空白字符）
func (e Expr) StrStripChars(chars string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrStripChars{
				StrStripChars: &pb.StringStripChars{
					Expr:  e.inner,
					Chars: chars,
				},
			},
		},
	}
}

// StrSlice 字符串切片（length 可选）
func (e Expr) StrSlice(offset int64, length ...uint64) Expr {
	var lengthPtr *uint64
	if len(length) > 0 {
		l := length[0]
		lengthPtr = &l
	}

	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrSlice{
				StrSlice: &pb.StringSlice{
					Expr:   e.inner,
					Offset: offset,
					Length: lengthPtr,
				},
			},
		},
	}
}

// StrSplit 按分隔符拆分
func (e Expr) StrSplit(by string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrSplit{
				StrSplit: &pb.StringSplit{
					Expr: e.inner,
					By:   by,
				},
			},
		},
	}
}

// StrPadStart 左侧填充
func (e Expr) StrPadStart(length uint64, fillChar string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrPadStart{
				StrPadStart: &pb.StringPad{
					Expr:     e.inner,
					Length:   length,
					FillChar: fillChar,
				},
			},
		},
	}
}

// StrPadEnd 右侧填充
func (e Expr) StrPadEnd(length uint64, fillChar string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrPadEnd{
				StrPadEnd: &pb.StringPad{
					Expr:     e.inner,
					Length:   length,
					FillChar: fillChar,
				},
			},
		},
	}
}

// StrStripPrefix removes a prefix if present.
func (e Expr) StrStripPrefix(prefix string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrStripPrefix{
				StrStripPrefix: &pb.StringAffix{
					Expr:  e.inner,
					Value: prefix,
				},
			},
		},
	}
}

// StrStripSuffix removes a suffix if present.
func (e Expr) StrStripSuffix(suffix string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrStripSuffix{
				StrStripSuffix: &pb.StringAffix{
					Expr:  e.inner,
					Value: suffix,
				},
			},
		},
	}
}

// StrExtractAll extracts all regex matches into a list column.
func (e Expr) StrExtractAll(pattern string) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrExtractAll{
				StrExtractAll: &pb.StringPattern{
					Expr:    e.inner,
					Pattern: pattern,
				},
			},
		},
	}
}

// StrCountMatches counts regex or literal matches.
func (e Expr) StrCountMatches(pattern string, literal bool) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrCountMatches{
				StrCountMatches: &pb.StringContains{
					Expr:    e.inner,
					Pattern: pattern,
					Literal: literal,
				},
			},
		},
	}
}

// StrReplaceN replaces the first n matches.
func (e Expr) StrReplaceN(pattern string, value string, literal bool, n int64) Expr {
	return Expr{
		inner: &pb.Expr{
			Kind: &pb.Expr_StrReplaceN{
				StrReplaceN: &pb.StringReplaceN{
					Expr:    e.inner,
					Pattern: pattern,
					Value:   value,
					Literal: literal,
					N:       n,
				},
			},
		},
	}
}
