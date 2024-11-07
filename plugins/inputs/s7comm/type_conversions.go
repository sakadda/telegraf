package s7comm

import (
	"encoding/binary"
	"math"

	"github.com/robinson/gos7"
)

var helper = &gos7.Helper{}

func determineConversion(dtype string) converterFunc {
	switch dtype {
	case "X":
		return func(buf []byte) interface{} {
			return buf[0] != 0
		}
	case "B":
		return func(buf []byte) interface{} {
			return buf[0]
		}
	case "C":
		return func(buf []byte) interface{} {
			return string(buf[0])
		}
	case "S":
		return func(buf []byte) interface{} {
			if len(buf) <= 2 {
				return ""
			}
			// Get the length of the encoded string
			length := int(buf[1])
			// Clip the string if we do not fill the whole buffer
			if length < len(buf)-2 {
				return string(buf[2 : 2+length])
			}
			return string(buf[2:])
		}
	case "W":
		return func(buf []byte) interface{} {
			return binary.BigEndian.Uint16(buf)
		}
	case "I":
		return func(buf []byte) interface{} {
			return int16(binary.BigEndian.Uint16(buf))
		}
	case "DW":
		return func(buf []byte) interface{} {
			return binary.BigEndian.Uint32(buf)
		}
	case "DI":
		return func(buf []byte) interface{} {
			return int32(binary.BigEndian.Uint32(buf))
		}
	case "R":
		return func(buf []byte) interface{} {
			x := binary.BigEndian.Uint32(buf)
			return math.Float32frombits(x)
		}
	case "RR":
		return func(buf []byte) interface{} {
			x := binary.BigEndian.Uint64(buf)
			return math.Round(math.Float64frombits(x)*100) / 100
		}
	case "DT":
		return func(buf []byte) interface{} {
			return helper.GetDateTimeAt(buf, 0).UnixNano()
		}
	case "LR":
        return func(b []byte) interface{} {
            if len(b) != 8 {
                return nil
            }
            high := binary.BigEndian.Uint32(b[:4])
            low := binary.BigEndian.Uint32(b[4:])
            combined := uint64(high)<<32 | uint64(low)
            return math.Float64frombits(combined)
        }
	case "LI":
		return func(buf []byte) interface{} {
			if len(buf) != 8 {
				return ""
			}
			return int64(binary.BigEndian.Uint64(buf))
		}
	}

	panic("Unknown type! Please file an issue on https://github.com/influxdata/telegraf including your config.")
}
