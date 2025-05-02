package greatvaluekafka

import (
	"hash/fnv"
	"time"
)

func HashToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func IsExpired(ttlMs int, timestamp int64) bool {
	return time.Now().UnixMilli()-timestamp > int64(ttlMs)
}
