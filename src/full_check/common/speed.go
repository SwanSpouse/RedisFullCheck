package common

import "time"

type Qos struct {
	Bucket chan struct{}

	limit int // qps
	close bool
}

func StartQoS(limit int) *Qos {
	q := new(Qos)
	q.limit = limit
	q.Bucket = make(chan struct{}, limit)

	go q.timer()
	return q
}

func (q *Qos) timer() {
	// 每秒进行一次检查
	for range time.NewTicker(1 * time.Second).C {
		if q.close {
			return
		}
		// 使用的是一个chan来进行限频
		// 每秒放进来limit个操作；其实这里有问题；不是严格的每秒limit次
		for i := 0; i < q.limit; i++ {
			select {
			case q.Bucket <- struct{}{}:
			default:
				// break if bucket if full
				break
			}
		}
	}
}

func (q *Qos) Close() {
	q.close = true
}
