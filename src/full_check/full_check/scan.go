package full_check

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/alibaba/RedisFullCheck/src/full_check/client"
	"github.com/alibaba/RedisFullCheck/src/full_check/common"

	"github.com/jinzhu/copier"
)

// 从redis里面开始scan key
// 可惜的是我们的scan已经被禁用了。。。
func (p *FullCheck) ScanFromSourceRedis(allKeys chan<- []*common.Key) {
	var wg sync.WaitGroup
	// 在这里开始搞物理dblist
	// 启动这么多个goroutine 并发
	wg.Add(len(p.sourcePhysicalDBList))
	for idx := 0; idx < len(p.sourcePhysicalDBList); idx++ {
		// use goroutine to run db concurrently
		go func(index int) {
			defer wg.Done()
			cursor := 0
			var sourceClient client.RedisClient
			var err error

			// build client
			if p.SourceHost.IsCluster() {
				var singleHost client.RedisHost
				copier.Copy(&singleHost, &p.SourceHost)
				// set single host address
				singleHost.Addr = []string{singleHost.Addr[index]}
				singleHost.DBType = common.TypeDB
				// build client by single db
				if sourceClient, err = client.NewRedisClient(singleHost, p.currentDB); err != nil {
					panic(common.Logger.Critical(err))
				}
			} else {
				sourceClient, err = client.NewRedisClient(p.SourceHost, p.currentDB)
				if err != nil {
					panic(common.Logger.Errorf("create redis client with host[%v] db[%v] error[%v]",
						p.SourceHost, p.currentDB, err))
				}
			}
			defer sourceClient.Close()

			common.Logger.Infof("build connection[%v]", sourceClient.String())

			for {
				var reply interface{}
				var err error
				// 在这里判断BType的类型，执行不同的命令把key从Redis里面利用scan命令取出来
				// 如果有一天可以支持头条了，在这里加一个Type就行了
				switch p.SourceHost.DBType {
				case common.TypeTencentProxy:
					reply, err = sourceClient.Do("scan", cursor, "count", p.BatchCount, p.sourcePhysicalDBList[index])
				case common.TypeDB:
					fallthrough
				case common.TypeCluster:
					reply, err = sourceClient.Do("scan", cursor, "count", p.BatchCount)
				case common.TypeAliyunProxy:
					reply, err = sourceClient.Do("iscan", index, cursor, "count", p.BatchCount)
				}
				if err != nil {
					panic(common.Logger.Critical(err))
				}
				// 在这里拿到返回值
				replyList, ok := reply.([]interface{})
				if ok == false || len(replyList) != 2 {
					panic(common.Logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.BatchCount, reply))
				}

				bytes, ok := replyList[0].([]byte)
				if ok == false {
					panic(common.Logger.Criticalf("scan %d count %d failed, result: %+v", cursor, p.BatchCount, reply))
				}
				// TODO @LiMingji 这里查一下RedisScan命令的返回结果是什么样的
				cursor, err = strconv.Atoi(string(bytes))
				if err != nil {
					panic(common.Logger.Critical(err))
				}

				keylist, ok := replyList[1].([]interface{})
				if ok == false {
					panic(common.Logger.Criticalf("scan failed, result: %+v", reply))
				}
				keysInfo := make([]*common.Key, 0, len(keylist))
				for _, value := range keylist {
					bytes, ok = value.([]byte)
					if ok == false {
						panic(common.Logger.Criticalf("scan failed, result: %+v", reply))
					}

					// check filter list
					if common.CheckFilter(p.FilterTree, bytes) == false {
						continue
					}

					keysInfo = append(keysInfo, &common.Key{
						Key:          bytes,
						Tp:           common.EndKeyType,
						ConflictType: common.EndConflict,
					})
					// common.Logger.Debugf("read key: %v", string(bytes))
				}
				p.IncrScanStat(len(keysInfo))
				allKeys <- keysInfo

				if cursor == 0 {
					break
				}
			} // end for{}
		}(idx)
	} // end fo for idx := 0; idx < p.sourcePhysicalDBList; idx++

	wg.Wait()
	close(allKeys)
}

func (p *FullCheck) ScanFromDB(allKeys chan<- []*common.Key) {
	// 获取上一次结果存储的table
	conflictKeyTableName, conflictFieldTableName := p.GetLastResultTable()

	keyQuery := fmt.Sprintf("select id,key,type,conflict_type,source_len,target_len from %s where id>? and db=%d limit %d", conflictKeyTableName, p.currentDB, p.BatchCount)
	keyStatm, err := p.db[p.times-1].Prepare(keyQuery)
	if err != nil {
		panic(common.Logger.Error(err))
	}
	defer keyStatm.Close()

	fieldQuery := fmt.Sprintf("select field,conflict_type from %s where key_id=?", conflictFieldTableName)
	fieldStatm, err := p.db[p.times-1].Prepare(fieldQuery)
	if err != nil {
		panic(common.Logger.Error(err))
	}
	defer fieldStatm.Close()

	var startId int64 = 0
	for {
		rows, err := keyStatm.Query(startId)
		if err != nil {
			panic(common.Logger.Error(err))
		}
		keyInfo := make([]*common.Key, 0, p.BatchCount)
		for rows.Next() {
			var key, keytype, conflictType string
			var id, source_len, target_len int64
			err = rows.Scan(&id, &key, &keytype, &conflictType, &source_len, &target_len)
			if err != nil {
				panic(common.Logger.Error(err))
			}
			oneKeyInfo := &common.Key{
				Key:          []byte(key),
				Tp:           common.NewKeyType(keytype),
				ConflictType: common.NewConflictType(conflictType),
				SourceAttr:   common.Attribute{ItemCount: source_len},
				TargetAttr:   common.Attribute{ItemCount: target_len},
			}
			if oneKeyInfo.Tp == common.EndKeyType {
				panic(common.Logger.Errorf("invalid type from table %s: key=%s type=%s ", conflictKeyTableName, key, keytype))
			}
			if oneKeyInfo.ConflictType == common.EndConflict {
				panic(common.Logger.Errorf("invalid conflict_type from table %s: key=%s conflict_type=%s ", conflictKeyTableName, key, conflictType))
			}

			if oneKeyInfo.Tp != common.StringKeyType {
				oneKeyInfo.Field = make([]common.Field, 0, 10)
				rowsField, err := fieldStatm.Query(id)
				if err != nil {
					panic(common.Logger.Error(err))
				}
				for rowsField.Next() {
					var field, conflictType string
					err = rowsField.Scan(&field, &conflictType)
					if err != nil {
						panic(common.Logger.Error(err))
					}
					oneField := common.Field{
						Field:        []byte(field),
						ConflictType: common.NewConflictType(conflictType),
					}
					if oneField.ConflictType == common.EndConflict {
						panic(common.Logger.Errorf("invalid conflict_type from table %s: field=%s type=%s ", conflictFieldTableName, field, conflictType))
					}
					oneKeyInfo.Field = append(oneKeyInfo.Field, oneField)
				}
				if err := rowsField.Err(); err != nil {
					panic(common.Logger.Error(err))
				}
				rowsField.Close()
			}
			keyInfo = append(keyInfo, oneKeyInfo)
			if startId < id {
				startId = id
			}
		} // rows.Next
		if err := rows.Err(); err != nil {
			panic(common.Logger.Error(err))
		}
		rows.Close()
		// 结束
		if len(keyInfo) == 0 {
			close(allKeys)
			break
		}
		p.IncrScanStat(len(keyInfo))
		allKeys <- keyInfo
	} // for{}
}
