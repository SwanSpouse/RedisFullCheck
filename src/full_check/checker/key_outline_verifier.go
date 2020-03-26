package checker

import (
	"sync"

	"github.com/alibaba/RedisFullCheck/src/full_check/client"
	"github.com/alibaba/RedisFullCheck/src/full_check/common"
	"github.com/alibaba/RedisFullCheck/src/full_check/metric"
)

type KeyOutlineVerifier struct {
	VerifierBase
}

func NewKeyOutlineVerifier(stat *metric.Stat, param *FullCheckParameter) *KeyOutlineVerifier {
	return &KeyOutlineVerifier{VerifierBase{stat, param}}
}

func (p *KeyOutlineVerifier) FetchKeys(keyInfo []*common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	// fetch type
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sourceKeyTypeStr, err := sourceClient.PipeTypeCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		for i, t := range sourceKeyTypeStr {
			keyInfo[i].Tp = common.NewKeyType(t)
			/*
			 * Bugfix: see https://github.com/alibaba/RedisFullCheck/issues/74.
			 * It will skip the conflict key check because keyInfo[i].SourceAttr.ItemCount is zero here.
			 * Unlike the FetchTypeAndLen method in full_value_verifier, which will assign a non -zero value of keylen to keyInfo[i].SourceAttr.ItemCount
			 * Refer to the VerifyOneGroupKeyInfo method for details.
			 */
			keyInfo[i].SourceAttr.ItemCount = 1
		}
		// TODO @LiMingji 如果上面发生panic这里就没有机会执行了。
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		targetKeyTypeStr, err := targetClient.PipeExistsCommand(keyInfo)
		if err != nil {
			panic(common.Logger.Critical(err))
		}
		// 这个t的值是exist命令的返回值，0、1两种结果
		for i, t := range targetKeyTypeStr {
			keyInfo[i].TargetAttr.ItemCount = t
		}
		// TODO @LiMingji 如果上面发生panic这里就没有机会执行了。
		wg.Done()
	}()

	wg.Wait()
}

// 对这一批的key进行校验
func (p *KeyOutlineVerifier) VerifyOneGroupKeyInfo(keyInfo []*common.Key, conflictKey chan<- *common.Key, sourceClient *client.RedisClient, targetClient *client.RedisClient) {
	p.FetchKeys(keyInfo, sourceClient, targetClient)

	// re-check ttl on the source side when key missing on the target side
	// 当target这边有，source没有的时候校验一下ttl
	p.RecheckTTL(keyInfo, sourceClient)

	// compare, filter
	for i := 0; i < len(keyInfo); i++ {
		// 在fetch type和之后的轮次扫描之间源端类型更改，不处理这种错误
		// 类型变了
		if keyInfo[i].SourceAttr.ItemCount == common.TypeChanged {
			continue
		}
		// key lack in target redis
		// 标记key在target里面缺失
		// iTemCount = 0 表示没有这个Key
		if keyInfo[i].TargetAttr.ItemCount == 0 && keyInfo[i].TargetAttr.ItemCount != keyInfo[i].SourceAttr.ItemCount {
			keyInfo[i].ConflictType = common.LackTargetConflict
			p.IncrKeyStat(keyInfo[i])
			// 发送到chan去处理
			conflictKey <- keyInfo[i]
		}
	} // end of for i := 0; i < len(keyInfo); i++
}
