package shardctrler

import "sort"

// 浅拷贝
func (sc *ShardCtrler) execQueryCmd(op *Op) {

	// 是-1就返回最近的配置
	if op.Num == -1 || op.Num >= len(sc.configs) {
		op.Cfg = sc.configs[len(sc.configs)-1]
		DPrintf(1111, "[节点%d执行query之后最新配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
			sc.me, len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
		return
	}
	op.Cfg = sc.configs[op.Num]
	DPrintf(1111, "[节点%d执行query获取版本号为%d的配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
		sc.me, op.Num, len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)

}

func (sc *ShardCtrler) execMoveCmd(op *Op) {
	sc.MoveShard(op.Shard, op.GID)
}
func (sc *ShardCtrler) MoveShard(shardId int, GID int) {
	DPrintf(1111, "[Move前的配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
		len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
	DPrintf(111, "move args: %d and %d", shardId, GID)
	// 获取最新的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards, // 注意: 这里我们只是复制了分片的分配，后面会更新目标分片
		Groups: make(map[int][]string),
	}

	// 复制复制组信息
	for gid, servers := range oldConfig.Groups {
		copiedServers := make([]string, len(servers))
		copy(copiedServers, servers)
		newConfig.Groups[gid] = copiedServers
	}

	// 移动目标分片到新的复制组
	if _, exists := newConfig.Groups[GID]; exists {
		newConfig.Shards[shardId] = GID
	} else {
		// 如果目标 GID 不存在，不执行移动操作。
		return
	}

	// 将新配置添加到配置列表
	sc.configs = append(sc.configs, newConfig)

	// 可以在这里输出新配置的信息，或者执行其他后续操作
	DPrintf(1111, "[Move后最新配置信息]: len(sc.configs)：%v, sc.configs[len(sc.configs)-1].Num： %v, sc.configs[len(sc.configs)-1].Shards： %v, sc.configs[len(sc.configs)-1].Groups： %v",
		len(sc.configs), sc.configs[len(sc.configs)-1].Num, sc.configs[len(sc.configs)-1].Shards, sc.configs[len(sc.configs)-1].Groups)
}

// 状态机执行join命令的时候
func (sc *ShardCtrler) execJoinCmd(op *Op) {
	//sc.mu.Lock()
	//defer sc.mu.Unlock()
	sc.RebalanceShardsForJoin(op.Servers)
	//sc.configs[newConfigIndex] = newConfig
}

func (sc *ShardCtrler) RebalanceShardsForJoin(newGroups map[int][]string) {
	// 获取最新的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards, // 直接复制旧配置的分片分配
		Groups: make(map[int][]string),
	}

	// 合并旧的和新的复制组
	for gid, servers := range oldConfig.Groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	for gid, servers := range newGroups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newConfig.Groups[gid] = newServers
	}

	// 如果没有复制组，所有分片都应该分配给GID 0
	if len(newConfig.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
		sc.configs = append(sc.configs, newConfig)
		return
	}

	// 计算每个GID当前拥有的分片数
	gidToShards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 计算每个GID应该拥有的分片数
	totalShards := NShards
	totalGroups := len(newConfig.Groups)
	shardsPerGroup := totalShards / totalGroups
	extraShards := totalShards % totalGroups

	// 获取排序后的GID列表
	gids := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 计算每个GID应该拥有的分片数
	targetCounts := make(map[int]int)
	for i, gid := range gids {
		targetCounts[gid] = shardsPerGroup
		if i < extraShards {
			targetCounts[gid]++
		}
	}

	// 重新分配分片
	// 1. 先处理没有在新配置中的GID的分片
	var shardsToReassign []int
	for gid, shards := range gidToShards {
		if gid != 0 && targetCounts[gid] == 0 { // GID不在新配置中
			shardsToReassign = append(shardsToReassign, shards...)
			delete(gidToShards, gid)
		}
	}

	// 2. 处理GID 0的分片(如果有的话)
	if shards, exists := gidToShards[0]; exists && len(newConfig.Groups) > 0 {
		shardsToReassign = append(shardsToReassign, shards...)
		delete(gidToShards, 0)
	}

	// 3. 处理有过多分片的GID
	for _, gid := range gids {
		shards := gidToShards[gid]
		for len(shards) > targetCounts[gid] {
			shardsToReassign = append(shardsToReassign, shards[len(shards)-1])
			shards = shards[:len(shards)-1]
		}
		gidToShards[gid] = shards
	}

	// 4. 将需要重新分配的分片分配给需要更多分片的GID
	sort.Ints(shardsToReassign) // 确保分片按顺序分配，使结果确定
	for _, shard := range shardsToReassign {
		for _, gid := range gids {
			if len(gidToShards[gid]) < targetCounts[gid] {
				gidToShards[gid] = append(gidToShards[gid], shard)
				newConfig.Shards[shard] = gid
				break
			}
		}
	}

	sc.configs = append(sc.configs, newConfig)
}

// 状态机执行leave命令的时候
func (sc *ShardCtrler) execLeaveCmd(op *Op) {

	sc.RebalanceShardsForLeave(op.GIDs)
}

func (sc *ShardCtrler) RebalanceShardsForLeave(removedGIDs []int) {
	// 获取最新的配置
	oldConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards, // 直接复制旧配置的分片分配
		Groups: make(map[int][]string),
	}

	// 将 removedGIDs 转换为 map 以便快速查找
	removedGIDMap := make(map[int]bool)
	for _, gid := range removedGIDs {
		removedGIDMap[gid] = true
	}

	// 复制不需要移除的复制组
	for gid, servers := range oldConfig.Groups {
		if !removedGIDMap[gid] {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 如果没有复制组，所有分片都应该分配给GID 0
	if len(newConfig.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = 0
		}
		sc.configs = append(sc.configs, newConfig)
		return
	}

	// 计算每个GID当前拥有的分片数
	gidToShards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 计算每个GID应该拥有的分片数
	totalShards := NShards
	totalGroups := len(newConfig.Groups)
	shardsPerGroup := totalShards / totalGroups
	extraShards := totalShards % totalGroups

	// 获取排序后的GID列表
	gids := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// 计算每个GID应该拥有的分片数
	targetCounts := make(map[int]int)
	for i, gid := range gids {
		targetCounts[gid] = shardsPerGroup
		if i < extraShards {
			targetCounts[gid]++
		}
	}

	// 收集需要重新分配的分片
	var shardsToReassign []int
	for gid, shards := range gidToShards {
		if removedGIDMap[gid] || gid == 0 {
			shardsToReassign = append(shardsToReassign, shards...)
			delete(gidToShards, gid)
		}
	}

	// 按顺序分配这些分片
	sort.Ints(shardsToReassign)
	for _, shard := range shardsToReassign {
		var targetGID int = -1
		minShards := NShards + 1

		for _, gid := range gids {
			currentShards := len(gidToShards[gid])
			if currentShards < targetCounts[gid] && currentShards < minShards {
				targetGID = gid
				minShards = currentShards
			}
		}

		if targetGID != -1 {
			gidToShards[targetGID] = append(gidToShards[targetGID], shard)
			newConfig.Shards[shard] = targetGID
		}
	}

	sc.configs = append(sc.configs, newConfig)
}
