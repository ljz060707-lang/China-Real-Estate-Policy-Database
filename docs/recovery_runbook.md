# 中断与失败恢复

1. 先运行 `progress status --city <城市>` 查看失败分片；
2. 网络异常运行 `network diagnose --city <城市>`；
3. 来源缺失运行 `sources candidates` 与 `sources verify-candidates`；
4. 可重试错误使用 `crawl exhaustive-retry --city <城市>`；
5. 正常中断使用 `crawl exhaustive-resume --city <城市>`；
6. 归档、AI或去重未闭环时仅运行相应增量后处理，不重抓已归档正文。

所有分片以稳定 `shard_id` 幂等合并。Raw与既有文档版本不可变，不删除历史错误。
无法访问D盘时停止正式写入，不回退到C盘。
