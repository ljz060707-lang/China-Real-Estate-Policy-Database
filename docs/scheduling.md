# GitHub Actions定时更新

`.github/workflows/update_policy.yml` 每天北京时间02:17运行，并使用
`concurrency` 防止多个更新任务互相覆盖。GitHub当前支持在schedule中使用IANA时区，因此工作流直接声明 `Asia/Shanghai`。

流程为增量发现与抓取、确定性解析、可选GLM、标准化、七大库整理、105城市面板、验证、测试、报告和滚动PR。固定分支为 `automation/policy-updates`，只有文件实际变化时才更新PR。

仓库设置中添加：

1. Settings → Secrets and variables → Actions；
2. 新建Repository secret `GLM_API_KEY`；
3. 可选Repository variable `GLM_MODEL`；
4. 不要把secret值粘贴到workflow、issue或日志。

自动工作流不自动合并PR。严重质量问题、105城市数量不等于105、测试失败或validate失败都会在创建数据更新前终止。

