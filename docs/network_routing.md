# 政府网站网络分流

CRPD 使用两套互不共享会话的客户端：

- `GovernmentDirectClient`：政府官网、PDF、附件、列表页和健康检查。固定
  `trust_env=False`、`follow_redirects=False`、`verify=True`，逐跳校验政府域名。
- `AIProxyClient`：SiliconFlow、GLM、Embedding、Rerank。允许
  `trust_env=True`，继续使用本机代理。

严禁用 `verify=False` 绕过证书。HTTP种子首先按原协议请求；只有服务器明确重定向才尝试HTTPS。
Windows TLS栈不兼容时可对已核验政府域名使用 `curl.exe --noproxy "*"` 的Schannel回退，
仍然保留证书校验、最终URL、状态码和内容哈希。

诊断：

```powershell
.\.venv\Scripts\policydb.exe network diagnose --city "南京市"
.\.venv\Scripts\policydb.exe network diagnose --url "https://example.gov.cn/"
.\.venv\Scripts\policydb.exe network probe-proxy --url "https://github.com"
.\.venv\Scripts\policydb.exe network probe-direct --url "https://www.nanjing.gov.cn/"
.\.venv\Scripts\policydb.exe network compare --url "https://www.nanjing.gov.cn/"
.\.venv\Scripts\policydb.exe network audit-sources --city "南京市" --enabled-only
```

若默认请求失败、完全直连成功，代理客户端应将
`DOMAIN-SUFFIX,gov.cn,DIRECT` 放在通用代理规则之前。网络失败窗口只能标为
`partial_network`。

`198.18.0.0/15` 是代理软件常用的 Fake-IP 保留网段。只要政府域名解析到该网段，报告必须标记为 `tun_intercepted`，不得把 TLS 失败写成来源站点不健康。Vortex/SakuraCat 需将 `gov.cn` 同时加入 DIRECT 与 Fake-IP 排除后，再执行真实入口健康检查。

本地启动器分离两类进程：`CRPD_Run_Proxy_Process.ps1` 只为 AI/搜索设置 `CRPD_*_PROXY_URL`；`CRPD_Run_Direct_Government_Process.ps1` 清空标准代理变量并强制政府抓取直连。代理地址和凭据不会写入诊断报告。
