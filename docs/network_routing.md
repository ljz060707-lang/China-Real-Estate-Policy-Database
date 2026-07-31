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
```

若默认请求失败、完全直连成功，代理客户端应将
`DOMAIN-SUFFIX,gov.cn,DIRECT` 放在通用代理规则之前。网络失败窗口只能标为
`partial_network`。
