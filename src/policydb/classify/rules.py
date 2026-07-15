from __future__ import annotations

TOPIC_RULES = {
    "宏观政策": ["宏观调控", "稳增长", "扩大内需", "经济工作会议"],
    "房地产市场": ["房地产市场", "楼市", "商品住房", "商品房市场"],
    "城市更新": ["城市更新"],
    "城中村改造": ["城中村"],
    "老旧小区改造": ["老旧小区"],
    "危旧房改造": ["危旧房"],
    "项目白名单": ["白名单"],
    "保交楼": ["保交楼"],
    "商品房收储": ["收储", "收购存量商品房"],
    "住房以旧换新": ["以旧换新"],
    "公积金": ["公积金"],
    "购房补贴": ["购房补贴", "房票"],
    "人才住房": ["人才"],
    "限购": ["限购"],
    "限售": ["限售"],
    "限贷": ["限贷", "首付"],
    "限价": ["限价"],
    "住房租赁": ["住房租赁", "保租房"],
    "住房保障": ["住房保障", "保障性住房", "保障房"],
    "保障性租赁住房": ["保障性租赁住房", "保租房"],
    "房企融资": ["房企融资", "房地产融资", "融资协调", "授信", "开发贷"],
    "房地产债务": ["房地产债务", "债务风险", "债券违约"],
    "REITs": ["REITs", "不动产投资信托"],
    "商业住房贷款": ["商业住房贷款", "个人住房贷款", "房贷利率"],
    "税费政策": ["契税", "房产税", "增值税", "税费优惠"],
    "落户政策": ["落户", "户籍"],
    "土地供应": ["土地供应", "供地"],
    "存量土地盘活": ["存量土地", "盘活"],
    "土地竞拍": ["土地竞拍", "土拍", "集中供地", "竞买保证金"],
    "预售资金监管": ["预售资金"],
    "二手房参考价": ["二手房参考价"],
    "住房品质": ["好房子", "住房品质", "绿色住宅"],
    "物业管理": ["物业管理", "物业服务"],
    "房企支持": ["房地产企业", "房企", "开发企业"],
}


def classify(text: str) -> list[dict]:
    result = []
    for topic, terms in TOPIC_RULES.items():
        matched = next((term for term in terms if term in text), None)
        if matched:
            pos = text.find(matched)
            result.append(
                {
                    "topic": topic,
                    "confidence": 0.9,
                    "evidence_excerpt": text[max(0, pos - 30) : pos + len(matched) + 50],
                }
            )
    return result or [{"topic": "其他", "confidence": 0.5, "evidence_excerpt": text[:80]}]


def infer_direction(text: str) -> str:
    if any(x in text for x in ("取消限", "放宽", "降低首付", "下调", "提高额度", "优化调整")):
        return "loosening"
    if any(x in text for x in ("收紧", "上调首付", "暂停", "不得购买")):
        return "tightening"
    if any(x in text for x in ("支持", "补贴", "保障", "促进")):
        return "supportive"
    return "unknown"
