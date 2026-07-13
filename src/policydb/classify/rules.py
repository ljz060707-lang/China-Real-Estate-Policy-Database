from __future__ import annotations

TOPIC_RULES = {
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
    "房企融资": ["融资", "授信"],
    "住房租赁": ["住房租赁", "保租房"],
    "土地供应": ["土地供应", "供地"],
    "存量土地盘活": ["存量土地", "盘活"],
    "预售资金监管": ["预售资金"],
    "二手房参考价": ["二手房参考价"],
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
