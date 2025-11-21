# ============================================================
# 패널 공통 특성 문장 생성
# ============================================================
from flask import jsonify, current_app
from config import antropicLLM # 클로드 llm 가져오기


def makeCommon(panels):
    # 로깅
    current_app.logger.info(f"🔍 공통 특성 분석: {len(panels)}개 패널")

    # 공통 특성 키워드를 담을 배열    
    keyword_counter = {}
    
    for panel in panels:
        gender = panel.get('gender')
        if gender and gender != '무응답':
            keyword_counter[gender] = keyword_counter.get(gender, 0) + 1
        
        residence = panel.get('residence')
        if residence and residence != '무응답':
            keyword_counter[residence] = keyword_counter.get(residence, 0) + 1
        
        job = panel.get('job')
        if job and job != '무응답':
            keyword_counter[job] = keyword_counter.get(job, 0) + 1
        
        age = panel.get('age')
        if age:
            if age < 20:
                age_group = '10대'
            elif age < 30:
                age_group = '20대'
            elif age < 40:
                age_group = '30대'
            elif age < 50:
                age_group = '40대'
            elif age < 60:
                age_group = '50대'
            else:
                age_group = '60대 이상'
            keyword_counter[age_group] = keyword_counter.get(age_group, 0) + 1
        
        income = panel.get('personalIncome')
        if income and income != '무응답':
            keyword_counter[income] = keyword_counter.get(income, 0) + 1
    
    # 가장 많이 카운팅된 키워드
    top_keywords = sorted(
        keyword_counter.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:5]
    
    keywords = [
        {"keyword": k, "count": v} 
        for k, v in top_keywords
    ]
    
    total_count = len(panels)
    avg_age = sum(p.get('age', 0) for p in panels) / total_count if total_count > 0 else 0
    
    gender_dist = {}
    for p in panels:
        g = p.get('gender', '무응답')
        gender_dist[g] = gender_dist.get(g, 0) + 1
    
    residence_dist = {}
    for p in panels:
        r = p.get('residence', '무응답')
        if r != '무응답':
            residence_dist[r] = residence_dist.get(r, 0) + 1
    
    summary_prompt = f"""다음은 {total_count}명의 패널 데이터 분석 결과입니다:

        공통 특성 상위 5개:
        {chr(10).join([f'- {k["keyword"]}: {k["count"]}명' for k in keywords])}

        평균 나이: {avg_age:.1f}세
        성별 분포: {', '.join([f'{k} {v}명' for k, v in gender_dist.items()])}
        주요 거주지: {', '.join([f'{k} {v}명' for k, v in sorted(residence_dist.items(), key=lambda x: x[1], reverse=True)[:3]])}

        이 패널 집단의 특징을 2-3문장으로 자연스럽게 요약해주세요. 
        마케팅이나 타겟팅 관점에서 유용한 인사이트를 포함해주세요."""

    message = antropicLLM.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=512,
        messages=[
            {"role": "user", "content": summary_prompt}
        ]
    )
    
    summary = message.content[0].text.strip()
    
    current_app.logger.info(f"✅ 공통 특성 분석 완료: {len(keywords)}개 키워드")
    
    return jsonify({
        "keywords": keywords,
        "summary": summary
    })