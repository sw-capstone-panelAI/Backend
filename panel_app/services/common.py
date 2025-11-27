# ============================================================
# 패널 공통 특성 문장 생성 (전체 데이터 분석)
# ============================================================
from flask import jsonify, current_app
from config import antropicLLM # 클로드 llm 가져오기


def makeCommon(panels):
    # 로깅
    current_app.logger.info(f"🔍 공통 특성 분석: {len(panels)}개 패널")

    # 공통 특성 키워드를 담을 배열    
    keyword_counter = {}
    
    total_count = len(panels)
    
    # ============================================================
    # 1. 기본 인구통계 데이터 수집
    # ============================================================
    
    # 성별
    gender_dist = {}
    for panel in panels:
        gender = panel.get('gender')
        if gender and gender != '무응답':
            gender_dist[gender] = gender_dist.get(gender, 0) + 1
            keyword_counter[gender] = keyword_counter.get(gender, 0) + 1
    
    # 연령대
    age_dist = {}
    for panel in panels:
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
            age_dist[age_group] = age_dist.get(age_group, 0) + 1
            keyword_counter[age_group] = keyword_counter.get(age_group, 0) + 1
    
    # 거주지
    residence_dist = {}
    for panel in panels:
        residence = panel.get('residence')
        if residence and residence != '무응답':
            residence_dist[residence] = residence_dist.get(residence, 0) + 1
            keyword_counter[residence] = keyword_counter.get(residence, 0) + 1
    
    # 결혼여부
    marital_dist = {}
    for panel in panels:
        marital = panel.get('maritalStatus')
        if marital and marital != '무응답':
            marital_dist[marital] = marital_dist.get(marital, 0) + 1
            keyword_counter[marital] = keyword_counter.get(marital, 0) + 1
    
    # 최종학력
    education_dist = {}
    for panel in panels:
        edu = panel.get('education')
        if edu and edu != '무응답':
            education_dist[edu] = education_dist.get(edu, 0) + 1
            keyword_counter[edu] = keyword_counter.get(edu, 0) + 1
    
    # ============================================================
    # 2. 직업 및 소득 데이터
    # ============================================================
    
    # 직업
    job_dist = {}
    for panel in panels:
        job = panel.get('job')
        if job and job != '무응답':
            job_dist[job] = job_dist.get(job, 0) + 1
            keyword_counter[job] = keyword_counter.get(job, 0) + 1
    
    # 직무
    role_dist = {}
    for panel in panels:
        role = panel.get('role')
        if role and role != '무응답':
            role_dist[role] = role_dist.get(role, 0) + 1
            keyword_counter[role] = keyword_counter.get(role, 0) + 1
    
    # 개인소득
    income_dist = {}
    for panel in panels:
        income = panel.get('personalIncome')
        if income and income != '무응답':
            income_dist[income] = income_dist.get(income, 0) + 1
            keyword_counter[income] = keyword_counter.get(income, 0) + 1
    
    # 가구소득
    household_income_dist = {}
    for panel in panels:
        h_income = panel.get('householdIncome')
        if h_income and h_income != '무응답':
            household_income_dist[h_income] = household_income_dist.get(h_income, 0) + 1
    
    # ============================================================
    # 3. 보유 제품 및 라이프스타일
    # ============================================================
    
    # 휴대폰 브랜드
    phone_brand_dist = {}
    for panel in panels:
        phone = panel.get('phoneBrand')
        if phone and phone != '무응답':
            phone_brand_dist[phone] = phone_brand_dist.get(phone, 0) + 1
            keyword_counter[phone] = keyword_counter.get(phone, 0) + 1
    
    # 차량 보유
    car_dist = {}
    for panel in panels:
        car = panel.get('carOwnership')
        if car and car != '무응답':
            car_dist[car] = car_dist.get(car, 0) + 1
            keyword_counter[f'차량{car}'] = keyword_counter.get(f'차량{car}', 0) + 1
    
    # 보유 전자제품 (JSONB 배열 처리)
    owned_products = {}
    for panel in panels:
        products = panel.get('ownedProducts', [])
        if isinstance(products, list):
            for product in products:
                if product:
                    owned_products[product] = owned_products.get(product, 0) + 1
    
    # ============================================================
    # 4. 라이프스타일 패턴 (lifestylePatterns)
    # ============================================================
    
    lifestyle_data = {}
    lifestyle_keys = [
        '체력_관리를_위한_활동',
        '이용_중인_OTT_서비스',
        '요즘_많이_사용하는_앱',
        '스트레스를_해소하는_방법',
        '사용해_본_AI_챗봇_서비스',
        '해외여행을_간다면_가고싶은_곳',
        '여행갈_때의_스타일',
        '미니멀리스트_맥시멀리스트_어느_쪽인지'
    ]
    
    for key in lifestyle_keys:
        lifestyle_data[key] = {}
    
    for panel in panels:
        patterns = panel.get('lifestylePatterns', {})
        if isinstance(patterns, dict):
            for key in lifestyle_keys:
                value = patterns.get(key)
                if value and value != '무응답':
                    if key not in lifestyle_data:
                        lifestyle_data[key] = {}
                    lifestyle_data[key][value] = lifestyle_data[key].get(value, 0) + 1
    
    # ============================================================
    # 5. 흡연 및 음주 데이터 (JSONB 배열 처리)
    # ============================================================
    
    # 흡연 경험
    smoking_exp = {}
    for panel in panels:
        smoking = panel.get('smokingExperience', [])
        if isinstance(smoking, list):
            for item in smoking:
                if item and item != '담배를 피워본 적이 없다':
                    smoking_exp[item] = smoking_exp.get(item, 0) + 1
    
    # 음주 경험
    drinking_exp = {}
    for panel in panels:
        drinking = panel.get('drinkingExperience', [])
        if isinstance(drinking, list):
            for item in drinking:
                if item and item != '최근 1년 이내 술을 마시지 않음':
                    drinking_exp[item] = drinking_exp.get(item, 0) + 1
    
    # ============================================================
    # 6. 상위 5개 키워드 추출
    # ============================================================
    
    top_keywords = sorted(
        keyword_counter.items(), 
        key=lambda x: x[1], 
        reverse=True
    )[:5]
    
    keywords = [
        {"keyword": k, "count": v} 
        for k, v in top_keywords
    ]
    
    # ============================================================
    # 7. 통계 계산
    # ============================================================
    
    avg_age = sum(p.get('age', 0) for p in panels) / total_count if total_count > 0 else 0
    
    # 자녀수 평균
    avg_children = sum(p.get('children', 0) for p in panels) / total_count if total_count > 0 else 0
    
    # ============================================================
    # 8. LLM 프롬프트 생성
    # ============================================================
    
    # 상위 항목들만 선별하여 프롬프트에 포함
    def format_top_items(dist, count=5):
        if not dist:
            return "데이터 없음"
        sorted_items = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:count]
        return ', '.join([f'{k} {v}명({v/total_count*100:.1f}%)' for k, v in sorted_items])
    
    summary_prompt = f"""다음은 {total_count}명의 패널 데이터를 종합 분석한 결과입니다.

【상위 5개 공통 특성】
{chr(10).join([f'{idx+1}. {k["keyword"]}: {k["count"]}명 ({(k["count"]/total_count*100):.1f}%)' for idx, k in enumerate(keywords)])}

【기본 인구통계】
- 총 인원: {total_count}명
- 평균 나이: {avg_age:.1f}세
- 성별 분포: {format_top_items(gender_dist, 2)}
- 연령대 분포: {format_top_items(age_dist, 5)}
- 주요 거주지: {format_top_items(residence_dist, 5)}
- 결혼 여부: {format_top_items(marital_dist, 3)}
- 학력 분포: {format_top_items(education_dist, 4)}

【직업 및 소득】
- 주요 직업: {format_top_items(job_dist, 5)}
- 주요 직무: {format_top_items(role_dist, 5)}
- 개인 소득: {format_top_items(income_dist, 5)}
- 가구 소득: {format_top_items(household_income_dist, 3)}
- 평균 자녀수: {avg_children:.1f}명

【디지털 및 소비 패턴】
- 휴대폰 브랜드: {format_top_items(phone_brand_dist, 3)}
- 차량 보유: {format_top_items(car_dist, 2)}
- 주요 보유 전자제품: {format_top_items(owned_products, 5)}

【라이프스타일】
- 체력관리: {format_top_items(lifestyle_data.get('체력_관리를_위한_활동', {}), 3)}
- OTT 이용: {format_top_items(lifestyle_data.get('이용_중인_OTT_서비스', {}), 3)}
- 주요 사용 앱: {format_top_items(lifestyle_data.get('요즘_많이_사용하는_앱', {}), 3)}
- 스트레스 해소: {format_top_items(lifestyle_data.get('스트레스를_해소하는_방법', {}), 3)}
- AI 챗봇 경험: {format_top_items(lifestyle_data.get('사용해_본_AI_챗봇_서비스', {}), 3)}
- 여행 선호지: {format_top_items(lifestyle_data.get('해외여행을_간다면_가고싶은_곳', {}), 3)}
- 여행 스타일: {format_top_items(lifestyle_data.get('여행갈_때의_스타일', {}), 2)}
- 소비 성향: {format_top_items(lifestyle_data.get('미니멀리스트_맥시멀리스트_어느_쪽인지', {}), 2)}

【기호 식품】
- 주요 음주 종류: {format_top_items(drinking_exp, 5)}
- 흡연 경험: {format_top_items(smoking_exp, 3)}

위의 모든 데이터를 종합적으로 분석하여 다음 두 가지 내용을 작성해주세요:

1. 패널 집단 요약 (1-2문장):
   - 이 패널 집단의 인구통계학적 특징과 라이프스타일을 종합적으로 요약
   - 주요 공통점과 특이사항을 포함

2. 마케팅 전략 제안 (1-2문장):
   - 이 그룹의 특성을 고려한 구체적이고 실행 가능한 마케팅 전략
   - 효과적인 채널, 메시지, 타이밍 등을 포함

【출력 형식】
패널 요약: 이 패널은 평균 40.8세로 ....
마케팅 전략: 디지털 제품을 많이 소비하는 집단으로 ...


중요: 순수한 텍스트로만 응답하세요. 마크다운 형식(**, *, #, - 등)을 사용하지 마세요."""

    message = antropicLLM.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=1024,
        messages=[
            {"role": "user", "content": summary_prompt}
        ]
    )
    
    response_text = message.content[0].text.strip()
    
    # 마크다운 기호 제거 (안전장치)
    response_text = response_text.replace('**', '')
    response_text = response_text.replace('*', '')
    response_text = response_text.replace('###', '')
    response_text = response_text.replace('##', '')
    response_text = response_text.replace('#', '')
    
    # [패널 요약]과 [마케팅 전략] 분리
    summary = ""
    marketing_strategy = ""
    
    if "[패널 요약]" in response_text and "[마케팅 전략]" in response_text:
        parts = response_text.split("[마케팅 전략]")
        summary = parts[0].replace("[패널 요약]", "").strip()
        marketing_strategy = parts[1].strip()
    else:
        # 구분자가 없는 경우 전체를 요약으로 처리
        summary = response_text
        marketing_strategy = ""
    
    current_app.logger.info(f"✅ 공통 특성 분석 완료: {len(keywords)}개 키워드, 총 {total_count}명 분석")
    
    return jsonify({
        "keywords": keywords,
        "summary": summary,
        "marketingStrategy": marketing_strategy,
        "totalCount": total_count,
        "avgAge": round(avg_age, 1)
    })