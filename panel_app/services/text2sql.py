# ============================================================
# SQL 생성 프롬프트
# ============================================================
from flask import jsonify, current_app
from config import antropicLLM # 클로드 llm 가져오기
from ..models import db # db연결된 객체
from sqlalchemy import text
import json
from ..services.reliability import LIFESTYLE_COLUMNS, calculate_reliability_score 

# llm에게 입력할 프롬프트 생성하는 함수
def create_sql_generation_prompt(user_query: str) -> str:
    """SQL 쿼리 생성 프롬프트 (생활패턴 기반 필터링 포함)"""
    
    # 테이블 스키마 설명서
    with open("./tabel_schema_info.json", "r", encoding="utf-8") as f:
        jsonFile = json.load(f)

    return f"""너는 자연어 쿼리가 들어왔을 때 그것을 SQL쿼리문으로 바꿔주는 데이터베이스 전문가이다.
            제공되는 테이블 스키마 가이드 json파일을 참고하여 사용자가 입력한 자연어 쿼리에 적합한 SQL 쿼리문을 만들어라.
            결과를 출력할때 쿼리 생성 규칙에 맞게 출력한다.

            [Tabel JSON]
            {jsonFile}

            [Query]
            {user_query}

            [SQL쿼리 생성 예시]
            입력 자연어 쿼리: 서울 거주하는 남성 중 여름철 땀냄새를 신경쓰는 사람
            LLM 생성 쿼리문: 
            SELECT *
            FROM panel_cb_all_label
            WHERE 지역 = '서울'
            AND 성별 = '남성'
            AND (
                    "여름철_가장_걱정되는_점" = '더위와 땀'
                    OR "여름철_땀_때문에_겪는_불편함" = '땀 냄새가 걱정된다'
                );
                

            [쿼리 생성 규칙]
            1. 기본 형식: SELECT * FROM panel_cb_all_label
            2. 나이대별 출생년도 (2025년 기준, 만 나이):
            - 10대 (만 10~19세): 출생년도 2005 ~ 2014
            - 20대 (만 20~29세): 출생년도 1995 ~ 2004
            - 30대 (만 30~39세): 출생년도 1985 ~ 1994
            - 40대 (만 40~49세): 출생년도 1975 ~ 1984
            - 50대 (만 50~59세): 출생년도 1965 ~ 1974
            - 60대 (만 60~69세): 출생년도 1955 ~ 1964
            3. 인원수 명시시 LIMIT 추가
            4. 고소득자는 월평균_개인소득 400만원 이상
            5. SQL문 생성시 모든 컬럼명에는 ""를 붙여준다.
            6. 사용자가 무응답(null)값을 검색하고자할 경우 is null로 검색한다.
            7. JSONB 타입 컬럼 처리 규칙 (매우 중요):
            - 다음 컬럼들은 JSONB 타입이므로 반드시 ::text로 캐스팅 후 비교해야 한다:
              음용경험_술, 흡연경험, 흡연경험_담배브랜드, 전자담배_이용경험, 보유전제품
            - JSONB 컬럼에 LIKE 사용 시: "컬럼명"::text LIKE '%값%'
            - JSONB 컬럼에 NOT LIKE 사용 시: "컬럼명"::text NOT LIKE '%값%'
            - JSONB 컬럼에 = 또는 != 사용 금지, 반드시 LIKE 또는 NOT LIKE 사용
            - 예시: "음용경험_술"::text LIKE '%소주%'
            - 예시: "흡연경험"::text NOT LIKE '%담배를 피워본 적이 없다%'
            8. OR 조건 사용시 반드시 괄호로 묶어야 한다.
            9. 테이블과 전혀 연관이 없는 쿼리가 들어온 경우 [FAIL]으로 리턴한다
            - 예시: ㅁㄴㅇㅁㄴㅇ, 똥마렵다, 후하하하

            [SQL 출력 형식]
            ```sql
            SELECT * 
            FROM panel_cb_all_label
            WHERE "출생년도" BETWEEN '1985' AND '1994'
            AND "체력_관리를_위한_활동" != '체력관리를 위해 하고 있는 활동이 없다'
            AND "음용경험_술"::text NOT LIKE '%최근 1년 이내 술을 마시지 않음%';
            ``` 
            [SQL문을 생성할 수 없는 경우 출력 방식]
            검색 결과: [FAIL]

            지금 SQL 쿼리를 생성하세요 (순수 SQL만):"""

# LLM으로 SQL 쿼리 생성
def create_sql_with_llm(query: str):

    # 클로드 불러와서 프롬프트 입력
    message = antropicLLM.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2048,
        messages=[
            {"role": "user", "content": create_sql_generation_prompt(query)}
        ]
    )
    
    # 출력 결과 받아옴
    sql_query = message.content[0].text.strip()
    
    # 전혀 관련없는 질문을 할 경우
    if "[FAIL]" in sql_query:
        current_app.logger.info("❌ 전혀 관련없는 질문입니다.")
        return jsonify({
            "panels": [],
            "words": []
        })


    # llm이 생성한 결과에서 SQL 쿼리문만 추출
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]
    if sql_query.startswith("```"):
        sql_query = sql_query[3:]
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]
    sql_query = sql_query.strip()
    
    current_app.logger.info(f"📝 생성된 SQL: {sql_query}")
    
    # SQL 쿼리 실행
    rows = db.session.execute(text(sql_query)).mappings().all()
    results = [dict(r) for r in rows]

    # 검색 결과 없는 경우
    if not results:
        current_app.logger.info("❌ 검색 결과 없음")
        return jsonify({
            "panels": [],
            "words": []
        })
    
    current_app.logger.info(f"✅ DB 조회 완료: {len(results)}개 패널")
    
    # 결과 변환
    panels = []
    for idx, row in enumerate(results, start=1):
        panel_dict = dict(row)
        
        # 신뢰도 계산
        score, hit_rules, hit_messages = calculate_reliability_score(panel_dict)
        
        birth_year = panel_dict.get('출생년도')
        age = None
        if birth_year:
            try:
                age = 2025 - int(birth_year) -1
            except:
                age = None
        
        def convert_null(value, default='무응답'):
            if value is None or value == '' or value == '-' or value == 'null':
                return default
            return value
        
        lifestyle_dict = {}
        for f in LIFESTYLE_COLUMNS:
            lifestyle_dict[f] = convert_null(panel_dict.get(f))


        # 불러온 값을 프론트 환경에 맞게 라벨링 작업
        panel = {
            "id": f"패널{idx}",
            "mbSn": convert_null(panel_dict.get('패널id'), f"MB{idx}"),
            "reliability": score,
            "reliabilityReasons": hit_messages,
            "age": age,
            "gender": convert_null(panel_dict.get('성별')),
            "occupation": convert_null(panel_dict.get('직업')),
            "residence": convert_null(panel_dict.get('지역')),
            "district": convert_null(panel_dict.get('지역구')),
            "maritalStatus": convert_null(panel_dict.get('결혼여부')),
            "education": convert_null(panel_dict.get('최종학력')),
            "job": convert_null(panel_dict.get('직업')),
            "role": convert_null(panel_dict.get('직무')),
            "personalIncome": convert_null(panel_dict.get('월평균_개인소득')),
            "householdIncome": convert_null(panel_dict.get('월평균_가구소득')),
            "children": panel_dict.get('자녀수') if panel_dict.get('자녀수') is not None else 0,
            "familySize": convert_null(panel_dict.get('가족수')),
            "phoneModel": convert_null(panel_dict.get('휴대폰_모델')),
            "phoneBrand": convert_null(panel_dict.get('휴대폰_브랜드')),
            "carOwnership": convert_null(panel_dict.get('차량여부'), '없음'),
            "carBrand": convert_null(panel_dict.get('자동차_제조사')),
            "carModel": convert_null(panel_dict.get('자동차_모델')),
            "smokingExperience": panel_dict.get('흡연경험') or [],
            "drinkingExperience": panel_dict.get('음용경험_술') or [],
            "ownedProducts": panel_dict.get('보유전제품') or [],
            "lifestylePatterns": lifestyle_dict,
            "birthYear": birth_year,
        }
        panels.append(panel)
    
    panels.sort(key=lambda x: x['reliability'], reverse=True)
        
    words = []
    keywords = query.split()
    for keyword in keywords:
        if len(keyword) > 1:
            words.append({"text": keyword, "value": 10})
    
    current_app.logger.info(f"🎉 검색 완료: {len(panels)}개 패널")
    
    return jsonify({
        "panels": panels,
        "words": words
    })
