from flask import jsonify
from config import antropicLLM # 클로드 llm 가져오기
from sqlalchemy import text
from ..models import db # db연결된 객체
import logging
import traceback

# 간단 질문시 llm에게 입력할 프롬프트
def create_sql_generation_prompt(user_query: str) -> str:
    return f"""당신은 PostgreSQL SQL 쿼리 생성 전문가입니다.

    테이블 이름: welcome_cb_scored

    테이블 스키마 (정확한 컬럼명):
    - 패널id (VARCHAR, PRIMARY KEY) ⚠️ 소문자 'id' 주의!
    - 성별 (VARCHAR) - 예: '남성', '여성'
    - 출생년도 (VARCHAR) ⚠️ 문자열이므로 숫자 비교시 반드시 ::INTEGER 캐스팅 필요!
    - 지역 (VARCHAR) - 예: '서울', '부산', '경기', '인천' 등
    - 지역구 (VARCHAR)
    - 결혼여부 (VARCHAR) - 예: '기혼', '미혼'
    - 자녀수 (INTEGER) - 이미 숫자형이므로 캐스팅 불필요
    - 가족수 (VARCHAR) - 숫자 비교시 ::INTEGER 캐스팅 필요
    - 최종학력 (VARCHAR)
    - 직업 (VARCHAR)
    - 직무 (VARCHAR)
    - 월평균_개인소득 (VARCHAR)
    - 월평균_가구소득 (VARCHAR)
    - 보유전제품 (JSONB)
    - 휴대폰_브랜드 (VARCHAR)
    - 휴대폰_모델 (VARCHAR)
    - 차량여부 (VARCHAR) - 예: '있음', '없음'
    - 자동차_제조사 (VARCHAR)
    - 자동차_모델 (VARCHAR)
    - 흡연경험 (JSONB)
    - 흡연경험_담배브랜드 (JSONB)
    - 흡연경험_담배브랜드_기타 (VARCHAR)
    - 전자담배_이용경험 (JSONB)
    - 흡연경험_담배_기타내용 (VARCHAR)
    - 음용경험_술 (JSONB)
    - 음용경험_술_기타내용 (VARCHAR)
    - 신뢰도_감점_사유 (jsonb)
    - 신뢰도_점수 (integer)

    사용자 요청: "{user_query}"

    쿼리 생성 규칙 (매우 중요!):
    1. 기본 형식: SELECT * FROM welcome_cb_scored WHERE [조건] LIMIT 100
    2. 컬럼명 정확히 사용: 패널id (대문자 ID 아님!)
    3. 출생년도로 나이 계산 시 반드시 ::INTEGER 캐스팅:
    ✅ 올바른 예: 출생년도::INTEGER BETWEEN 1985 AND 1994
    ❌ 틀린 예: 출생년도 BETWEEN 1985 AND 1994
    4. 나이대별 출생년도 (2025년 기준):
    - 10대: 출생년도::INTEGER BETWEEN 2006 AND 2015
    - 20대: 출생년도::INTEGER BETWEEN 1995 AND 2005
    - 30대: 출생년도::INTEGER BETWEEN 1985 AND 1994
    - 40대: 출생년도::INTEGER BETWEEN 1975 AND 1984
    - 50대: 출생년도::INTEGER BETWEEN 1965 AND 1974
    - 60대: 출생년도::INTEGER BETWEEN 1955 AND 1964
    5. 자녀수는 이미 INTEGER이므로: 자녀수 >= 2 (캐스팅 불필요)
    6. 가족수는 VARCHAR이므로: 가족수::INTEGER >= 4 (캐스팅 필요)
    7. JSONB 존재 확인: 흡연경험 IS NOT NULL
    8. 텍스트 검색: 휴대폰_브랜드 LIKE '%삼성%'
    9. 차량 소유: 차량여부 = '있음'
    10. 순수 SQL만 반환 (설명, 코드블록 없이)
    11. LIMIT 제한 없이 전체를 뽑는다.

    좋은 예시:
    - "서울 30대 남성 자녀 2명 이상"
    → SELECT * FROM welcome_cb_scored WHERE 지역 = '서울' AND 성별 = '남성' AND 출생년도::INTEGER BETWEEN 1985 AND 1994 AND 자녀수 >= 2 

    나쁜 예시 (절대 이렇게 하지 마세요):
    - 출생년도 BETWEEN... (❌ 캐스팅 없음)
    - 패널ID (❌ 대문자 ID)

    지금 SQL 쿼리를 생성하세요 (순수 SQL만):"""


def has_data(value):
    """데이터가 실제로 존재하는지 확인"""
    if value is None or value == '' or value == '-':
        return False
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


# LLM으로 SQL 쿼리 생성
def create_sql_with_llm(query: str):
    try:
        # Claude API로 SQL 쿼리 생성
        message = antropicLLM.messages.create(
            model="claude-sonnet-4-20250514", 
            max_tokens=1024,
            messages=[
                # 자연어 쿼리 조합하여 프롬프트 생성
                {"role": "user", "content": create_sql_generation_prompt(query)}
            ]
        )

        # LLM이 생성한 SQL 쿼리문
        sql_query = message.content[0].text.strip()
            
        # SQL 쿼리 정제
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        sql_query = sql_query.strip()
        
        logging.info(f"생성된 SQL: {sql_query}")

        # SQL 쿼리 실행
        rows = db.session.execute(text(sql_query)).mappings().all()
        results = [dict(r) for r in rows]
        
        # 추출된 패널이 없는 경우
        if not results:
            return jsonify({
                "panels": [],
                "words": []
            })
        
        # 결과를 프론트엔드 형식에 맞게 변환
        panels = []
        for idx, row in enumerate(results):
            panel_dict = dict(row)

            # 출생년도로 나이 계산
            birth_year = panel_dict.get('출생년도')
            age = None
            if birth_year:
                try:
                    age = 2025 - int(birth_year)
                except:
                    age = None

            # 차량 정보 구성
            vehicle_info = {
                "hasVehicle": panel_dict.get('차량여부') == '있음',
                "type": panel_dict.get('자동차_모델') or panel_dict.get('자동차_제조사') or '없음'
            }

            # 프론트엔드에서 기대하는 형식으로 변환
            panel = {
                "id": panel_dict.get('패널id', f"panel_{idx}"),
                "reliability": panel_dict.get('신뢰도_점수'),
                "reliabilityReasons": panel_dict.get('신뢰도_감점_사유'),
                "age": age,
                "gender": panel_dict.get('성별'),
                "occupation": panel_dict.get('직업') or '-',
                "residence": panel_dict.get('지역') or '-',
                "district": panel_dict.get('지역구') or '-',
                "maritalStatus": panel_dict.get('결혼여부') or '-',
                "education": panel_dict.get('최종학력') or '-',
                "job": panel_dict.get('직업') or '-',
                "role": panel_dict.get('직무') or '-',
                "personalIncome": panel_dict.get('월평균_개인소득') or '-',
                "householdIncome": panel_dict.get('월평균_가구소득') or '-',
                "children": panel_dict.get('자녀수'),
                "familySize": panel_dict.get('가족수') or '-',
                "phoneModel": panel_dict.get('휴대폰_모델') or '-',
                "phoneBrand": panel_dict.get('휴대폰_브랜드') or '-',
                "vehicle": vehicle_info,
                "carOwnership": panel_dict.get('차량여부') or '없음',
                "carBrand": panel_dict.get('자동차_제조사') or '-',
                "carModel": panel_dict.get('자동차_모델') or '-',
                "smokingExperience": panel_dict.get('흡연경험'),
                "drinkingExperience": panel_dict.get('음용경험_술'),
                "ownedProducts": panel_dict.get('보유전제품'),
                "birthYear": birth_year,
                "smokingBrand": panel_dict.get('흡연경험_담배브랜드'),
                "ecigaretteExperience": panel_dict.get('전자담배_이용경험'),
            }
            panels.append(panel)

        # 신뢰도 높은 순으로 정렬
        panels.sort(key=lambda x: x['reliability'], reverse=True)

        logging.info(f"검색 결과: {len(panels)}개 패널")    
        logging.info(f"생성된 SQL: {sql_query}")

        # 검색어에서 키워드(추천 검색어 기능?) 추출
        words = []
        keywords = query.split()
        for keyword in keywords:
            if len(keyword) > 1:
                words.append({"text": keyword, "value": 10})

        return jsonify({
            "panels": panels,
            "words": words
        })

    except Exception as e:
        logging.error(f"검색 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "검색 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500
