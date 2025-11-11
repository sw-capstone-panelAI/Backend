from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from anthropic import Anthropic
import json
import logging
import traceback
import re

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host": "rds-postgresql-pgvector.cbq4y662mptt.ap-northeast-2.rds.amazonaws.com",
    "user": "root",
    "password": "rkdtmdwls123",
    "dbname": "postgres",
    "port": 5432
}

client = Anthropic(api_key="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")

def create_sql_generation_prompt(user_query: str) -> str:
    return f"""당신은 PostgreSQL SQL 쿼리 생성 전문가입니다.

테이블 이름: panel_responses

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

사용자 요청: "{user_query}"

쿼리 생성 규칙 (매우 중요!):
1. 기본 형식: SELECT * FROM panel_responses WHERE [조건] LIMIT 100
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
  → SELECT * FROM panel_responses WHERE 지역 = '서울' AND 성별 = '남성' AND 출생년도::INTEGER BETWEEN 1985 AND 1994 AND 자녀수 >= 2 

나쁜 예시 (절대 이렇게 하지 마세요):
- 출생년도 BETWEEN... (❌ 캐스팅 없음)
- 패널ID (❌ 대문자 ID)

지금 SQL 쿼리를 생성하세요 (순수 SQL만):"""

def parse_income(income_str):
    """소득 문자열을 숫자로 변환 (예: '400-499만원' -> 450)"""
    if not income_str or income_str == '-' or income_str == '':
        return 0
    
    try:
        income_str = str(income_str).strip()
        
        if '-' in income_str and '만원' in income_str:
            parts = income_str.replace('만원', '').replace(' ', '').split('-')
            if len(parts) == 2:
                return (int(parts[0]) + int(parts[1])) / 2
        
        if '이상' in income_str:
            num = income_str.replace('만원', '').replace('이상', '').replace(' ', '').strip()
            return int(num)
        
        if '미만' in income_str:
            num = income_str.replace('만원', '').replace('미만', '').replace(' ', '').strip()
            return int(num) / 2
        
        if '만원' in income_str:
            num = income_str.replace('만원', '').replace(' ', '').strip()
            return int(num)
        
        return int(income_str)
    except Exception as e:
        logging.warning(f"소득 파싱 실패: {income_str}, 오류: {e}")
        return 0

def has_data(value):
    """데이터가 실제로 존재하는지 확인"""
    if value is None or value == '' or value == '-':
        return False
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True

def normalize_vehicle_data(panel_dict):
    """차량 정보의 일관성을 확인하고 수정"""
    car_ownership = panel_dict.get('차량여부', '')
    car_brand = panel_dict.get('자동차_제조사', '')
    car_model = panel_dict.get('자동차_모델', '')
    
    has_car_info = has_data(car_brand) or has_data(car_model)
    
    # 차량여부가 '없음'인데 차량 정보가 있는 경우
    if car_ownership == '없음' and has_car_info:
        # 차량 정보를 우선시하여 차량여부를 '있음'으로 변경
        panel_dict['차량여부'] = '있음'
        logging.info(f"패널 {panel_dict.get('패널id')}: 차량여부 수정 (없음 → 있음)")
    
    # 차량여부가 '있음'인데 차량 정보가 없는 경우
    elif car_ownership == '있음' and not has_car_info:
        # 차량 정보가 없으므로 차량여부를 '없음'으로 변경
        panel_dict['차량여부'] = '없음'
        logging.info(f"패널 {panel_dict.get('패널id')}: 차량여부 수정 (있음 → 없음)")
    
    # 차량여부가 비어있는데 차량 정보가 있는 경우
    elif not car_ownership or car_ownership == '-':
        if has_car_info:
            panel_dict['차량여부'] = '있음'
        else:
            panel_dict['차량여부'] = '없음'
    
    return panel_dict

def calculate_reliability(panel, query):
    """패널의 신뢰도를 계산하는 함수 (0-100)"""
    score = 100
    deduction_reasons = []
    
    birth_year = panel.get('출생년도')
    age = None
    
    if birth_year:
        try:
            age = 2025 - int(birth_year)
        except:
            age = None
    
    # ===== 차량 정보 일관성 검증 =====
    car_ownership = panel.get('차량여부', '')
    car_brand = panel.get('자동차_제조사', '')
    car_model = panel.get('자동차_모델', '')
    
    has_car_info = has_data(car_brand) or has_data(car_model)
    
    # 차량여부와 차량 정보 불일치
    if car_ownership == '없음' and has_car_info:
        score -= 10
        deduction_reasons.append("차량 없음으로 표기했으나 차량 정보 존재")
    elif car_ownership == '있음' and not has_car_info:
        score -= 5
        deduction_reasons.append("차량 있음으로 표기했으나 차량 정보 없음")
    
    # ===== 연령 기반 검증 =====
    if age is not None:
        # 1. 18세 미만 미성년자 검증
        if age < 18:
            # 결혼 여부
            if panel.get('결혼여부') == '기혼':
                score -= 15
                deduction_reasons.append("미성년자이지만 기혼 상태")
            
            # 자녀수
            children = panel.get('자녀수')
            if children and children > 0:
                score -= 15
                deduction_reasons.append("미성년자이지만 자녀 보유")
            
            # 최종학력
            education = panel.get('최종학력') or ''  # ✅ None 방지
            if '대학교' in education and '졸업' in education:
                score -= 10
                deduction_reasons.append("미성년자이지만 대학교 졸업")
            
            # 직업
            job = panel.get('직업') or ''  # ✅ None 방지
            if job and job not in ['', '-', '학생', '무직']:
                score -= 10
                deduction_reasons.append("미성년자이지만 직업 보유")
        
        # 2. 20세 미만 차량 보유
        if age < 20:
            if panel.get('차량여부') == '있음':
                score -= 15
                deduction_reasons.append("20세 미만이지만 차량 보유")
        
        # 3. 미성년자 흡연/음주 (불법)
        if age < 19:
            if has_data(panel.get('흡연경험')):
                score -= 20
                deduction_reasons.append("미성년자이지만 흡연 경험")
            if has_data(panel.get('음용경험_술')):
                score -= 20
                deduction_reasons.append("미성년자이지만 음주 경험")
        
        # 4. 1900년대생이 중고등학생
        if birth_year and int(birth_year) < 2000:
            job = panel.get('직업') or ''  # ✅ None 방지
            if '학생' in job and ('중학' in job or '고등' in job):
                score -= 20
                deduction_reasons.append("성인이지만 중고등학생")
        
        # 5. 80대 이상이 학생
        if age >= 80:
            job = panel.get('직업') or ''  # ✅ None 방지
            if '학생' in job:
                score -= 15
                deduction_reasons.append("80세 이상이지만 학생")
    
    # ===== 학력-직업 검증 =====
    education = panel.get('최종학력') or ''  # ✅ None 방지
    job = panel.get('직업') or ''  # ✅ None 방지
    
    # 고졸 이하인데 전문직
    if education and ('고등학교' in education or '중학교' in education or '초등학교' in education):
        professional_jobs = ['의사', '변호사', '교수', '판사', '검사', '약사', '회계사', '변리사', '건축사']
        if any(prof in job for prof in professional_jobs):
            score -= 15
            deduction_reasons.append("학력과 직업 불일치 (고졸 이하 - 전문직)")
    
    # ===== 소득 검증 =====
    personal_income = parse_income(panel.get('월평균_개인소득'))
    household_income = parse_income(panel.get('월평균_가구소득'))
    
    if personal_income > 0 and household_income > 0:
        if personal_income > household_income:
            score -= 10
            deduction_reasons.append("개인소득이 가구소득보다 높음")
    
    # ===== 휴대폰 브랜드-모델 일치성 검증 =====
    phone_brand = panel.get('휴대폰_브랜드') or ''  # ✅ None 방지
    phone_model = panel.get('휴대폰_모델') or ''  # ✅ None 방지
    
    phone_brand = str(phone_brand).lower()
    phone_model = str(phone_model).lower()
    
    if phone_brand and phone_model and phone_brand not in ['-', ''] and phone_model not in ['-', '']:
        brand_model_map = {
            '삼성': ['갤럭시', 'galaxy'],
            '애플': ['아이폰', 'iphone'],
            'lg': ['lg', 'v', 'g'],
            '샤오미': ['샤오미', 'xiaomi', 'mi', 'redmi'],
            '화웨이': ['화웨이', 'huawei', 'mate', 'p'],
        }
        
        brand_matched = False
        for brand_key, model_keywords in brand_model_map.items():
            if brand_key in phone_brand:
                if any(keyword in phone_model for keyword in model_keywords):
                    brand_matched = True
                    break
        
        if not brand_matched and phone_model not in ['기타', '모름', '-', '']:
            score -= 5
            deduction_reasons.append("휴대폰 브랜드와 모델명 불일치")
    
    # ===== 흡연 경험 일관성 검증 =====
    smoking_exp = panel.get('흡연경험')
    smoking_brand = panel.get('흡연경험_담배브랜드')
    smoking_brand_etc = panel.get('흡연경험_담배브랜드_기타')
    ecigarette_exp = panel.get('전자담배_이용경험')
    smoking_etc = panel.get('흡연경험_담배_기타내용')
    
    if not has_data(smoking_exp):
        if has_data(smoking_brand):
            score -= 10
            deduction_reasons.append("흡연 경험 없지만 담배 브랜드 표기")
        if has_data(smoking_brand_etc):
            score -= 5
            deduction_reasons.append("흡연 경험 없지만 기타 브랜드 표기")
        if has_data(ecigarette_exp):
            score -= 10
            deduction_reasons.append("흡연 경험 없지만 전자담배 경험 표기")
        if has_data(smoking_etc):
            score -= 5
            deduction_reasons.append("흡연 경험 없지만 기타 내용 표기")
    
    # ===== 음주 경험 일관성 검증 =====
    drinking_exp = panel.get('음용경험_술')
    drinking_etc = panel.get('음용경험_술_기타내용')
    
    if not has_data(drinking_exp):
        if has_data(drinking_etc):
            score -= 10
            deduction_reasons.append("음주 경험 없지만 기타 내용 표기")
    
    # ===== 검색어 매칭 검증 =====
    query_lower = query.lower()
    
    if '남성' in query_lower or '남자' in query_lower:
        if panel.get('성별') != '남성':
            score -= 10
            deduction_reasons.append("검색 조건(남성)과 불일치")
    
    if '여성' in query_lower or '여자' in query_lower:
        if panel.get('성별') != '여성':
            score -= 10
            deduction_reasons.append("검색 조건(여성)과 불일치")
    
    if '서울' in query_lower:
        if panel.get('지역') != '서울':
            score -= 10
            deduction_reasons.append("검색 조건(서울)과 불일치")
    
    if '차량' in query_lower or '자동차' in query_lower:
        if panel.get('차량여부') != '있음':
            score -= 10
            deduction_reasons.append("검색 조건(차량)과 불일치")
    
    # 최종 점수는 0~100 사이로 제한
    final_score = max(0, min(100, score))
    
    return final_score, deduction_reasons

@app.route('/api/search', methods=['POST'])
def search():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()

        if not query:
            return jsonify({"error": "쿼리를 입력해주세요."}), 400

        logging.info(f"검색 쿼리: {query}")

        # Claude API로 SQL 쿼리 생성
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": create_sql_generation_prompt(query)}
            ]
        )
        
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
        
        # DB 조회 실행
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql_query)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        if not results:
            return jsonify({
                "panels": [],
                "words": []
            })
        
        # 결과를 프론트엔드 형식에 맞게 변환
        panels = []
        for idx, row in enumerate(results):
            panel_dict = dict(row)
            
            # 차량 정보 일관성 확인 및 수정
            panel_dict = normalize_vehicle_data(panel_dict)
            
            # 출생년도로 나이 계산
            birth_year = panel_dict.get('출생년도')
            age = None
            if birth_year:
                try:
                    age = 2025 - int(birth_year)
                except:
                    age = None
            
            # 소득 파싱
            income = parse_income(panel_dict.get('월평균_개인소득'))
            
            # 신뢰도 계산
            reliability, reasons = calculate_reliability(panel_dict, query)
            
            # 차량 정보 구성
            vehicle_info = {
                "hasVehicle": panel_dict.get('차량여부') == '있음',
                "type": panel_dict.get('자동차_모델') or panel_dict.get('자동차_제조사') or '없음'
            }
            
            # 프론트엔드에서 기대하는 형식으로 변환
            panel = {
                "id": panel_dict.get('패널id', f"panel_{idx}"),
                "reliability": reliability,
                "reliabilityReasons": reasons,
                "age": age,
                "gender": panel_dict.get('성별'),
                "occupation": panel_dict.get('직업') or '-',
                "residence": panel_dict.get('지역') or '-',
                "district": panel_dict.get('지역구') or '-',
                "maritalStatus": panel_dict.get('결혼여부') or '-',
                "education": panel_dict.get('최종학력') or '-',
                "job": panel_dict.get('직업') or '-',
                "role": panel_dict.get('직무') or '-',
                "income": income,
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
        
        # 검색어에서 키워드 추출
        words = []
        keywords = query.split()
        for keyword in keywords:
            if len(keyword) > 1:
                words.append({"text": keyword, "value": 10})
        
        logging.info(f"검색 결과: {len(panels)}개 패널")    
        logging.info(f"생성된 SQL: {sql_query}")

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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(host='0.0.0.0', port=5000, debug=True)