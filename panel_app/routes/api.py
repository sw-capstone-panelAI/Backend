# ============================================================
# API 엔드포인트
# ============================================================

from flask import Blueprint, request, jsonify, current_app
import traceback
from ..services.text2sql import create_sql_with_llm
from ..services.exportCSV import makeCsv
    
# 파일 구조화를 위해 블루프린터 객체 생성
bp_api = Blueprint("api", __name__, url_prefix="/api")

# 패널 검색
@bp_api.route('/search', methods=['POST'])
def search():
    try:
        # 프론트에서 자연어 쿼리 받아오기
        data = request.get_json()
        query = data.get('query', '').strip()

        # 입력 쿼리가 없을 경우
        if not query:
            return jsonify({"error": "쿼리를 입력해주세요."}), 400

        # 로깅
        current_app.logger.info(f"🔍 검색 쿼리: {query}")
        
        # 자연어 쿼리 입력시 llm이 sql 쿼리문 생성
        response = create_sql_with_llm(query)

        # 프론트로 리턴할 값
        return response
    except Exception as e:
        # 검색 오류 처리
        current_app.logger.error(f"💥 검색 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "검색 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500



# 공통 특성
@bp_api.route('/common-characteristics', methods=['POST'])
def common_characteristics():
    """패널들의 공통 특성 분석"""
    try:
        data = request.get_json()
        panels = data.get('panels', [])
        
        if not panels or len(panels) == 0:
            return jsonify({"error": "분석할 패널 데이터가 없습니다."}), 400
        
        current_app.logger.info(f"🔍 공통 특성 분석: {len(panels)}개 패널")
        
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

        message = client.messages.create(
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
        
    except Exception as e:
        current_app.logger.error(f"💥 공통 특성 분석 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "공통 특성 분석 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500







# 추천 검색어
@bp_api.route('/related_keywords', methods=['POST'])
def related_keywords():
    
    # 추출된 패널과 입력한 자연어 쿼리 가져옴
    data = request.get_json()
    user_query = data.get('query', '').strip()
    
    
    return jsonify({'keywords': related})





# csv 생성
@bp_api.route('/export-csv', methods=['POST'])
def export_csv():
    """패널 데이터를 CSV로 내보내기"""
    try:       
        data = request.get_json()
        panels = data.get('panels', [])
        
        # 받아온 패널 데이터로 csv 파일 생성
        response = makeCsv(panels)

        # 생성된 csv 프론트로 리턴
        return response
    
    except Exception as e:
        current_app.logger.error(f"💥 CSV 내보내기 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "CSV 내보내기 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500
