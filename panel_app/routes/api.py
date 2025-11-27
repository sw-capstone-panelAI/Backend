# ============================================================
# API 엔드포인트
# ============================================================

from flask import Blueprint, request, jsonify, current_app
import traceback
from ..services.text2sql import create_sql_with_llm
from ..services.exportCSV import makeCsv
from ..services.keyword import makeKeyword, makeNewQuery
from ..services.common import makeCommon
    
# 파일 구조화를 위해 블루프린터 객체 생성
bp_api = Blueprint("api", __name__, url_prefix="/api")

# 패널 검색
@bp_api.route('/search', methods=['POST'])
def search():
    try:
        # 프론트에서 자연어 쿼리 받아오기
        data = request.get_json()
        query = data.get('query', '').strip()
        model = data.get('model', '').strip() # fast 모델, deep 모델

        # 입력 쿼리가 없을 경우
        if not query:
            return jsonify({"error": "쿼리를 입력해주세요."}), 400

        # 로깅
        current_app.logger.info(f"🔍 검색 쿼리: {query} \n 🔍 검색 모델: {model}")
        
        # 자연어 쿼리 입력시 llm이 sql 쿼리문 생성
        response = create_sql_with_llm(query, model)

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
        
        # 전달된 패널들이 없는 경우
        if not panels or len(panels) == 0:
            return jsonify({"error": "분석할 패널 데이터가 없습니다."}), 400
        
        # 패널의 공통 특성문장 생성
        response = makeCommon(panels) # function
        
        # 프론트에게 결과 날림
        return response        
    except Exception as e:
        current_app.logger.error(f"💥 공통 특성 분석 오류: {str(e)}")
        traceback.print_exc()
        return jsonify({
            "error": "공통 특성 분석 중 오류가 발생했습니다.",
            "detail": str(e)
        }), 500


# 추천 검색어
@bp_api.route('/related-keywords', methods=['POST'])
def related_keywords():
    # 추출된 패널과 입력한 자연어 쿼리 가져옴
    data = request.get_json()
    user_query = data.get('query', '').strip()
    
    # 입력한 쿼리가 없을 경우 예외처리
    if not user_query:
        return jsonify([
            { "text": "키워드1" },
            { "text": "키워드2" },
            { "text": "키워드3" },
            { "text": "키워드4" },
            { "text": "키워드5" },
            { "text": "키워드6" }
        ])

    # 추천 키워드를 생성
    keywords = makeKeyword(user_query)
    current_app.logger.info(f"생성된 키워드: {keywords}")

    # 프론트로 키워드 반환
    return jsonify(keywords)


# 추천어 기반 자연어 쿼리 생성
@bp_api.route('/keywords-newQuery', methods=['POST'])
def keywords_base_query():
    # 추출된 패널과 입력한 자연어 쿼리 가져옴
    data = request.get_json()
    user_query = data.get('query', '').strip()
    keywords = data.get('keywords', [])

    # 입력한 쿼리가 없을 경우 예외처리
    if not user_query:
        return jsonify({'query': "서울 거주 20대"})

    # 추천어 기반 자연어 쿼리 생성
    newQuery = makeNewQuery(user_query, keywords)

    # 프론트로 키워드 반환
    return jsonify({'query': newQuery})


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
