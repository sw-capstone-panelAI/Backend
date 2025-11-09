from flask import Blueprint, request, jsonify
from ..models import db

# 파일 구조화를 위해 블루프린터 객체 생성
bp_search = Blueprint("search", __name__, url_prefix="/search")


@bp_search.route("/panel", methods=["POST"]) # 패널 검색 주소
def search():
    data = request.get_json()               # axios.post()로 보낸 json 데이터를 파싱
    query = data.get('query', '').strip()   # 딕셔너리처럼 접근 가능
    print(f"쿼리 수신: {query}")

    # RDS연결은 models폴더에서 미리 세팅(우선은 여기서), 여기서 검색이 발생하고
    





    return jsonify({
        "panels" : panels,  # 추출 패널 데이터
        "words": words,   # 추천 검색어 
    }) # 검색된 패널 리턴 (패널 아이디, 기본 정보 정도만 전달? 아니면 모든 데이터 전달)


@bp_search.route("/Detail")
def panelDetail():
    return jsonify() # 조회하고자 하는 한 패널의 상세 정보


@bp_search.route("/word")
def reocmmendWord():
    return jsonify() # 쿼리 추천어 리턴


@bp_search.route("/common")
def commonCharac():
    return jsonify() # 패널의 공통 특성