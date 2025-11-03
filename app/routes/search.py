from flask import Blueprint, jsonify
from ..models.panels import PANELS

bp = Blueprint("search", __name__)

@bp.route("/", methods=["POST"])
def search():

    words = ["남성", "서울", "회사원", "고소득", "차량", "20대", "30대", "40대"]

    return jsonify({
        "panels" : PANELS,  # 추출 패널 데이터
        "words": words,   # 추천 검색어 
    })          # 딕셔너리 형태로 전달
