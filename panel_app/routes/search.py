from flask import Blueprint, request, jsonify
from ..models import db
from ..services.panel_search import embed_text_to_vector, search_panels_by_cosine, fetch_responses_by_panel_ids
from config import EMBED_DIM # 벡터 차원

# 파일 구조화를 위해 블루프린터 객체 생성
bp_search = Blueprint("search", __name__, url_prefix="/search")


@bp_search.route("/panel", methods=["POST"]) # 패널 검색 주소
def search_panel():
    data = request.get_json()               # axios.post()로 보낸 json 데이터를 파싱
    query = data.get('query', '').strip()   # 딕셔너리처럼 접근 가능
    print(f"쿼리 수신: {query}")

    # RDS연결은 models폴더에서 미리 세팅, 여기서 검색이 발생하고
    top_k = 36000
    sim_th = 0.5

    # 1~2단계: 임베딩
    qvec = embed_text_to_vector(query)

    # 3~4단계: 검색 (유사도 0.6 이상)
    panels = search_panels_by_cosine(qvec, sim_threshold=sim_th, top_k=top_k)
    panel_ids = [p["panel_id"] for p in panels]

    responses = fetch_responses_by_panel_ids(panel_ids)
    cnt = 0
    for response in responses:
        cnt += 1
        print(response["패널id"])

    print(cnt)

    # 5단계: 응답
    return jsonify({"panels": responses, "words": []}), 200


@bp_search.route("/Detail")
def panelDetail():
    return jsonify() # 조회하고자 하는 한 패널의 상세 정보


@bp_search.route("/word")
def reocmmendWord():
    return jsonify() # 쿼리 추천어 리턴


@bp_search.route("/common")
def commonCharac():
    return jsonify() # 패널의 공통 특성