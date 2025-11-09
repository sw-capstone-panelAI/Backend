# app/services/panel_search.py
from sqlalchemy import text
from app.models import db

# (1) 쿼리 → 임베딩
def embed_query(query: str) -> list[float]:
    """
    TODO: 여기를 네 실제 임베딩 생성 함수로 교체.
    예: OpenAI, HF, 로컬 임베더 등.
    """
    import random
    random.seed(hash(query) % (2**32))
    return [random.random() for _ in range(768)]  # dim은 너희 DB 스키마에 맞추기

# (2) 파이썬 리스트 → pgvector 리터럴 문자열 변환 (어댑터 없이 안전)
def to_vector_literal(vec: list[float]) -> str:
    # pgvector는 '[0.1,0.2,...]' 형식의 리터럴을 ::vector로 캐스팅하면 인식함
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"

# (3) Top-K + 유사도 임계치 필터 검색
def search_panels_by_embedding(query_vec: list[float], top_k: int = 20, similarity_threshold: float = 0.5):
    """
    cosine similarity >= threshold  <=>  cosine distance <= (1 - threshold)
    pgvector: <=> 는 cosine distance
    """
    dist_threshold = 1.0 - float(similarity_threshold)
    vec_literal = to_vector_literal(query_vec)

    sql = text("""
        SELECT
            p.id,
            p.name,
            p.age,
            p.gender,
            p.region,
            p.tags,
            p.attrs,
            (pp.embedding <=> :qvec::vector) AS cosine_distance
        FROM panel_profiles pp
        JOIN panels p ON p.id = pp.panel_id
        WHERE (pp.embedding <=> :qvec::vector) <= :dist_th
        ORDER BY pp.embedding <=> :qvec::vector
        LIMIT :k
    """)
    rows = db.session.execute(
        sql,
        {"qvec": vec_literal, "dist_th": dist_threshold, "k": int(top_k)}
    ).mappings().all()

    results = []
    for r in rows:
        results.append({
            "id": r["id"],
            "name": r["name"],
            "age": r["age"],
            "gender": r["gender"],
            "region": r["region"],
            "tags": r.get("tags") or [],
            "attrs": r.get("attrs") or {},
            "cosine_similarity": 1.0 - float(r["cosine_distance"]),
        })
    return results

# (4) 간단 추천어: 결과 tags를 모아 상위 N개 추출 (가벼운 베이스라인)
def recommend_words_from_results(panels: list[dict], max_words: int = 8) -> list[str]:
    from collections import Counter
    bag = Counter()
    for p in panels:
        for t in (p.get("tags") or []):
            if isinstance(t, str) and t.strip():
                bag[t.strip()] += 1
    return [w for w, _ in bag.most_common(max_words)]
