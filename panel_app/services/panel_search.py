#패널 검색시 필요한 함수(서비스)들
from sqlalchemy import text, bindparam
from pgvector.sqlalchemy import Vector
from panel_app.models import db
from config import embedder # 임베딩 객체 불러옴 (쿠레)
import numpy as np

# pgvector 컬럼 차원 (DB의 embeding 컬럼 차원과 반드시 일치)
EMBED_DIM = embedder.get_sentence_embedding_dimension() # 임베딩 차원
DEFAULT_SIM_THRESHOLD = 0.6 # 유사도 필터 기준

# panel_app/services/search_service.py (이어짐)

def embed_text_to_vector(query: str) -> list[float]:
    """
    (1) 자연어 쿼리 → (2) 임베딩 벡터화
    - 항상 batch 입력으로 호출해 2D를 강제한 뒤, 첫 행만 꺼낸다.
    - 최종적으로 1D python list[float]을 반환한다.
    """
    # 2D (1, dim)를 강제
    vecs = embedder.encode([query], normalize_embeddings=True)

    # numpy → list 변환 및 차원 방어
    if isinstance(vecs, np.ndarray):
        if vecs.ndim == 2 and vecs.shape[0] == 1:
            vec = vecs[0].tolist()
        elif vecs.ndim == 1:
            vec = vecs.tolist()
        else:
            raise ValueError(f"Unexpected embedding shape: {vecs.shape}")
    else:
        # 일부 임베더는 list[list[float]] 형태를 돌려줌
        if not vecs:
            raise ValueError("Empty embedding result")
        first = vecs[0]
        vec = list(first) if isinstance(first, (list, np.ndarray)) else list(vecs)

    # 차원 확인 (DB 컬럼 차원과 일치해야 함)
    if len(vec) != EMBED_DIM:
        raise ValueError(f"Embedding dim mismatch: got {len(vec)}, expected {EMBED_DIM}")

    return vec


# panel_app/services/search_service.py (이어짐)

# 안전한 리터럴 → vector 캐스팅
def to_vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(map(str, vec)) + "]"

def search_panels_by_cosine(query_vec: list[float],
                            sim_threshold: float = 0.6,
                            top_k: int = 20) -> list[dict]:
    sim_threshold = max(0.0, min(1.0, float(sim_threshold)))
    dist_th = 1.0 - sim_threshold

    sql = text(f"""
        SELECT
            panel_id,
            profile,
            1 - (embedding <=> CAST(:qvec AS vector)) AS cosine_similarity
        FROM panel_profiles
        WHERE (embedding <=> CAST(:qvec AS vector)) <= :dist_th
        ORDER BY cosine_similarity DESC
        LIMIT :k
    """)

    params = {
        "qvec": to_vector_literal(query_vec),  # 리터럴 문자열, 예: "[0.1,0.2,...]"
        "dist_th": dist_th,
        "k": int(top_k),
    }

    rows = db.session.execute(sql, params).mappings().all()

    return [
        {
            "panel_id": r["panel_id"],
            "profile": r["profile"],
            "cosine_similarity": float(r["cosine_similarity"]),
        }
        for r in rows
    ]


def fetch_responses_by_panel_ids(panel_ids: list):
    """
    panel_ids에 들어있는 ID와 일치하는 panel_responses 전체 행을 가져옴
    - SQLAlchemy의 expanding 바인딩을 사용해서 안전하게 IN 조건 처리
    """
    if not panel_ids:
        return []

    sql = text("""
        SELECT r.*
        FROM panel_responses AS r
        WHERE r."패널id" IN :ids
        ORDER BY r."패널id"
    """).bindparams(bindparam("ids", expanding=True))

    rows = db.session.execute(sql, {"ids": panel_ids}).mappings().all()
    return [dict(r) for r in rows]