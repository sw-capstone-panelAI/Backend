from sqlalchemy import text
from ..models import db
from config import embedder, EMBED_DIM

EMBED_DIM = embedder.get_sentence_embedding_dimension()
print(f"✅ embedding dim: {EMBED_DIM}")


def embed_text(text: str):
    """
    단일 문자열을 입력받아 임베딩 벡터(list[float])로 반환.
    """
    embedding = embedder.encode(
        [text],
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=True
    )
    return embedding[0].tolist()


def sampleQueryEmbedding(user_query: str, top_k: int = 5):
    """
    RDS pgvector에 저장된 sample_sql_templates 테이블의 embedding과
    user_query 임베딩 간의 유사도를 계산해서
    유사도 상위 top_k개의 (input, query)를 JSON 형태(list[dict])로 반환한다.
    """
    # 1) 입력된 자연어 쿼리 문장 임베딩
    query_emb = embed_text(user_query)
    
    # 2) 리스트를 pgvector 문자열 형식으로 변환
    # 예: [0.1, 0.2, 0.3] -> '[0.1,0.2,0.3]'
    qvec_str = '[' + ','.join(map(str, query_emb)) + ']'

    # 3) pgvector 유사도 검색 (CAST로 명시적 타입 변환)
    sql = text(
        """
        SELECT
            "input",
            "query",
            1 - ("embedding" <=> CAST(:qvec AS vector)) AS similarity
        FROM sample_query_vec
        ORDER BY "embedding" <=> CAST(:qvec AS vector)
        LIMIT :top_k;
        """
    )

    # 4) 실행
    with db.engine.connect() as conn:
        rows = conn.execute(sql, {"qvec": qvec_str, "top_k": top_k}).mappings().all()

    # 5) JSON 형태로 변환해서 리턴
    results = [
        {
            "input": row["input"],
            "query": row["query"],
            # "similarity": float(row["similarity"]),  # 필요시 추가
        }
        for row in rows
    ]

    return results