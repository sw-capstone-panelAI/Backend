# ============================================================
# 추천 검색어 기능
# ============================================================
from flask import jsonify
from config import embedder # 임베딩 객체 불러옴 (쿠레)
from sentence_transformers import util

# 키워드 후보군
KEYWORD_POOL = [
    '20대 여성', '서울 거주', '직장인', '월소득 300만원', '미혼', '대졸', 'IT업계',
    '베이비붐 세대', '프리랜서', '운동 좋아함', '30대 남성', '부동산 투자', '경기도',
    '여행', '육아', '부모'
]

def makeKeyword(user_query, top_n):
    # 쿼리와 키워드 유사도 비교 후 top 7 추출
    cand_emb = embedder.encode(KEYWORD_POOL, convert_to_tensor=True)
    q_emb = embedder.encode(user_query, convert_to_tensor=True)
    sims = util.pytorch_cos_sim(q_emb, cand_emb).cpu().numpy().flatten()
    indices = sims.argsort()[::-1][:top_n]
    related = [
        {'text': KEYWORD_POOL[i], 'similarity': float(sims[i])} for i in indices
    ]

    return jsonify({'keywords': related})