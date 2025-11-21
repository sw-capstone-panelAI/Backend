# ============================================================
# 추천 검색어 기능
# ============================================================

# 키워드 후보군
KEYWORD_POOL = [
    '20대 여성', '서울 거주', '직장인', '월소득 300만원', '미혼', '대졸', 'IT업계',
    '베이비붐 세대', '프리랜서', '운동 좋아함', '30대 남성', '부동산 투자', '경기도',
    '여행', '육아', '부모'
]

def makeKeyword(user_query, top_n):
    
    
    if not user_query:
        return jsonify({'keywords': []})
    
    cand_emb = kure_model.encode(KEYWORD_POOL, convert_to_tensor=True)
    q_emb = kure_model.encode(user_query, convert_to_tensor=True)
    sims = util.pytorch_cos_sim(q_emb, cand_emb).cpu().numpy().flatten()
    indices = sims.argsort()[::-1][:top_n]
    related = [
        {'text': KEYWORD_POOL[i], 'similarity': float(sims[i])} for i in indices
    ]