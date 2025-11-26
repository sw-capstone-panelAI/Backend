# 샘플쿼리와 자연어 쿼리를 임베딩합니다.

def sampleQueryEmbedding(user_query: str):

    # RDS pgvector에 저장된 샘플쿼리.json의 벡터화 데이터들과 
    # 입력된 자연어 쿼리와의 유사도 검색을하여 유사도 top-k  높은순 10개를 추출하여
    #  json 형태로 리턴한다.

    return 