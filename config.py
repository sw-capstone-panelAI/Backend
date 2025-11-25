import os
from sentence_transformers import SentenceTransformer
from anthropic import Anthropic
from dotenv import load_dotenv
load_dotenv()

# --- Database ---
user = os.getenv('PG_USER')
password = os.getenv('PG_PASSWORD')
host = os.getenv('PG_HOST')
database = os.getenv('PG_DB') 
port = os.getenv('PG_PORT')

DATABASE_CONNECTION_URI = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

# --- LLM / embeddingModel ---
api_key = os.getenv("ANTHROPIC_API_KEY") # .env에서 llm key값 가져옴
antropicLLM = Anthropic(api_key=api_key) # 백엔드에서 사용할 llm 객체 생성

enbedding_model = os.getenv("EMBEDDING_MODEL") # 임베딩 모델 이름 불러오기
embedder = SentenceTransformer(enbedding_model) # 임베딩 모델 객체 생성
EMBED_DIM = embedder.get_sentence_embedding_dimension() # 임베딩 차원