import os
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
load_dotenv()

user = os.getenv('PG_USER')
password = os.getenv('PG_PASSWORD')
host = os.getenv('PG_HOST')
database = os.getenv('PG_DB') 
port = os.getenv('PG_PORT')

DATABASE_CONNECTION_URI = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

MODEL_NAME = "nlpai-lab/KURE-v1" # 임베딩 모델
embedder = SentenceTransformer(MODEL_NAME) # 임베딩 모델 불러오기
EMBED_DIM = embedder.get_sentence_embedding_dimension() # 임베딩 차원