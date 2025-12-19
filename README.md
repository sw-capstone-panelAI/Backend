# PanelFinder BackEnd

https://github.com/user-attachments/assets/4bf3a125-a012-4d6c-8c82-bc0c87eeaba3

## Preview

<img width="1587" height="2245" alt="판넬" src="https://github.com/user-attachments/assets/57e43de9-e7a8-4de3-8f58-c683a8d593db" />

### Members

<table width="50%" align="center">
    <tr>
        <td align="center"><b>LEAD/FE</b></td>
        <td align="center"><b>FE/BE</b></td>
        <td align="center"><b>AI/DATA</b></td>
        <td align="center"><b>AI/DB</b></td>
    </tr>
    <tr>
        <td align="center"><img src="https://avatars.githubusercontent.com/u/173050233?v=4" /></td>
        <td align="center"><img src="https://avatars.githubusercontent.com/u/120187934?v=4" /></td>
        <td align="center"><img src="https://avatars.githubusercontent.com/u/132585785?v=4"></td>
        <td align="center"><img src="https://avatars.githubusercontent.com/u/203871424?v=4" /></td>
    </tr>
    <tr>
        <td align="center"><b><a href="https://github.com/Aegis0424">안성민</a></b></td>
        <td align="center"><b><a href="https://github.com/seungjin777">강승진</a></b></td>
        <td align="center"><b><a href="https://github.com/iral304">송정은</a></b></td>
        <td align="center"><b><a href="https://github.com/hyeon-414">우현</a></b></td> 
    </tr>
</table>

## Tech Stack

### 🐍 Core Framework

- **Python 3.x** - 런타임 환경
- **Flask 3.1.2** - 웹 프레임워크
- **Flask-CORS 6.0.1** - CORS 처리

### 🗄️ Database & ORM

- **PostgreSQL** - 관계형 데이터베이스
- **SQLAlchemy 2.0.44** - ORM
- **pgvector 0.4.1** - 벡터 유사도 검색

### 🤖 AI & Machine Learning

- **Anthropic Claude API 0.72.0** - LLM (자연어→SQL 변환)
  - claude-sonnet-4-5-20250929 (Fast Mode)
  - claude-opus-4-5-20251101 (Deep Mode)
- **Sentence Transformers 5.1.2** - 텍스트 임베딩
- **PyTorch 2.9.0** - 딥러닝 프레임워크

### 🔧 Utility

- **python-dotenv 1.2.1** - 환경 변수 관리
- **Pydantic 2.12.4** - 데이터 검증

## Getting Started

### Installation

flask 세팅

가상환경 세팅

```bash
python -m venv venv
```

가상환경 실행

```bash
venv\Scripts\activate
```

필요한 라이브러리 설치

```bash
pip install -r requirements.txt
```

### Environment Variables

`.env` 파일 생성 후 다음 내용 입력:

```env
# Database
PG_USER=<사용자명>
PG_PASSWORD=<비밀번호>
PG_HOST=<호스트>
PG_DB=<데이터베이스명>
PG_PORT=<포트>

# AI
ANTHROPIC_API_KEY=<Claude API 키>
EMBEDDING_MODEL=<임베딩 모델명>
```

### Run

```bash
python app.py
```

- 추가 정보
  가상환경 종료

```bash
  venv\Scripts\deactivate
```

## Project Structure

```
📦Backend
┃📂panel_app
┃ ┣ 📂models
┃ ┃ ┗ 📜__init__.py
┃ ┣ 📂routes
┃ ┃ ┣ 📜api.py
┃ ┃ ┗ 📜__init__.py
┃ ┣ 📂services
┃ ┃ ┣ 📜common.py
┃ ┃ ┣ 📜embedding.py
┃ ┃ ┣ 📜exportCSV.py
┃ ┃ ┣ 📜keyword.py
┃ ┃ ┣ 📜reliability.py
┃ ┃ ┣ 📜tabel_schema_info.json
┃ ┃ ┣ 📜text2sql.py
┃ ┃ ┗ 📜__init__.py
┃ ┗ 📜__init__.py
┣ 📜.env
┣ 📜config.py
┗ 📜requirements.txt
```

## Key Features

### 🔍 AI 기반 자연어 검색

- **자연어 → SQL 자동 변환**: Claude API를 활용한 지능형 쿼리 생성
- **Fast Mode**: Claude Sonnet 4.5로 빠른 검색 (일반 프롬프트)
- **Deep Mode**: Claude Opus 4.5 Extended Thinking으로 정교한 검색 (벡터 유사도 검색)
- **샘플 쿼리 학습**: 유사한 과거 쿼리를 참조하여 SQL 생성 정확도 향상

### 📊 벡터 유사도 검색

- **pgvector 기반**: PostgreSQL 벡터 확장으로 의미론적 유사도 계산
- **Sentence Transformers**: 텍스트를 고차원 임베딩 벡터로 변환
- **Top-K 검색**: 입력 쿼리와 가장 유사한 샘플 쿼리 추출

### ✅ 신뢰도 점수 시스템

- **15개 규칙 기반 검증**: 논리적 모순 및 데이터 무결성 체크
- **100점 만점 계산**: 필수 정보 누락(-26점), 규칙 위반(-5점)
- **상세 감점 사유 제공**: 각 패널의 신뢰도 저하 원인 명시

### 🎯 공통 특성 분석

- **인구통계 데이터 집계**: 성별, 연령, 거주지, 소득, 직업 등 자동 분석
- **라이프스타일 패턴 분석**: OTT, 운동, 스트레스 해소, AI 챗봇 사용 등
- **AI 요약 생성**: Claude API로 패널 집단 특성 및 마케팅 전략 제안
- **상위 5개 키워드 추출**: 공통 특성을 한눈에 파악

### 🔑 추천 검색어 생성

- **AI 기반 키워드 추천**: 입력 쿼리와 연관된 6개 키워드 자동 생성
- **쿼리 증강 기능**: 선택한 키워드로 자연어 쿼리 재구성
- **검색 정확도 향상**: 테이블 스키마 기반 최적화된 검색어 제안

### 📥 데이터 내보내기

- **CSV 파일 생성**: 검색된 패널 데이터를 CSV 형식으로 내보내기
- **UTF-8 BOM 인코딩**: 한글 깨짐 방지
- **신뢰도 정보 포함**: 신뢰도 점수 및 감점 사유 함께 제공

### 🏗️ 모듈화된 아키텍처

- **Blueprint 패턴**: API 엔드포인트 모듈화
- **Service Layer**: 비즈니스 로직 분리 (text2sql, embedding, reliability, common, keyword, exportCSV)
- **Factory Pattern**: 확장 가능한 앱 생성 구조

## API Endpoints

```
POST /api/search                    # 패널 검색 (자연어 → SQL)
POST /api/common-characteristics    # 공통 특성 분석
POST /api/related-keywords          # 추천 검색어 생성
POST /api/keywords-newQuery         # 키워드 기반 쿼리 재생성
POST /api/export-csv                # CSV 내보내기
```

# ETC

기업 고객의 개인정보 유출 방지를 위해 기업 데이터, 개인정보 파일은 삭제 처리 되었습니다.
위 파일을 클론하여도 작동하지 않는게 정상입니다.

## License

이 프로젝트는 한성대학교 기업연계 SW캡스톤디자인 수업에서 진행되었습니다.
