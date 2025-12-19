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

[기술 스택]

## Getting Started

### Installation

flask 세팅

가상환경 세팅 python -m venv venv

가상환경 실행
vevn\Scripts\activate

필요한 라이브러리 설치 pip install -r requirements.txt

- 추가 정보
  가상환경 종료
  venv\Scripts\deactivate

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

[주요기능]

## License

이 프로젝트는 한성대학교 기업연계 SW캡스톤디자인 수업에서 진행되었습니다.
