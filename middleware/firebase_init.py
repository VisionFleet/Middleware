import os
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, db
from dotenv import load_dotenv


# 프로젝트 루트 기준으로 .env 로드
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

cred_path = BASE_DIR / os.getenv("FIREBASE_CREDENTIAL_PATH")
database_url = os.getenv("FIREBASE_DATABASE_URL")

if not cred_path.exists():
    raise FileNotFoundError(f"서비스 계정 키 파일을 찾을 수 없습니다: {cred_path}")

if not database_url:
    raise ValueError("FIREBASE_DATABASE_URL이 .env에 없습니다.")

cred = credentials.Certificate(str(cred_path))

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        "databaseURL": database_url
    })

initial_data = {
    "sensingTable": {
        "AGV_01": {
            "status": "idle",
            "current_node": "A",
            "next_node": "B",
            "battery": 100,
            "distance": 999,
            "obstacle": False,
            "updated_at": ""
        },
        "AGV_02": {
            "status": "idle",
            "current_node": "A",
            "next_node": "B",
            "battery": 100,
            "distance": 999,
            "obstacle": False,
            "updated_at": ""
        }
    },
    "commandTable": {},
    "eventTable": {},
    "mapTable": {
        "edges": {
            "A-B": {"from": "A", "to": "B", "status": "open", "cost": 1},
            "B-C": {"from": "B", "to": "C", "status": "open", "cost": 1},
            "B-D": {"from": "B", "to": "D", "status": "open", "cost": 1},
            "D-C": {"from": "D", "to": "C", "status": "open", "cost": 1}
        }
    }
}

db.reference("/").update(initial_data)

print("Firebase 초기 테이블 생성 완료")