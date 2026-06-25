import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# 인증 키
cred = credentials.Certificate(
    "../configs/serviceAccountKey.json"
)

# Firebase 초기화
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://fleet-313a7-default-rtdb.asia-southeast1.firebasedatabase.app/'
})

# 테스트 데이터 저장
ref = db.reference("test")

ref.set({
    "message": "firebase connected",
    "status": True
})

print("Firebase 연결 성공")