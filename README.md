﻿## AWS 리소스 모니터
1.0  CLI 사용

2.0  boto 라이브러리 사용


아래 형식의 accounts.json파일 필요
[
  {
    "account_id": "1234567891011",
    "role_name": "role_name",
    "name": "profile_name"
  }
]


EC2, RDS 개수 확인

멀티스레딩 적용으로 속도 향상