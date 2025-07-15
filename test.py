import smtplib
from email.mime.text import MIMEText

def send_email(to_email, subject, body):
    smtp_user = "datadev2402@gmail.com"              # Gmail 주소
    smtp_password = "wccq lyyc vmyy exmd"              # Gmail 앱 비밀번호
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    # 메일 본문 구성
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_user
    msg['To'] = to_email

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # TLS 보안 시작
            server.login(smtp_user, smtp_password)  # 로그인
            server.sendmail(smtp_user, to_email, msg.as_string())  # 메일 전송
            print("✅ 메일 전송 완료")
    except Exception as e:
        print(f"❌ 메일 전송 실패: {e}")

# 예시 호출
if __name__ == "__main__":
    send_email(
        to_email="ryeong2105@gmail.com",  # 수신자 주소
        subject="[탄소배출권 경고] 📉",
        body="현재 배출권 사용량이 설정한 임계치(95%)를 초과했습니다."
             "추가 구매 또는 감축 조치가 필요합니다."
    )
