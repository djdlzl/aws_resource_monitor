#!/usr/bin/python3

import json
import sqlite3
import requests
import os
from datetime import datetime
from config import *
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import configparser



def load_accounts():
    with open(ACCOUNTS_FILE, "r") as f:
        return json.load(f)

def get_all_regions(profile_name):
    """모든 AWS 리전 목록 가져오기"""
    session = boto3.Session(profile_name=profile_name)
    ec2 = session.client('ec2')
    regions = [region['RegionName'] for region in ec2.describe_regions()['Regions']]
    return regions
def remove_expired_profiles(profile_name):
    """ 만료된 프로필을 ~/.aws/credentials 파일에서 안전하게 삭제 """
    if not os.path.exists(AWS_CREDENTIALS_FILE):
        return

    # configparser를 사용하여 파일 파싱
    config = configparser.ConfigParser()
    config.read(AWS_CREDENTIALS_FILE)
    print("이까지 실행 완료")
    # 프로필 삭제
    if profile_name in config:
        config.remove_section(profile_name)

        # 변경된 내용 저장
        with open(AWS_CREDENTIALS_FILE, "w") as f:
            config.write(f)

        print(f"만료된 프로필 삭제 완료: {profile_name}")
    else:
        print(f"삭제할 프로필이 존재하지 않음: {profile_name}")

import botocore

def refresh_credentials(account):
    role_arn = f"arn:aws:iam::{account['account_id']}:role/{account['role_name']}"
    profile_name = account["name"]
    print(f'refresh_credentials | profile_name: {profile_name}')
    
    try:
        # 현재 자격 증명 확인
        session = boto3.Session(profile_name=profile_name)
        sts = session.client('sts')

        try:
            identity = sts.get_caller_identity()
            current_arn = identity['Arn']
            print("refresh_credentials - ", identity)

            # 기존 자격 증명이 여전히 유효한 경우
            if current_arn.startswith(f"arn:aws:sts::{account['account_id']}:assumed-role/{account['role_name']}"):
                print(f"✅ 이미 {profile_name} 역할 사용 중, 갱신 생략")
                return
            else:
                print("⚠️ 현재 자격 증명이 유효하지 않음, 새로 발급 필요")
                
        except botocore.exceptions.ClientError as e:
            print(f"🔴 STS 호출 오류 발생: {str(e)}")
            print("⚠️ 기존 자격 증명 무효화, 새로 발급 시작")

        except botocore.exceptions.NoCredentialsError:
            print("🔴 자격 증명 없음: 새로 발급 필요")

        except Exception as e:
            print(f"⚠️ 알 수 없는 예외 발생: {str(e)}")
        
        # 기존 프로필 삭제
        print("🛠 만료된 기존 프로필 삭제 로직 시작")
        remove_expired_profiles(profile_name)
        
        # 자격 증명 갱신
        assumed_role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName=SESSION_NAME,
            DurationSeconds=43200
        )
        
        # 자격 증명 저장 - 형식 수정
        with open(AWS_CREDENTIALS_FILE, 'a') as f:
            f.write(f"\n[{profile_name}]\n")
            f.write(f"aws_access_key_id={assumed_role['Credentials']['AccessKeyId']}\n")
            f.write(f"aws_secret_access_key={assumed_role['Credentials']['SecretAccessKey']}\n")
            f.write(f"aws_session_token={assumed_role['Credentials']['SessionToken']}\n")
            f.write("region=ap-northeast-2\n")  # 기본 리전 설정
            
        print(f"✅ 자격 증명 갱신 완료: {profile_name}")

    except botocore.exceptions.ClientError as e:
        print(f"🚨 STS AssumeRole 실패: {str(e)}")
    except Exception as e:
        print(f"🚨 예외 발생: {str(e)}")


def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # 새로운 스키마로 테이블 생성
        c.execute('''CREATE TABLE IF NOT EXISTS instance_counts (
            id INTEGER,
            sequence_id INTEGER,
            timestamp TEXT,
            account_name TEXT,
            account_id TEXT,
            region TEXT,
            ec2_count INTEGER,
            rds_count INTEGER,
            PRIMARY KEY (account_name, region, sequence_id)
        )''')
        
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB 작업 실패: ", e)

def get_or_create_id(c, account_name, account_id, region):
    """계정-리전별 고유 ID 조회 또는 생성"""
    c.execute("""
        SELECT id 
        FROM instance_counts 
        WHERE account_name = ? AND region = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (account_name, region))
    
    result = c.fetchone()
    if result:
        return result[0]
    
    # 새로운 계정-리전 조합인 경우에만 새 ID 생성
    c.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM instance_counts")
    return c.fetchone()[0]

def save_to_db_optimized(c, account_name, account_id, region, ec2_count, rds_count):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 계정-리전별 고유 ID 가져오기 (account_id 추가)
    fixed_id = get_or_create_id(c, account_name, account_id, region)
    
    # 나머지 로직은 동일...
    c.execute("""
        SELECT COUNT(*), COALESCE(MAX(sequence_id), 0)
        FROM instance_counts 
        WHERE account_name = ? AND region = ?
    """, (account_name, region))
    
    count, max_sequence = c.fetchone()
    
    if count < 12:
        new_sequence = max_sequence + 1
    else:
        # 가장 오래된 데이터의 sequence_id 재사용
        c.execute("""
            SELECT sequence_id
            FROM instance_counts 
            WHERE account_name = ? 
            AND region = ? 
            AND timestamp = (
                SELECT MIN(timestamp)
                FROM instance_counts
                WHERE account_name = ?
                AND region = ?
            )
        """, (account_name, region, account_name, region))
        new_sequence = c.fetchone()[0]
        
        # 가장 오래된 데이터 삭제
        c.execute("""
            DELETE FROM instance_counts 
            WHERE account_name = ? 
            AND region = ? 
            AND timestamp = (
                SELECT MIN(timestamp)
                FROM instance_counts
                WHERE account_name = ?
                AND region = ?
            )
        """, (account_name, region, account_name, region))
    
    # 새로운 데이터 저장 (고정 ID 사용)
    c.execute("""
        INSERT INTO instance_counts 
        (id, sequence_id, timestamp, account_name, account_id, region, ec2_count, rds_count) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (fixed_id, new_sequence, timestamp, account_name, account_id, region, ec2_count, rds_count))

def get_instance_counts(profile_name, account, region):
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            ec2_future = executor.submit(get_ec2_count, profile_name, region)
            rds_future = executor.submit(get_rds_count, profile_name, region)
            
            ec2_count = ec2_future.result()
            rds_count = rds_future.result()
            
        print(f"계정: {profile_name}, account id: {account}, 리전: {region} - EC2: {ec2_count}, RDS: {rds_count}")
        return ec2_count, rds_count
    except Exception as e:
        print(f"오류 발생 ({profile_name}:{account} - {region}): {e}")
        return 0, 0

def get_ec2_count(profile_name, region):
    try:
        session = boto3.Session(profile_name=profile_name)
        ec2 = session.client('ec2', region_name=region)
        
        # 모든 상태의 인스턴스 조회
        instances = ec2.describe_instances(
            Filters=[{
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'stopping', 'stopped', 'shutting-down']
            }]
        )
        
        count = sum(len(reservation['Instances']) for reservation in instances['Reservations'])
        return count
    except ClientError:
        return 0

def get_rds_count(profile_name, region):
    try:
        session = boto3.Session(profile_name=profile_name)
        rds = session.client('rds', region_name=region)
        
        # 일반 RDS 인스턴스 조회
        instances = rds.describe_db_instances()
        standalone_instances = len([db for db in instances['DBInstances'] 
                                 if db['DBInstanceStatus'] != 'deleted' 
                                 and 'DBClusterIdentifier' not in db])
        
        # Aurora 클러스터 내 인스턴스 조회
        clusters = rds.describe_db_clusters()
        cluster_instances = sum(len([member for member in cluster['DBClusterMembers'] 
                                  if 'DBInstanceIdentifier' in member]) 
                              for cluster in clusters['DBClusters'])
        
        return standalone_instances + cluster_instances
    except ClientError as e:
        print(f"RDS 조회 오류: {str(e)}")
        return 0

def send_notifications(account_name, region, ec2_count, rds_count):
    """Slack과 AlertNow로 알림 전송"""
    messages = []
    if ec2_count >= EC2_THRESHOLD:
        messages.append(f"EC2 새 인스턴스 수가 임계값을 넘었습니다!\n계정 {account_name}\n리전 {region}\n새로 생성된 EC2: {ec2_count}대 (임계값: {EC2_THRESHOLD})")

    if rds_count >= RDS_THRESHOLD:
        messages.append(f"RDS 새 인스턴스 수가 임계값을 넘었습니다!\n계정 {account_name}\n리전 {region}\n새로 생성된 RDS: {rds_count}대 (임계값: {RDS_THRESHOLD})")

    if messages:
        message_text = "\n".join(messages)
        
        # Slack 알림 전송 (봇 토큰 사용)
        slack_headers = {
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json"
        }
        slack_payload = {
            "channel": SLACK_CHANNEL,
            "text": message_text
        }
        slack_response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=slack_headers,
            json=slack_payload
        )
        print("Slack 상태 코드:", slack_response.status_code)
        if slack_response.status_code != 200:
            print(f"Slack 전송 실패: {slack_response.text}")
            
        # AlertNow 알림 전송
        alertnow_payload = {
            "subject": f"리소스 임계치 초과 알림 - {account_name}/{region}",
            "message": message_text
        }
        alertnow_response = requests.post(
            ALERTNOW_WEBHOOK_URL,
            json=alertnow_payload,
            headers={"Content-Type": "application/json"},
            verify=True
        )
        print("AlertNow 상태 코드:", alertnow_response.status_code)
        if alertnow_response.status_code != 200:
            print(f"AlertNow 전송 실패: {alertnow_response.text}")

def process_region(conn, account, profile_name, region):
    conn = sqlite3.connect(DB_FILE, isolation_level=None)
    try:
        c = conn.cursor()
        ec2_count, rds_count = get_instance_counts(profile_name, account, region)
        save_to_db_optimized(c, profile_name, account, region, ec2_count, rds_count)
        conn.commit()
        # 현재 ID 값 조회
        c.execute("""
            SELECT DISTINCT id 
            FROM instance_counts 
            WHERE account_name = ? AND region = ?
            LIMIT 1
        """, (profile_name, region))
        
        current_id = c.fetchone()
        if not current_id:
            print(f"DEBUG - {region}: 이전 데이터 없음")
            prev_ec2, prev_rds = None, None
        else:
            # 같은 ID를 가진 데이터들 중 timestamp 기준 최근 2개 조회
            c.execute("""
                SELECT ec2_count, rds_count, sequence_id, timestamp
                FROM instance_counts 
                WHERE account_name = ? AND region = ? AND id = ?
                ORDER BY timestamp DESC
                LIMIT 2
            """, (profile_name, region, current_id[0]))
            
            rows = c.fetchall()
            if len(rows) == 1:
                print(f"DEBUG - {region}: 첫 번째 데이터만 있음 (sequence_id: {rows[0][2]})")
                prev_ec2, prev_rds = rows[0][0], rows[0][1]
            else:
                print(f"DEBUG - {region}: 현재 sequence_id: {rows[0][2]}, 이전 sequence_id: {rows[1][2]}")
                print(f"DEBUG - {region}: 현재 시간: {rows[0][3]}, 이전 시간: {rows[1][3]}")
                print(f"DEBUG - {region}: 현재 EC2/RDS: {rows[0][0]}/{rows[0][1]}, 이전 EC2/RDS: {rows[1][0]}/{rows[1][1]}")
                prev_ec2, prev_rds = rows[1][0], rows[1][1]
        
        # 임계치 체크
        if prev_ec2 != None and prev_rds != None:
            new_ec2 = ec2_count - prev_ec2
            new_rds = rds_count - prev_rds
            print(f"DEBUG - {region}: 증감 확인 - EC2: {prev_ec2}->{ec2_count} ({new_ec2}), RDS: {prev_rds}->{rds_count} ({new_rds})")
            
            if new_ec2 >= EC2_THRESHOLD or new_rds >= RDS_THRESHOLD:
                print(f"임계치 초과 발생 - 계정: {profile_name}, 계정: {account},리전: {region}")
                print(f"EC2 증가: {new_ec2}대\n(임계치: {EC2_THRESHOLD})")
                print(f"RDS 증가: {new_rds}대\n(임계치: {RDS_THRESHOLD})")
                send_notifications(profile_name, region, new_ec2, new_rds)
                return True
        return False
    finally:
        conn.close()

def main():
    init_db()
    accounts = load_accounts()
    
    # DB 연결을 한 번만 생성
    conn = sqlite3.connect(DB_FILE)
    try:
        # 모든 계정의 자격 증명을 먼저 갱신
        for account in accounts:
            refresh_credentials(account)  # IAM 역할 갱신

        # 최대 5개씩 병렬로 실행
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            # 계정별 & 리전별 인스턴스 개수 조회
            for account in accounts:
                print("account: ", account)
                profile_name = account["name"]
                print("profile_name: ", profile_name)
                account_id = account["account_id"]
                regions = get_all_regions(profile_name)
                
                for region in regions:
                    futures.append(executor.submit(process_region, conn, account_id, profile_name, region))

            # 모든 스레드가 끝날 때까지 대기
            for future in futures:
                future.result()

        conn.commit()
    finally:
        conn.close()
        
if __name__ == "__main__":
    main()
