# Spark Thrift Server Metastore 사용 가이드

## 개요

StarRocks는 Spark Thrift Server를 Hive Metastore의 대안으로 사용할 수 있습니다. 이 가이드는 Spark Thrift Server metastore를 설정하고 사용하는 방법을 설명합니다.

---

## 설정 가이드

### 필수 설정

Spark Thrift Server catalog를 생성하려면 다음 파라미터가 필요합니다:

| 파라미터 | 설명 | 필수 여부 | 기본값 |
|---------|------|----------|--------|
| `type` | Catalog 타입 | 필수 | - |
| `hive.metastore.type` | Metastore 타입 | 필수 | - |
| `spark.thrift.server.jdbc.url` | Spark Thrift Server JDBC URL | 필수 | - |

#### 기본 Catalog 생성

```sql
CREATE EXTERNAL CATALOG spark_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://spark-server:10000"
);
```

### 선택적 설정

| 파라미터 | 설명 | 기본값 |
|---------|------|--------|
| `spark.thrift.server.username` | JDBC 연결 사용자명 | - |
| `spark.thrift.server.password` | JDBC 연결 비밀번호 | - |
| `spark.thrift.server.timeout` | 연결 타임아웃 (초) | 600 |
| `spark.thrift.server.connection.pool.size` | 최대 연결 풀 크기 | 32 |
| `spark.thrift.server.principal` | Kerberos principal | - |

#### 인증 설정 예시

```sql
CREATE EXTERNAL CATALOG spark_catalog_auth
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://spark-server:10000",
    "spark.thrift.server.username" = "hive",
    "spark.thrift.server.password" = "password123"
);
```

#### Kerberos 인증 예시

```sql
CREATE EXTERNAL CATALOG spark_catalog_kerberos
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://spark-server:10000/default;principal=hive/_HOST@REALM",
    "spark.thrift.server.principal" = "hive/_HOST@REALM"
);
```

#### 성능 튜닝 예시

```sql
CREATE EXTERNAL CATALOG spark_catalog_tuned
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://spark-server:10000",
    "spark.thrift.server.timeout" = "300",
    "spark.thrift.server.connection.pool.size" = "64"
);
```

---

## 사용 예시

### 데이터베이스 조회

```sql
-- Catalog의 모든 데이터베이스 조회
SHOW DATABASES FROM spark_catalog;

-- 특정 데이터베이스 정보 조회
DESCRIBE DATABASE spark_catalog.my_database;
```

### 테이블 조회

```sql
-- 데이터베이스의 모든 테이블 조회
SHOW TABLES FROM spark_catalog.my_database;

-- 테이블 스키마 조회
DESCRIBE spark_catalog.my_database.my_table;

-- 테이블 상세 정보 조회
SHOW CREATE TABLE spark_catalog.my_database.my_table;
```

### 파티션 조회

```sql
-- 테이블의 모든 파티션 조회
SHOW PARTITIONS FROM spark_catalog.my_database.partitioned_table;

-- 특정 파티션 데이터 조회
SELECT * FROM spark_catalog.my_database.partitioned_table
WHERE year = 2023 AND month = 12;
```

### 데이터 쿼리

```sql
-- 기본 쿼리
SELECT * FROM spark_catalog.my_database.my_table LIMIT 10;

-- 조인 쿼리
SELECT a.*, b.name
FROM spark_catalog.db1.table1 a
JOIN spark_catalog.db2.table2 b ON a.id = b.id;

-- 집계 쿼리
SELECT year, month, COUNT(*) as cnt
FROM spark_catalog.my_database.partitioned_table
GROUP BY year, month;
```

---

## 제약사항

> [!WARNING]
> **읽기 전용 Metastore**
> 
> Spark Thrift Server metastore는 **읽기 전용**입니다. 다음 작업은 지원하지 않습니다:

### 지원하지 않는 작업

- ❌ 데이터베이스 생성/삭제 (`CREATE DATABASE`, `DROP DATABASE`)
- ❌ 테이블 생성/삭제 (`CREATE TABLE`, `DROP TABLE`)
- ❌ 테이블 변경 (`ALTER TABLE`)
- ❌ 파티션 추가/삭제 (`ALTER TABLE ADD PARTITION`, `DROP PARTITION`)
- ❌ 통계 정보 업데이트 (`ANALYZE TABLE`)

### 지원하는 작업

- ✅ 데이터베이스 조회 (`SHOW DATABASES`, `DESCRIBE DATABASE`)
- ✅ 테이블 조회 (`SHOW TABLES`, `DESCRIBE TABLE`)
- ✅ 파티션 조회 (`SHOW PARTITIONS`)
- ✅ 데이터 쿼리 (`SELECT`)
- ✅ 테이블 통계 조회 (Spark 통계 파라미터 기반)

---

## 트러블슈팅

### 연결 문제

#### 문제: "Failed to load Hive JDBC driver"

**원인**: Hive JDBC 드라이버가 클래스패스에 없습니다.

**해결**:
1. `fe/fe-core/pom.xml`에 `hive-jdbc` 의존성이 있는지 확인
2. StarRocks FE를 재시작

```bash
# FE 재시작
cd starrocks/fe
./bin/stop_fe.sh
./bin/start_fe.sh
```

#### 문제: "Connection refused"

**원인**: Spark Thrift Server에 연결할 수 없습니다.

**해결**:
1. Spark Thrift Server가 실행 중인지 확인
2. JDBC URL이 올바른지 확인
3. 방화벽 설정 확인

```bash
# Spark Thrift Server 상태 확인
netstat -an | grep 10000

# Spark Thrift Server 시작
$SPARK_HOME/sbin/start-thriftserver.sh
```

#### 문제: "Authentication failed"

**원인**: 인증 정보가 잘못되었습니다.

**해결**:
1. 사용자명/비밀번호 확인
2. Kerberos 설정 확인 (Kerberos 사용 시)

```sql
-- Catalog 재생성 with 올바른 인증 정보
DROP CATALOG spark_catalog;

CREATE EXTERNAL CATALOG spark_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://spark-server:10000",
    "spark.thrift.server.username" = "correct_username",
    "spark.thrift.server.password" = "correct_password"
);
```

### 성능 문제

#### 문제: 쿼리가 느림

**원인**: 연결 풀 크기가 부족하거나 타임아웃이 짧습니다.

**해결**:
1. 연결 풀 크기 증가
2. 타임아웃 증가

```sql
ALTER CATALOG spark_catalog SET PROPERTIES (
    "spark.thrift.server.connection.pool.size" = "128",
    "spark.thrift.server.timeout" = "1200"
);
```

#### 문제: "Connection pool exhausted"

**원인**: 동시 쿼리가 너무 많습니다.

**해결**:
1. 연결 풀 크기 증가
2. 쿼리 동시성 제한

```sql
-- 연결 풀 크기 증가
ALTER CATALOG spark_catalog SET PROPERTIES (
    "spark.thrift.server.connection.pool.size" = "256"
);
```

### 메타데이터 문제

#### 문제: "Table not found"

**원인**: 테이블이 Spark에 존재하지 않거나 권한이 없습니다.

**해결**:
1. Spark에서 테이블 존재 여부 확인
2. 권한 확인

```sql
-- Spark Thrift Server에서 직접 확인
-- beeline 사용
beeline -u jdbc:hive2://spark-server:10000 -n username -p password
> SHOW TABLES IN my_database;
```

#### 문제: 통계 정보가 없음

**원인**: Spark 테이블에 통계가 수집되지 않았습니다.

**해결**:
Spark에서 통계 수집:

```sql
-- Spark SQL에서 실행
ANALYZE TABLE my_database.my_table COMPUTE STATISTICS;
```

---

## 로깅 및 디버깅

### FE 로그 확인

```bash
# FE 로그 위치
tail -f starrocks/fe/log/fe.log

# Spark Thrift Server 관련 로그 필터링
grep "SparkThriftServer" starrocks/fe/log/fe.log
```

### 디버그 로깅 활성화

`fe/conf/fe.conf`에 추가:

```properties
# Spark Thrift Server 디버그 로깅
sys_log_level = INFO
```

FE 재시작 후 적용됩니다.

---

## 모범 사례

### 1. 연결 풀 크기 설정

- **소규모 환경** (< 10 동시 사용자): 32 (기본값)
- **중규모 환경** (10-50 동시 사용자): 64-128
- **대규모 환경** (> 50 동시 사용자): 128-256

### 2. 타임아웃 설정

- **빠른 쿼리** (< 1분): 300초
- **일반 쿼리** (1-5분): 600초 (기본값)
- **긴 쿼리** (> 5분): 1200-1800초

### 3. 보안

- 프로덕션 환경에서는 항상 인증 사용
- Kerberos 사용 권장
- 비밀번호는 암호화된 저장소 사용 권장

### 4. 모니터링

- FE 로그 정기적으로 확인
- 연결 풀 사용률 모니터링
- 쿼리 성능 모니터링

---

## FAQ

### Q: Spark Thrift Server와 HMS의 차이는?

**A**: 
- **프로토콜**: Spark는 JDBC, HMS는 Thrift RPC
- **기능**: 둘 다 읽기 작업 지원, 쓰기는 HMS만 지원
- **성능**: 비슷하지만 네트워크 환경에 따라 다를 수 있음

### Q: 여러 Spark Thrift Server를 사용할 수 있나요?

**A**: 네, 여러 catalog를 생성하여 각각 다른 Spark Thrift Server에 연결할 수 있습니다.

```sql
CREATE EXTERNAL CATALOG spark_catalog1
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://server1:10000"
);

CREATE EXTERNAL CATALOG spark_catalog2
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "spark",
    "spark.thrift.server.jdbc.url" = "jdbc:hive2://server2:10000"
);
```

### Q: Spark 통계가 자동으로 업데이트되나요?

**A**: 아니요. Spark에서 `ANALYZE TABLE` 명령을 실행하여 수동으로 통계를 업데이트해야 합니다.

### Q: 성능을 개선하려면?

**A**:
1. 연결 풀 크기 증가
2. Spark Thrift Server 리소스 증가
3. 네트워크 대역폭 확인
4. 통계 정보 최신 상태 유지

---

## 참고 자료

- [StarRocks 공식 문서](https://docs.starrocks.io/)
- [Spark Thrift Server 문서](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html)
- [Hive JDBC 드라이버 문서](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC)
