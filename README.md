# VS Code Snowflake 작업 공간

이 폴더는 Snowflake 쿼리 작성 및 실행용 VS Code 워크스페이스 템플릿입니다.

빠른 시작

- 환경 변수 설정(권장):

```bash
export SNOWFLAKE_ACCOUNT=<your_account>
export SNOWFLAKE_USER=<your_user>
# SnowSQL은 비밀번호 프롬프트를 사용하거나 ~/.snowsql/config 파일로 설정하세요
```

- VS Code에서 이 폴더 열기:

```bash
code /Users/mhs/src/vscode/snowflake-git
```

- `queries/sample_query.sql` 파일을 열고 `Terminal -> Run Task... -> Run Snowflake Query (current file)` 선택

새 업무(폴더) 추가 방법

- `work/<업무명>` 같은 하위 폴더를 만들고 그 안에 쿼리, 스크립트, 메모를 넣으세요.
- 필요한 경우 `.vscode/tasks.json`에 새 Task를 추가해 각 업무별 실행 흐름을 만들 수 있습니다.

도움말

- SnowSQL 설치: https://docs.snowflake.com/en/user-guide/snowsql
- 인증을 파일로 관리하려면 `~/.snowsql/config`를 사용하세요.

워크스페이스 전용 래퍼 사용

- 목적: 전역 설정을 바꾸지 않고 기존 `snowsql` 호출을 `snow sql`로 안전히 포워딩하기 위해 `.vscode/bin/snowsql` 래퍼를 제공합니다.
- 통합 터미널 접근: 워크스페이스 설정(`.vscode/settings.json`)이 `.vscode/bin`을 `PATH`에 추가하므로 VS Code 통합 터미널에서 바로 `snowsql`을 호출할 수 있습니다.
- 사용 예:
	- 파일 실행 (Task 또는 스크립트에서): `snowsql -f queries/sample_query.sql` (래퍼가 `snow sql`로 포워딩)
	- 버전 확인: `.vscode/check_snowsql.sh` 실행

추가 참고: 래퍼는 우선 `snow`(Snowflake CLI)를 사용하고, 없으면 시스템 `snowsql`로 폴백합니다. 글로벌 alias를 만들지 않고 워크스페이스 전용으로 안전하게 전환할 수 있습니다.

설치 및 마이그레이션 (권장)

1) Snowflake CLI 설치(권장)

- macOS (Homebrew):

```bash
brew install snowflake-cli || brew install --cask snowflake-cli
```

- 또는 pip (Python):

```bash
pip install --user snowflake-cli
```

- 플랫폼별 설치/최신 정보는 공식 문서 참조: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation

2) 기존 SnowSQL 설정 마이그레이션

- Snowflake CLI는 SnowSQL 설정을 가져오는 헬퍼를 제공합니다:

```bash
snow helpers import-snowsql-connections
snow helpers check-snowsql-env-vars
```

- 또는 수동으로 `~/.snowsql/config`의 연결 정보를 `~/.snowflake/config.toml`로 옮긴 후 이름 변경(예: `accountname` → `account`, `username` → `user`)을 수행하세요. 마이그레이션 가이드는 문서에 상세히 설명되어 있습니다.

3) 예시 명령

- 쿼리 실행

```bash
snow sql -f queries/sample_query.sql
snow sql -q "select 1"
```

- 이전 스크립트(또는 CI)에서 `snowsql`을 그대로 사용하려면 워크스페이스 래퍼 또는 alias 사용:

```bash
# 워크스페이스에서는 .vscode/bin/snowsql 래퍼를 사용합니다 (권장)
alias snowsql='snow sql'  # 전역 변경을 원할 때만 사용
```

4) 문제 해결

- `snow --info`로 기본 설정 파일 위치 확인 가능:

```bash
snow --info
```

- 연결 테스트:

```bash
snow connection test
```

이 섹션을 따라 설치·마이그레이션을 완료하면 워크스페이스의 Task와 스크립트(`.vscode/bin/snowsql` 래퍼 포함)가 `snow`(Snowflake CLI)를 우선 사용하도록 원활히 동작합니다.
