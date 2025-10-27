# Notion Sprint 資料同步工具

此雲端函式會定期從 Notion 官方 Task Tracker 看板擷取 Sprint 與 Task，轉換成分析友善的資料後寫入 BigQuery，協助團隊追蹤每個 Sprint 的故事點數與達成率。

## 功能重點
- 以 HTTP 觸發的 Cloud Function 執行，支援 `mode=current`（僅處理狀態為 Current 的 Sprint）與 `mode=backfill`（由舊到新依序補齊至 Current）。
- 從 `NOTION_CONFIGS_JSON` 取得各環境對應的 Token 與資料庫 ID，同一份程式即可服務多個 Workspace。
- 產出兩個 BigQuery 資料表：所有 Sprint 任務（All Tasks）與通過完成條件的任務（Completed Tasks）。
- 上傳前會先刪除同一 Sprint、相同 Department 的舊資料，避免重複寫入。
- 會將 Sprint 開始日期校正為當週星期一，方便以週為單位分析。

## 執行流程
1. 解析 HTTP Query 參數中的 `env`、`mode` 與選填 `department`（預設為 `N/A`）。
2. 從環境變數載入 BigQuery 設定與 `NOTION_CONFIGS_JSON`，解析對應環境的 Notion Token 與資料庫 ID。
3. 依 `mode` 取得要處理的 Sprint：
   - `current`：回傳狀態為 Current 的 Sprint。
   - `backfill`：依 Sprint 名稱裡的數字排序，自最舊的 Sprint 一路處理到 Current。
4. 對每個 Sprint：
   - 取得專案對照表與該 Sprint 的所有任務。
   - 依任務資料分別建立 All Tasks 與 Completed Tasks 的 pandas DataFrame。
   - 刪除 BigQuery 既有資料後重新載入最新結果。
5. 回傳執行成功或錯誤訊息，方便 Cloud Scheduler 或監控工具使用。

## BigQuery Schema
### `BQ_ALL_TASKS_TABLE_ID`
| 欄位 | 說明 |
| --- | --- |
| id | Notion Task Page ID |
| Task_ID | Notion Unique ID（prefix + number）|
| task_name | 任務名稱 |
| Parent_task | 父任務名稱（若有）|
| sprint | Sprint 名稱 |
| assignee_name | 指派人姓名 |
| estimates | 故事點數（從 Estimates select 轉成整數）|
| Project | 任務所屬專案名稱 |
| Status | 任務狀態文字 |
| Department | 來自 HTTP 參數的部門資訊 |
| sprint_week_start_date | Sprint 週起始日（星期一，date）|

### `BQ_COMPLETED_TASKS_TABLE_ID`
| 欄位 | 說明 |
| --- | --- |
| Task_ID | Notion Unique ID |
| Taskid | Notion Task Page ID |
| completed_sprint | 任務完成所屬的 Sprint |
| assignee_name | 指派人姓名 |
| task_name | 任務名稱 |
| estimates | 故事點數 |
| Department | 部門資訊 |
| sprint_week_start_date | Sprint 週起始日 |

### 完成任務判定規則
- 任務狀態需包含在 `COMPLETED_STATUSES` 列表內。
- 故事點數不可為 0。
- 必須只有一位指派人；多指派的任務會被略過並在日誌中提示。
- 若為父任務，僅當子任務故事點總和為 0 時才會被記錄，避免重複計算。
- 若該 Task 已在其他 Sprint 的 Completed 表中存在，會略過以維持唯一性。

## 必要環境變數
```bash
BQ_PROJECT_ID="your-gcp-project"
BQ_DATASET_ID="analytics_dataset"
BQ_ALL_TASKS_TABLE_ID="notion_sprint_all_tasks"
BQ_COMPLETED_TASKS_TABLE_ID="notion_sprint_completed_tasks"
NOTION_CONFIGS_JSON='{
  "ops": {
    "SPRINT_DB_ID": "1ddb30fbe8b2804fbdfbe7f5f49aba89",
    "TASK_DB_ID": "1dcb30fbe8b280d8b41bf2e4f5f907f2",
    "PROJECT_DB_ID": "1dcb30fbe8b280998b86f769911db6b7",
    "COMPLETED_STATUSES": ["已完成"],
    "TOKEN_VARIABLE_NAME": "FZ_NOTION_TOKEN"
  },
  "wm": {
    "SPRINT_DB_ID": "277b30fbe8b2807ca21fd571c1d77c1c",
    "TASK_DB_ID": "277b30fbe8b2813397e5f6924c3d22c2",
    "PROJECT_DB_ID": "277b30fbe8b281bbaa67fdafc3f3cbd1",
    "COMPLETED_STATUSES": ["完成"],
    "TOKEN_VARIABLE_NAME": "FZ_NOTION_TOKEN"
  },
  "mkt": {
    "SPRINT_DB_ID": "1d5b30fbe8b28073bf95d5ce61dea11b",
    "TASK_DB_ID": "1d5b30fbe8b28002a3b5c8e745b4f0ae",
    "PROJECT_DB_ID": "1d5b30fbe8b280c98229cfd434ba3055",
    "COMPLETED_STATUSES": ["完成", "上線"],
    "TOKEN_VARIABLE_NAME": "FZ_NOTION_TOKEN"
  },
  "se": {
    "SPRINT_DB_ID": "d0d6b17ca30c4df48f36da22be06b2f6",
    "TASK_DB_ID": "0724a861156f414bbed695570f5ab941",
    "PROJECT_DB_ID": "01c92d1d6516410e9c6c562bfd839a72",
    "COMPLETED_STATUSES": ["Done", "Release", "Closed", "Waiting for release", "Testing"],
    "TOKEN_VARIABLE_NAME": "FM_NOTION_TOKEN"
  },
  "pd": {
    "SPRINT_DB_ID": "1afd63987af080069611ce70898315c1",
    "TASK_DB_ID": "e4eb5dd6fb4445258585779b5f2fe5df",
    "PROJECT_DB_ID": "01c92d1d6516410e9c6c562bfd839a72",
    "COMPLETED_STATUSES": ["Done"],
    "TOKEN_VARIABLE_NAME": "FM_NOTION_TOKEN"
  },
  "so": {
    "SPRINT_DB_ID": "1e7d63987af081f69d31cd374abc0cf6",
    "TASK_DB_ID": "1e7d63987af081939242cee8b6c71b0f",
    "PROJECT_DB_ID": "1e7d63987af081619a00c2a3ce66d55a",
    "COMPLETED_STATUSES": ["Done"],
    "TOKEN_VARIABLE_NAME": "FM_NOTION_TOKEN"
  },
  "dti": {
    "SPRINT_DB_ID": "1aed63987af080688838c88c138abe30",
    "TASK_DB_ID": "1add63987af0812ebc58fd7be4fbedeb",
    "PROJECT_DB_ID": "22ad63987af0802991c4f5e85b5e1f05",
    "COMPLETED_STATUSES": ["Done"],
    "TOKEN_VARIABLE_NAME": "FM_NOTION_TOKEN"
  }
}'
FZ_NOTION_TOKEN="your-notion-token-for-fz"
FM_NOTION_TOKEN="your-notion-token-for-fm"
NOTION_API_VERSION="2022-06-28"
```

- `NOTION_CONFIGS_JSON` 中的 key（例如 `ops`、`wm`、`mkt`）就是 HTTP 參數 `env` 的合法值；函式會依此決定要抓哪一組資料庫與 token。
- `TOKEN_VARIABLE_NAME` 指向實際儲存 Token 的環境變數名稱，需事先設定好（例如 `FZ_NOTION_TOKEN`）。
- 執行此 Function 的服務帳號需具備 BigQuery Data Editor 權限（或等效自訂角色）。
- 若請求未帶 `department`，會以 `N/A` 存入資料表。

## 本機測試
```bash
pip install -r requirement.txt
functions-framework --target notion_bq_sync_trigger --port 8080
```

```bash
curl "http://localhost:8080?env=ops&mode=current&department=Data%20Team"
```

## 部署與排程建議
- 建議部署為第二代 Cloud Functions（Python 3.11），入口點設為 `notion_bq_sync_trigger`。
- 使用 Cloud Scheduler 每日觸發 `mode=current`，保持當週 Sprint 指標最新。
- 需要補齊歷史資料時再手動呼叫 `mode=backfill`。

## 資料品質注意事項
- 多指派任務會被排除，請定期檢查 Notion 看板是否有異常分派。
- Sprint 名稱若無數字，回填排序會依 Notion 回傳順序，可能與期待不同。
- Notion API Token 過期或資料庫 ID 變動會導致函式回傳 500，需透過日誌追蹤。
- BigQuery 刪除條件包含 `Department`，排程時請統一大小寫與命名。

## 後續分析建議
- 在 All Tasks 中彙總故事點與狀態，觀察 Sprint 負載與進度。
- 在 Completed Tasks 依 Sprint、部門、指派人統計完成點數，評估達成率。
- 搭配 BigQuery View 或 Looker Studio 建立 Sprint 成效儀表板。
