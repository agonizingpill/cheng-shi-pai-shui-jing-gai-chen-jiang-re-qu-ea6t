# 城市排水井盖沉降热区

`manhole-settlement` 面向市政排水巡测团队，把井盖资产、同批次测量点、空间热区、道路施工约束和维修复测串成一条可追溯的处置链。

## 真实场景

模拟客户为 **东澜市排水设施维护中心**，团队规模约 18 人，负责南河路、西环路等老城区排水井盖的日常巡检与维修派单。

现有工作流由测量员用 Leica MS60 导出 CSV，班长在 GIS 图层中手工圈热区，再把片区截图发到施工群。主要痛点是坐标基准混用会让距离失真，施工围挡与维修片区重叠时容易把井盖派到错误的道路窗口，维修完成后也缺少同点复测的闭环证据。

上游输入是井盖资产台账和 `RUN-1121-AM` 测量记录。本系统替代“测量点核验 → 空间聚类 → 道路边界复核 → 施工联动 → 复测销项”这一段。下游是市政施工队、道路封闭协调员和设施维护主管。

## 主要功能

- 井盖资产与测量批次登记，记录坐标基准、仪器、沉降值和测量质量状态。
- 对同一坐标基准的测量点执行 DBSCAN，使用局部密度评分排序维修片区。
- 发现施工边界冲突时阻止直接施工，主管按道路边界拆分片区并保留原始聚类证据。
- 施工联动单提交、主管确认、维修启动、复测提交和主管核验销项。
- 相邻批次热区沿革：按井盖串接原始聚类，分裂/合并保留各支去向，漏测井盖标为待确认。
- 迟到巡测批次单独成批出片后按巡测日期插回，旧关联标待更新，前后两段算齐后同事务替换。
- 沿革任务断点续算，重试不新增同一段沿革；热区复核页可按井盖查历次归属。
- JWT 三角色权限、审计日志、请求限流、Redis 异步任务和 worker 心跳。

## 一天的工作剧本

1. 07:30，测量员导入 `SURV-2026-1121-AM`，仪器为 `Leica MS60 / RUN-1121-AM`。
2. 07:42，系统发现 `SP-1121-20` 使用 `CGCS2000`，而批次基准为 `WGS84-METER`，聚类请求被阻断。
3. 07:48，操作员将该点修正到批次基准，批次回到 `ready`。
4. 07:50，worker 消费 DBSCAN 任务，形成南河路和西环路两个 10 点热区，并写入局部密度评分。
5. 07:53，西环路热区与 `WN-XH-20261121` 的施工围挡重叠，主管按 `x=1126.5` 拆成两个道路边界片区。
6. 08:05，操作员为南河路热区提交 `SG-2026-NH-071` 施工联动单，主管确认后进入维修。
7. 16:20，施工队提交同片区复测，沉降从约 11.95 mm 降到 6.4 mm。
8. 16:30，主管核验通过，片区与施工联动单同时关闭。

## 隔周沿革剧本

1. 种子数据中 `SURV-2026-1107-AM` 已预出片，是沿革链起点。
2. 1121 批次出片后，沿革任务自动把 1107→1121 串为两条 `continue` 关联。
3. 迟到的 `SURV-2026-1114-PM` 单独出片后按巡测日期插回：1107→1121 旧关联先标 `待更新`，1107→1114 与 1114→1121 两段算齐后整段替换。
4. 1114 批次漏测的 `MH-016`~`MH-020` 在归属表中标为 `漏测待确认`。
5. 打开「热区复核」，片区卡片显示来源/去向各支；右侧「按井盖查沿革」输入 `MH-016` 可见 1107 入区 → 1114 漏测待确认 → 1121 回区。

## 快速启动

```bash
cp .env.example .env
docker compose --env-file .env up -d --build
# 首次启动会自动执行 seed
# 也可手动幂等执行
docker compose exec backend python -m scripts.seed_demo
```

访问前端地址为 <http://127.0.0.1:18269>，API 健康检查为 <http://127.0.0.1:19269/health>。

## 演示账号

| 账号 | 密码 | 角色 | 权限 |
|---|---|---|---|
| `operator` | `operator123` | operator | 录入、修正基准、发起施工、提交复测 |
| `supervisor` | `supervisor123` | supervisor | 主管确认、道路拆分、核验销项 |
| `viewer` | `viewer123` | viewer | 查看测绘、热区、施工和复测结果 |

## 算法口径

worker 使用固定参数 `DBSCAN(eps=26m, min_samples=3)` 对同一测量批次的二维投影坐标聚类。每个非噪声簇写入 `RepairZone`。局部密度评分为 `簇内点数 / 外接矩形面积 × 10,000`。坐标基准集合不为单一值时，后端以 `409 DATUM_CONFLICT` 拒绝聚类。算法输入、参数、结果摘要、失败状态和完成时间均写入 `AnalysisJob`。

## 沿革口径

批次出片（聚类成功）后自动触发 `LineageJob`，按 `measured_at` 把批次排成时间链，仅串接原始聚类（道路拆分片不参与）。相邻两批之间，共享井盖的热区连成关联边：一出度大于一为 `split`，一入度大于一为 `merge`，一对一为 `continue`；无来源为 `emerge`，无去向为 `dissolve`，各支去向都保留在 `ZoneLineageLink`。上一批热区成员在本批漏测的井盖写入 `ManholeZoneMembership`，标记 `pending_confirmation`（待确认）。

迟到批次单独成批出片后，按巡测日期找到插入点，受影响旧关联先标 `pending_update`，插入点前后两段都计算完成（逐段提交断点）后，在同一个事务里删除旧关联、暂存链接转正、归属重写。任务中断后由 worker 启动恢复或 `POST /api/lineage/rebuild/{batch_id}` 触发续算；分段采用先清暂存再写入，重试不会新增同一段沿革。

## 技术栈与目录

| 层 | 技术 |
|---|---|
| frontend | Nginx、原生 HTML/CSS/ES modules |
| backend | Python 3.11、FastAPI、SQLAlchemy |
| worker | Redis list、scikit-learn DBSCAN、numpy |
| db | PostgreSQL 16 |
| auth | JWT HS256、PBKDF2-SHA256 |

共享枚举在 `backend/src/constants/enums.py` 与前端状态渲染共同出现。角色枚举在 `backend/src/security.py`、`backend/src/main.py` 和 `frontend/src/app.js` 中使用。片区、施工单、批次状态在 `backend/src/constants/enums.py`、`backend/src/models.py`、`backend/src/main.py` 和前端各页面中出现。

```text
backend/src/models.py                 领域表与关联
backend/src/services/cluster.py       DBSCAN 与局部密度评分
backend/src/services/lineage.py       相邻批次沿革、断点续算与整段替换
backend/src/main.py                   认证、领域路由、状态规则
backend/scripts/seed_demo.py           幂等演示数据
backend/tests/test_lineage.py          沿革引擎单元测试
backend/tests/test_lineage_api.py      沿革接口测试
frontend/src/app.js                   四个业务页面与交互
frontend/src/api/client.js             API 封装
frontend/src/styles.css                市政测绘图板视觉系统
test-artifacts/gb-1070/                验收脚本、截图与追踪文档
```

## Docker 说明

Compose 恰好运行 `frontend`、`backend`、`worker`、`db`、`redis` 五个服务。数据库、Redis 和共享 worker 产物使用命名卷。`.env` 只保留本地端口、凭据和连接字符串，不提交到 Git。

## License

Internal evaluation artifact.


## 业务核验模块

登录后打开页面右下角的「业务核验」。可从实时业务台账筛选记录、导出所选结果，主管可对当前证据签认并填写说明。

- **道路施工许可对照** 比较同道路、非撤销的封道许可时段，列出每项通知许可缺口和并发通行冲突，供排班避让。
- **维修片区复测变化** 对照片区沉降前后值并计算恢复比例，直观核对复测是否反弹。

### 二次扩展核验

- **测量坐标系入簇预检** 逐点比对来源坐标系与测量批声明，并列明质检或未入簇原因，避免异坐标点进入维修片区。

## 专项业务核对

登录后打开「业务核验」，可以筛选和导出记录，主管可签认指定证据。

- **道路封闭解除核对** 交叉核对封道许可、维修区状态、最后一次复测及许可截止。只有已完工并有主管验收的片区进入解封确认。
