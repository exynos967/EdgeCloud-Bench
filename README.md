# EdgeCloud-Bench

**边缘云协同调度能力评测基准** — 用于评测大模型的协议理解与在线调度策略设计能力。

## 题目

系统由 1 台本地计算机 E 和 K 台远端计算机 C0…C(K-1) 组成，持续接收推理请求。每个请求先经 **prefill**（P PRE→P PROC→P POST，一次），再逐 token 执行 **decode**（D PRE→D PROC→D POST，每 token 一轮）。传输自动排队（上行/下行两条 FIFO 共享链路），参赛者每收到一个事件帧就要决定各空闲计算机执行什么任务。题目全文见 [`docs/PROBLEM.md`](docs/PROBLEM.md)

## 打分方式

每题得分（与官方公式逐字段对齐）：

```
tp   = ΣL_out / 总耗时                      # 吞吐
tdr  = 各请求 " arrival→可开始decode " 均值
tpot = 相邻 token 间隔均值
dist = sqrt( max(0,(tdr-SLO1)/SLO1)² + max(0,(tpot-SLO2)/SLO2)² )
Score = 1000 × ( w_tp·clamp(tp; tp_base, tp_UB) + w_c·clamp(dist; dist_base, 0) )
```

赛制：

- **预赛 22 题**（`fixtures/public/` 1 题 + `fixtures/hidden/` 21 题）：逐题明细反馈
- **决赛 20 题**（`fixtures/final/`，冻结）：默认提交时自动附跑，只输出总分/均值，即最终成绩

**一次提交一次出分**：`python3 runner.py <解答文件>` 一份报告同时给出预赛明细与决赛聚合分。

## 考察模型能力的维度

| 维度 | 考察点 |
|---|---|
| 协议理解 | 9 种事件、6 种指令、状态机约束（分片连续、远端绑定、E 单任务、每帧 ≤K+1 任务） |
| 调度策略 | prefill/decode 权衡、攒批、分片、负载均衡 |
| 前瞻建模 | 传输 FIFO 完成时间估计、积压感知 |
| 工程性能 | 15s CPU / 256MB 下处理约 10⁶ 事件帧 |
| 鲁棒性 | FIN 与 TDN 同帧、空帧、END、表插值边界 |

## 用法

```bash
python3 runner.py <解答文件>          # 评测: 预赛 22 题明细 + 决赛 20 题聚合分
python3 runner.py <解答文件> --binary # 解答为已编译的可执行文件时, 跳过编译
python3 runner.py <解答文件> --subset public|hidden|practice|samples|stress
python3 runner.py <解答文件> --final  # 只跑冻结决赛（调试用；正式评测默认已含决赛，无需单独跑）
python3 make_handout.py <dir>         # 生成模型侧目录 (只有题面+样例)
```

解答文件语言不限：源文件按扩展名自动识别并编译/解释，也可直接提交编译好的可执行文件（`--binary`）。

测试集：`public/` 官方预赛 #1 样例 1 题 · `hidden/` 21 题 · `final/` 冻结决赛 20 题 · `practice/` 10 道额外练习题（不计入正式成绩）· `samples/` 官方样例参考 · `stress/` 超规格压力题（opt-in）

所有题集可由 `fixtures/gen_*.py` 重新生成（种子固定，逐字节可复现）。

## 评测纪律

本基准为自测自评。为保证分数可比：

- 被测模型只应接触 `make_handout.py` 生成的目录（题面 + 样例），**不应接触本仓库的题集与评分器**
- 每个模型正式评测一次，成绩以决赛 MEAN 为准
- 分享成绩请附上 `results/<name>.json`（单文件，含预赛明细与决赛聚合），任何人可用同一仓库复现审计

## 排行榜

排名依据为决赛平均分（FINAL MEAN），预赛均分仅作展示。上榜方式（SWE-bench 式自评 + 公开审计）：

1. 按上文评测纪律完成一次正式评测
2. 提 PR 修改 [`docs/leaderboard.json`](docs/leaderboard.json)，在 `entries` 中添加一条：
   ```json
   {"model": "模型名+思考强度", "harness": "模型使用的Harness",
    "final_mean": 554.54, "final_passed": "20/20",
    "prelim_mean": 549.20, "lang": "使用语言", "cpu_time": 123.4, "date": "2026-09-11"}
   ```
3. PR 中附 `results/<name>.json` 归档（`成绩摘要` 块即以上各字段，`cpu_time` 取其中 `决赛CPU时间(秒)`），供任何人复跑审计

## 给模型的测试提示词

```text
你在参加一个交互式调度编程评测。

题目：阅读当前目录下的 PROBLEM.md。
参考：samples/ 下两个 JSON 由官方题面样例整理为评测器输入格式（test_ex1 = 官方 Example 1
      完整参数；test_ex2 = 官方 Example 2 交互片段的重建，部分参数为补全值），
      可用于理解协议，但评测题与它们不同。

任务：独立编写程序 solution（语言不限），通过标准输入输出与交互器通信：
      循环读取事件帧 → 输出任务分配（首行任务数 n，随后 n 行指令）→ flush。
      程序限制 15 秒 CPU、256MB 内存。

规则：
- 只允许依据 PROBLEM.md 与 samples/ 解题，不得读取评测环境中的其他任何文件
- 提交前请自行充分调试；samples/ 的样例参数可供你自建模拟环境验证协议与策略，评测后不允许再更改和再次评测。
