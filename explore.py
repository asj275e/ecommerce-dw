#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
建仓前的数据探查（pandas）
==========================

用途：在正式建仓之前，先用 pandas 快速摸清数据长什么样，
      据此确定「该定哪些清洗规则」和「怎么定判重口径」。

为什么这一步用 pandas 而不是标准库：
    · 探查是一次性的、需要灵活试，pandas 的 describe/value_counts
      比自己写循环快得多
    · 正式建仓链路要保持「零依赖、可复现」，所以仍用标准库 + SQLite
    —— 两者分工明确，不是重复劳动

用法：
    python explore.py                              # 探查内置样例数据
    python explore.py --csv UserBehavior.csv       # 探查真实数据
    python explore.py --csv UserBehavior.csv --nrows 2000000   # 只抽样看

输出：
    终端打印探查结果，并在 reports/ 下生成 探查报告.md
"""

import argparse
import os
import sys

import pandas as pd

COLS = ["user_id", "item_id", "category_id", "behavior", "ts"]
VALID_BEHAVIOR = {"pv", "buy", "cart", "fav"}
DATE_MIN, DATE_MAX = "2017-11-25", "2017-12-03"

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(HERE, "reports")


def title(s):
    print("\n" + "=" * 74)
    print(f"  {s}")
    print("=" * 74)


def main():
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser(description="建仓前的 pandas 数据探查")
    ap.add_argument("--csv", default="sample_user_behavior.csv",
                    help="要探查的 CSV（默认用内置样例数据）")
    ap.add_argument("--nrows", type=int, default=None,
                    help="只读前 N 行（大文件抽样用）")
    args = ap.parse_args()

    path = args.csv
    if not os.path.isabs(path):
        path = os.path.join(HERE, path)
    if not os.path.exists(path):
        print(f"[错误] 找不到文件：{path}")
        print("       先运行 python dw_pipeline.py 生成样例数据。")
        return 1

    # ------------------------------------------------------------------
    title("1. 读取数据")
    # ------------------------------------------------------------------
    kw = dict(header=None, names=COLS, dtype=str, keep_default_na=False,
              on_bad_lines="skip")
    if args.nrows:
        kw["nrows"] = args.nrows
    df = pd.read_csv(path, **kw)
    print(f"  文件      : {os.path.basename(path)}")
    print(f"  读取行数  : {len(df):,}" + (f"（抽样前 {args.nrows:,} 行）" if args.nrows else ""))
    print(f"  列        : {list(df.columns)}")

    # ------------------------------------------------------------------
    title("2. 字段概况与空值检查")
    # ------------------------------------------------------------------
    # user_id / item_id / category_id 应是数字，ts 是时间戳 —— 先做类型探测
    null_cnt = (df == "").sum()
    print(f"  {'字段':<14}{'空值数':>10}{'空值率':>10}")
    print("  " + "-" * 34)
    for c in COLS:
        n = int(null_cnt[c])
        print(f"  {c:<14}{n:>10,}{n / len(df):>10.2%}")

    # 强制转数字，转不了的记成 NaN —— 这能发现"字段里混了非数字"
    numeric = {}
    for c in ["user_id", "item_id", "category_id", "ts"]:
        numeric[c] = pd.to_numeric(df[c], errors="coerce")
    bad_type = {c: int(numeric[c].isna().sum() - null_cnt[c]) for c in numeric}
    print("\n  非数字值（无法转成数字的脏数据）：")
    for c, n in bad_type.items():
        flag = "  <-- 需要清洗" if n > 0 else ""
        print(f"    {c:<14}{n:>10,}{flag}")

    # ------------------------------------------------------------------
    title("3. behavior 值域分布")
    # ------------------------------------------------------------------
    vc = df["behavior"].value_counts()
    print(f"  {'取值':<22}{'次数':>12}{'占比':>10}")
    print("  " + "-" * 44)
    for k, v in vc.items():
        mark = "" if k in VALID_BEHAVIOR else "   <-- 非法值，需清洗"
        print(f"  {k:<22}{v:>12,}{v / len(df):>10.2%}{mark}")
    illegal = int((~df["behavior"].isin(VALID_BEHAVIOR)).sum())
    print(f"\n  非法 behavior 合计：{illegal:,} 行")

    # ------------------------------------------------------------------
    title("4. 时间范围检查")
    # ------------------------------------------------------------------
    ts = numeric["ts"]
    dt = pd.to_datetime(ts, unit="s", errors="coerce", utc=True).dt.tz_convert("Asia/Shanghai")
    day = dt.dt.strftime("%Y-%m-%d")
    print(f"  时间范围  : {day.min()} ~ {day.max()}")
    print(f"  应为范围  : {DATE_MIN} ~ {DATE_MAX}")
    out_of_range = int(((day < DATE_MIN) | (day > DATE_MAX)).sum())
    print(f"  越界行数  : {out_of_range:,}  <-- 需按日期范围过滤")

    print(f"\n  每日行数分布：")
    dc = day.value_counts().sort_index()
    for k, v in dc.items():
        inrange = "" if DATE_MIN <= str(k) <= DATE_MAX else "   <-- 越界"
        print(f"    {k}  {v:>12,}{inrange}")

    # ------------------------------------------------------------------
    title("5. 重复上报检查（判重口径验证）")
    # ------------------------------------------------------------------
    # 这是关键一步：验证"用 user_id+item_id+ts+behavior 四个字段判重"是否成立
    dup_key = ["user_id", "item_id", "ts", "behavior"]
    dup_mask = df.duplicated(subset=dup_key, keep=False)
    dup_rows = int(dup_mask.sum())
    dup_groups = int(df[dup_mask].groupby(dup_key, dropna=False).ngroups) if dup_rows else 0
    print(f"  判重字段        : {dup_key}")
    print(f"  重复的行数      : {dup_rows:,}")
    print(f"  涉及重复组数    : {dup_groups:,}")
    if dup_rows:
        print(f"  去重后剩余行数  : {len(df) - int(df.duplicated(subset=dup_key).sum()):,}")
        print("\n  重复样例（前 3 组）：")
        g = df[dup_mask].groupby(dup_key, dropna=False).size().sort_values(ascending=False).head(3)
        for idx, cnt in g.items():
            print(f"    user={idx[0]} item={idx[1]} ts={idx[2]} behavior={idx[3]}  → {cnt} 条")
        print("\n  说明：四个字段完全相同 = 同一秒对同一商品记录同一行为，")
        print("        现实中不可能重复发生，因此判定为重复上报。")
    else:
        print("  未发现完全重复的记录。")

    # ------------------------------------------------------------------
    title("6. 数据分布特征（用于判断是否倾斜）")
    # ------------------------------------------------------------------
    # 商品热度分布：验证是否存在热点商品（会导致 Shuffle 倾斜）
    item_cnt = df["item_id"].value_counts()
    total = item_cnt.sum()
    top1pct = int(len(item_cnt) * 0.01) or 1
    share = item_cnt.head(top1pct).sum() / total
    print(f"  不同商品数         : {len(item_cnt):,}")
    print(f"  Top 1% 商品占比    : {share:.2%}")
    print(f"  最大单商品记录数   : {int(item_cnt.iloc[0]):,}")
    print(f"  中位数商品记录数   : {int(item_cnt.median()):,}")
    if share > 0.5:
        print(f"\n  => 明显存在热点商品（Top 1% 占了 {share:.1%} 的记录），")
        print(f"     按 item_id 分组时会产生数据倾斜，需要留意。")

    # 用户活跃度分布
    user_cnt = df["user_id"].value_counts()
    print(f"\n  不同用户数         : {len(user_cnt):,}")
    print(f"  人均记录数         : {len(df) / len(user_cnt):.1f}")

    # ------------------------------------------------------------------
    title("7. 探查结论 → 清洗规则")
    # ------------------------------------------------------------------
    rules = []
    if int(null_cnt[["user_id", "item_id"]].sum()) > 0 or sum(bad_type.values()) > 0:
        rules.append("主键字段（user_id / item_id）为空或非数字 → 丢弃")
    if illegal > 0:
        rules.append(f"behavior 非法值（不在 pv/buy/cart/fav）→ 丢弃，共 {illegal:,} 行")
    if out_of_range > 0:
        rules.append(f"时间戳越界（不在 {DATE_MIN} ~ {DATE_MAX}）→ 丢弃，共 {out_of_range:,} 行")
    if dup_rows > 0:
        rules.append(f"重复上报（user_id+item_id+ts+behavior 全同）→ 去重保留一条")

    if rules:
        for i, r in enumerate(rules, 1):
            print(f"  {i}. {r}")
    else:
        print("  数据较干净，未发现需要清洗的问题。")

    # ------------------------------------------------------------------
    title("8. 写入报告")
    # ------------------------------------------------------------------
    os.makedirs(REPORT_DIR, exist_ok=True)
    rpt = os.path.join(REPORT_DIR, "探查报告.md")
    lines = [
        "# 数据探查报告（pandas）",
        "",
        f"- 数据文件：`{os.path.basename(path)}`",
        f"- 读取行数：{len(df):,}" + (f"（抽样前 {args.nrows:,} 行）" if args.nrows else ""),
        f"- 时间范围：{day.min()} ~ {day.max()}",
        "",
        "## 空值与非数字值",
        "",
        "| 字段 | 空值数 | 空值率 | 非数字值 |",
        "|---|---|---|---|",
    ]
    for c in COLS:
        n = int(null_cnt[c])
        bt = bad_type.get(c, 0)
        lines.append(f"| {c} | {n:,} | {n/len(df):.2%} | {bt:,} |")

    lines += ["", "## behavior 值域分布", "", "| 取值 | 次数 | 占比 | 是否合法 |", "|---|---|---|---|"]
    for k, v in vc.items():
        lines.append(f"| {k} | {v:,} | {v/len(df):.2%} | {'是' if k in VALID_BEHAVIOR else '**否**'} |")

    lines += [
        "", "## 重复上报", "",
        f"- 判重字段：`user_id + item_id + ts + behavior`",
        f"- 重复行数：**{dup_rows:,}**",
        f"- 涉及组数：{dup_groups:,}",
        "",
        "判定依据：四个字段完全相同意味着「同一用户在同一秒对同一商品记录了同一行为」，",
        "现实中不可能发生，因此判定为重复上报（常见于客户端超时重试、消息队列至少一次投递）。",
        "",
        "## 数据分布特征", "",
        f"- 不同商品数：{len(item_cnt):,}",
        f"- Top 1% 商品占全部记录的 **{share:.2%}**",
        f"- 不同用户数：{len(user_cnt):,}，人均记录数 {len(df)/len(user_cnt):.1f}",
        "",
    ]
    if share > 0.5:
        lines.append(f"> ⚠️ 存在明显热点：Top 1% 商品占 {share:.1%} 记录，"
                     f"按 `item_id` 分组时会产生**数据倾斜**。\n")

    lines += ["## 探查结论 → 清洗规则", ""]
    if rules:
        for i, r in enumerate(rules, 1):
            lines.append(f"{i}. {r}")
    else:
        lines.append("数据较干净，未发现需要清洗的问题。")

    with open(rpt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  已生成：{os.path.relpath(rpt, HERE)}")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
