# /// script
# [tool.marimo.display]
# theme = "dark"
# ///

import marimo

__generated_with = "0.22.5"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    import marimo as mo

    return mo, os


@app.cell
def _():
    from quixlake import QuixLakeClient

    return (QuixLakeClient,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Query QuixLake Data
    """)
    return


@app.cell
def _(QuixLakeClient, os):
    # TODO: Replace with your QuixLake URL
    QUIXLAKE_URL = "https://quixlake-quixdev-quixlakev2-dev.deployments-dev.quix.io"

    client = QuixLakeClient(
        base_url=QUIXLAKE_URL,
        token=os.environ["QUIX_LAKE_TOKEN"],timeout=300
    )
    return (client,)


@app.cell
def _(mo):
    load_btn = mo.ui.run_button(label="Load Data")
    mo.vstack([load_btn])
    return (load_btn,)


@app.cell
def _(client, load_btn, mo):
    mo.stop(not load_btn.value, mo.md("*Click **Load Data** to fetch results.*"))

    query = """
          SELECT
            wm.run_id,
            wm.wm_count          AS "watermarking count",
            nowm.nowm_count       AS "nowatermarking count",
            v4.v4_count           AS "v4 count",
            wm.wm_sum             AS "watermarking sum",
            nowm.nowm_sum         AS "nowatermarking sum",
            v4.v4_sum             AS "v4 sum",
            wm.wm_range           AS "watermarking",
            nowm.nowm_range       AS "nowatermarking",
            v4.v4_range           AS "v4"
          FROM (
            SELECT
              run_id,
              count(*)                     AS wm_count,
              sum(count)                   AS wm_sum,
              abs(max(count) - min(count)) AS wm_range
            FROM carcolours_vx2
            GROUP BY run_id
          ) AS wm
          JOIN (
            SELECT
              run_id,
              count(*)                     AS nowm_count,
              sum(count)                   AS nowm_sum,
              abs(max(count) - min(count)) AS nowm_range
            FROM carcolours_nowm2
            GROUP BY run_id
          ) AS nowm ON wm.run_id = nowm.run_id
          JOIN (
            SELECT
              run_id,
              count(*)                     AS v4_count,
              sum(count)                   AS v4_sum,
              abs(max(count) - min(count)) AS v4_range
            FROM carcolours_v4_2
            GROUP BY run_id
          ) AS v4 ON wm.run_id = v4.run_id
          ORDER BY wm.run_id DESC
          LIMIT 100
          """

    df = client.query(query)
    return (df,)


@app.cell
def _(df, mo):
    mo.vstack([
              mo.md(f"## Results ({len(df)} rows)"),
              mo.ui.table(df, selection=None, page_size=20),
        ])
    return


@app.cell
def _(mo):
    date_from = mo.ui.date(label="From date", value="2026-04-01")
    date_to = mo.ui.date(label="To date", value="2026-04-07")
    time_from = mo.ui.text(value="00:00:00", label="From time")
    time_to = mo.ui.text(value="23:59:59", label="To time")
    filter_btn = mo.ui.run_button(label="Filter by Time Window")

    mo.vstack([
      mo.md("## Filter by Time Window"),
      mo.hstack([date_from, time_from, date_to, time_to], justify="start", gap=1),
      filter_btn,
    ])
    return


@app.cell
def _(client):
    import pandas as pd

    q_wm = "SELECT run_id, count(*) as cnt, sum(count) as total FROM carcolours_vx2 GROUP BY run_id ORDER BY run_id DESC LIMIT 50"
    q_nowm = "SELECT run_id, count(*) as cnt, sum(count) as total FROM carcolours_nowm2 GROUP BY run_id ORDER BY run_id DESC LIMIT 50"
    q_v4 = "SELECT run_id, count(*) as cnt, sum(count) as total FROM carcolours_v4_2 GROUP BY run_id ORDER BY run_id DESC LIMIT 50"

    df_wm = client.query(q_wm)
    df_nowm = client.query(q_nowm)
    df_v4 = client.query(q_v4)

    df_wm = df_wm.rename(columns={"cnt": "wm_cnt", "total": "wm_total"})
    df_nowm = df_nowm.rename(columns={"cnt": "nowm_cnt", "total": "nowm_total"})
    df_v4 = df_v4.rename(columns={"cnt": "v4_cnt", "total": "v4_total"})

    merged = df_wm.merge(df_nowm, on="run_id", how="outer").merge(df_v4, on="run_id", how="outer")
    merged = merged.sort_values("run_id", ascending=False).reset_index(drop=True)

    import re
    from datetime import datetime

    def extract_ts(run_id):
      m = re.search(r'run_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', str(run_id))
      return m.group(1) if m else "NO MATCH"

    merged["parsed_ts"] = merged["run_id"].apply(extract_ts)

    merged


    return pd, re


@app.cell
def _(df, pd, re):
    sample = df["run_id"].head(5).tolist()

    results = []
    for rid in sample:
      m = re.search(r'run_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', str(rid))
      results.append({"run_id": rid, "match": m.group(1) if m else "NO MATCH"})

    pd.DataFrame(results)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
