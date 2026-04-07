# /// script
# [tool.marimo.display]
# theme = "dark"
# ///

import marimo

__generated_with = "0.22.4"
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
        token=os.environ["QUIX_LAKE_TOKEN"]
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
            FROM carcolours_vx1
            GROUP BY run_id
          ) AS wm
          JOIN (
            SELECT
              run_id,
              count(*)                     AS nowm_count,
              sum(count)                   AS nowm_sum,
              abs(max(count) - min(count)) AS nowm_range
            FROM carcolours_nowm1
            GROUP BY run_id
          ) AS nowm ON wm.run_id = nowm.run_id
          JOIN (
            SELECT
              run_id,
              count(*)                     AS v4_count,
              sum(count)                   AS v4_sum,
              abs(max(count) - min(count)) AS v4_range
            FROM carcolours_v4
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


app._unparsable_cell(
    r"""
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
    """,
    name="_"
)


app._unparsable_cell(
    r"""
    import re
    from datetime import datetime

    mo.stop(not filter_btn.value, mo.md("*Click **Filter by Time Window** to filter results.*"))

    dt_from = datetime.strptime(f"{date_from.value} {time_from.value}", "%Y-%m-%d %H:%M:%S")
    dt_to = datetime.strptime(f"{date_to.value} {time_to.value}", "%Y-%m-%d %H:%M:%S")

    def extract_ts(run_id):
      m = re.search(r'run_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', str(run_id))
      if m:
          return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
      return None

    filtered = df.copy()
    filtered["_ts"] = filtered["run_id"].apply(extract_ts)
    filtered = filtered[filtered["_ts"].notna()]
    filtered = filtered[(filtered["_ts"] >= dt_from) & (filtered["_ts"] <= dt_to)]
    filtered = filtered.drop(columns=["_ts"])

    return (filtered,)
    """,
    name="_"
)


@app.cell
def _(filtered, mo):
    if len(filtered) == 0:
      mo.output.replace(mo.md("*No runs found in this time window.*"))
    else:
      summary = {
          "metric": ["watermarking", "nowatermarking", "v4"],
          "total sum": [
              filtered["watermarking sum"].sum(),
              filtered["nowatermarking sum"].sum(),
              filtered["v4 sum"].sum(),
          ],
          "total count (rows)": [
              filtered["watermarking count"].sum(),
              filtered["nowatermarking count"].sum(),
              filtered["v4 count"].sum(),
          ],
          "runs": [len(filtered)] * 3,
      }
      import pandas as pd
      summary_df = pd.DataFrame(summary)

      mo.vstack([
          mo.md(f"## Time Window Summary ({len(filtered)} runs)"),
          mo.ui.table(summary_df, selection=None),
          mo.md("### Filtered Runs"),
          mo.ui.table(filtered, selection=None, page_size=20),
      ])
    return


if __name__ == "__main__":
    app.run()
