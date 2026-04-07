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


if __name__ == "__main__":
    app.run()
