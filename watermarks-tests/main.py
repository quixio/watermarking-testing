# /// script
# [tool.marimo.display]
# theme = "dark"
# ///

import marimo

__generated_with = "0.21.0"
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
    QUIXLAKE_URL = "https://quixlake-quixers-testrigdemodatawarehouse-prod.az-france-0.app.quix.io"

    client = QuixLakeClient(
        base_url=QUIXLAKE_URL,
        token=os.environ["QUIX_LAKE_TOKEN"]
    )
    return (client,)


@app.cell
def _(mo):
    # TODO: Modify the SQL query for your data
    default_query = """
    SELECT
      wm.colour,
      wm.run_id,
      avg(wm.processed_ts - wm.end) / 1000.0   AS wm_avg_lag_s,
      avg(nm.processed_ts - nm.end)  / 1000.0  AS nowm_avg_lag_s,
      max(wm.processed_ts - wm.end)  / 1000.0  AS wm_max_lag_s,
      max(nm.processed_ts - nm.end)  / 1000.0  AS nowm_max_lag_s,
      count(wm.count)                           AS wm_windows,
      count(nm.count)                           AS nowm_windows
    FROM carcolours_daniel AS wm
    LEFT OUTER JOIN carcoloursnomwv2_daniel AS nm
      ON wm.run_id = nm.run_id
      AND wm.colour = nm.colour
      AND wm.start = nm.start
    WHERE wm.run_id > 'run_2026-03-17 12:00:00'
      AND wm.run_id < 'run_2026-03-17 12:04:00'
    GROUP BY wm.colour, wm.run_id
    ORDER BY wm.run_id DESC, wm.colour
    LIMIT 100
    """.strip()

    sql_form = mo.ui.code_editor(
        value=default_query,
        language="sql",
        label="SQL query",
        min_height=150,
    )

    sql_form
    return (sql_form,)


@app.cell
def _(client, sql_form):
    df = client.query(sql_form.value)
    df
    return (df,)


@app.cell
def _():
    import altair as alt

    return (alt,)


@app.cell
def _(alt, df):
    # Fold wm_avg_lag_s and nowm_avg_lag_s into long form for grouped bars
    _chart = alt.Chart(df).transform_fold(
        ['wm_avg_lag_s', 'nowm_avg_lag_s'],
        as_=['pipeline', 'avg_lag_s']
    ).mark_bar().encode(
        x=alt.X('colour:N', title='Colour', axis=alt.Axis(labelAngle=45)),
        y=alt.Y('avg_lag_s:Q', title='Avg window closure lag (s)'),
        color=alt.Color('pipeline:N',
                        title='Pipeline',
                        scale=alt.Scale(
                            domain=['wm_avg_lag_s', 'nowm_avg_lag_s'],
                            range=['#1f77b4', '#ff7f0e']
                        )),
        xOffset='pipeline:N',
        column=alt.Column('run_id:N', title='Run ID'),
        tooltip=['colour:N', 'pipeline:N', 'avg_lag_s:Q', 'wm_windows:Q', 'nowm_windows:Q']
    ).properties(
        width=400,
        height=300,
        title='Window closure lag by colour: watermarking (blue) vs no-watermarking (orange)'
    ).interactive()

    _chart
    return


if __name__ == "__main__":
    app.run()
