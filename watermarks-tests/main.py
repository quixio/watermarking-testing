# /// script
# [tool.marimo.display]
# theme = "dark"
# ///

import marimo

__generated_with = "0.20.4"
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
      watermarking.run_id,
      count(watermarking.count) as "watermarking count",
      count(nowatermarking.count) as "nowatermarking count",
      abs(max(watermarking.count)-min(watermarking.count)) as "watermaking", 
      abs(max(nowatermarking.count)-min(nowatermarking.count)) as "nowatermarking", 

    FROM carcoloursnomwv2 as nowatermarking
    LEFT OUTER JOIN carcoloursv2 as watermarking ON watermarking.run_id == nowatermarking.run_id
    GROUP BY watermarking.run_id
    ORDER BY run_id DESC
    LIMIT 10
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
    # Create base chart
    _base_chart = alt.Chart(df).add_selection(
        alt.selection_interval()
    ).properties(
        width=700,
        height=400,
        title='Watermarking Metrics and Count by Run ID'
    )

    # Left axis - watermarking metrics starting from 0
    _left_axis_chart = _base_chart.transform_fold(
        ['watermaking', 'nowatermarking'],
        as_=['metric_type', 'difference_value']
    ).mark_line(
        point=True,
        strokeWidth=3
    ).encode(
        x=alt.X('run_id:O', 
                title='Run ID',
                axis=alt.Axis(labelAngle=45)),
        y=alt.Y('difference_value:Q', 
                title='Count Difference (Left Axis)',
                scale=alt.Scale(domain=[0, df[['watermaking', 'nowatermarking']].max().max() * 1.1], zero=True)),
        color=alt.Color('metric_type:N', 
                       title='Metric Type',
                       scale=alt.Scale(
                           domain=['watermaking', 'nowatermarking'],
                           range=['#1f77b4', '#ff7f0e']
                       )),
        tooltip=['run_id:O', 'metric_type:N', 'difference_value:Q']
    )

    # Right axis - watermarking count and nowatermarking count
    _right_axis_chart = _base_chart.transform_fold(
        ['watermarking count', 'nowatermarking count'],
        as_=['count_type', 'count_value']
    ).mark_line(
        point=True,
        strokeWidth=3,
        strokeDash=[5, 5]
    ).encode(
        x=alt.X('run_id:O'),
        y=alt.Y('count_value:Q', 
                title='Count (Right Axis)',
                scale=alt.Scale(
                    domain=[
                        df[['watermarking count', 'nowatermarking count']].min().min() * 0.95,
                        df[['watermarking count', 'nowatermarking count']].max().max() * 1.05
                    ]
                )),
        color=alt.Color('count_type:N', 
                       title='Metric Type',
                       scale=alt.Scale(
                           domain=['watermarking count', 'nowatermarking count'],
                           range=['#d62728', '#2ca02c']
                       )),
        tooltip=['run_id:O', 'count_type:N', 'count_value:Q']
    )

    # Layer the charts with independent y-scales
    chart = alt.layer(
        _left_axis_chart,
        _right_axis_chart
    ).resolve_scale(
        y='independent'
    ).interactive()

    chart
    return


if __name__ == "__main__":
    app.run()
