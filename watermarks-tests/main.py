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

      min(watermarking.count) as "watermaking-min", 
      mean(watermarking.count) as "watermaking-mean", 
      max(watermarking.count) as "watermaking-max",

      min(nowatermarking.count) as "nowatermarking-min", 
      mean(nowatermarking.count) as "nowatermarking-mean", 
      max(nowatermarking.count) as "nowatermarking-max",
  
    FROM carcoloursv2 as watermarking
    JOIN carcoloursnomwv2 as nowatermarking ON watermarking.run_id == nowatermarking.run_id
    GROUP BY watermarking.run_id
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
    # Create watermarking chart with updated colors
    _zoom1 = alt.selection_interval(bind='scales', name='zoom_selection1')

    _watermarking_chart = alt.Chart(df).mark_area(
        opacity=0.3,
        color='lightblue'
    ).encode(
        x=alt.X('run_id:N', title='Run ID', axis=alt.Axis(labelAngle=-45, labelLimit=200)),
        y=alt.Y('watermaking-min:Q', title='Count'),
        y2=alt.Y2('watermaking-max:Q'),
        tooltip=['run_id:N', 'watermaking-min:Q', 'watermaking-max:Q', 'watermaking-mean:Q']
    ).add_params(
        _zoom1
    ).properties(
        title='Watermarking Count Ranges by Run ID',
        width=800,
        height=350
    )

    # Add mean line with high contrast
    _watermarking_mean_line = alt.Chart(df).mark_line(
        color='navy',
        strokeWidth=3,
        strokeDash=[5, 5]
    ).encode(
        x='run_id:N',
        y='watermaking-mean:Q',
        tooltip=['run_id:N', 'watermaking-mean:Q']
    ).add_params(
        _zoom1
    )

    watermarking_chart = (_watermarking_chart + _watermarking_mean_line)
    watermarking_chart
    return


@app.cell
def _(alt, df):
    # Create nowatermarking chart with updated colors
    _zoom2 = alt.selection_interval(bind='scales', name='zoom_selection2')

    _nowatermarking_chart = alt.Chart(df).mark_area(
        opacity=0.3,
        color='wheat'
    ).encode(
        x=alt.X('run_id:N', title='Run ID', axis=alt.Axis(labelAngle=-45, labelLimit=200)),
        y=alt.Y('nowatermarking-min:Q', title='Count'),
        y2=alt.Y2('nowatermarking-max:Q'),
        tooltip=['run_id:N', 'nowatermarking-min:Q', 'nowatermarking-max:Q', 'nowatermarking-mean:Q']
    ).add_params(
        _zoom2
    ).properties(
        title='No-Watermarking Count Ranges by Run ID',
        width=800,
        height=350
    )

    # Add mean line with high contrast
    _nowatermarking_mean_line = alt.Chart(df).mark_line(
        color='darkred',
        strokeWidth=3,
        strokeDash=[5, 5]
    ).encode(
        x='run_id:N',
        y='nowatermarking-mean:Q',
        tooltip=['run_id:N', 'nowatermarking-mean:Q']
    ).add_params(
        _zoom2
    )

    nowatermarking_chart = (_nowatermarking_chart + _nowatermarking_mean_line)
    nowatermarking_chart
    return


if __name__ == "__main__":
    app.run()
