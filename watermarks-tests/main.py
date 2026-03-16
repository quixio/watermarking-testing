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
       run_id, 
       min(count) as "min", 
       mean(count) as "mean", 
       max(count) as "max"
    FROM carcoloursv2
    GROUP BY run_id
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
    # Create a layered chart with range and mean line
    _zoom2 = alt.selection_interval(bind='scales', name='zoom_selection2')

    _base2 = alt.Chart(df)

    # Background range area
    _range_area2 = _base2.mark_area(
        opacity=0.3,
        color='lightblue'
    ).encode(
        x=alt.X('run_id:N', title='Run ID', axis=alt.Axis(labelAngle=-45, labelLimit=200)),
        y=alt.Y('min:Q', title='Count'),
        y2=alt.Y2('max:Q'),
        tooltip=['run_id:N', 'min:Q', 'max:Q']
    )

    # Mean line
    _mean_line2 = _base2.mark_line(
        color='red',
        strokeWidth=3
    ).encode(
        x='run_id:N',
        y='mean:Q',
        tooltip=['run_id:N', 'mean:Q']
    )

    # Mean points for better visibility
    _mean_points2 = _base2.mark_circle(
        color='red',
        size=100
    ).encode(
        x='run_id:N',
        y='mean:Q',
        tooltip=['run_id:N', 'mean:Q', 'min:Q', 'max:Q']
    )

    # Combine layers and add the selection parameter once on the combined chart
    chart2 = (_range_area2 + _mean_line2 + _mean_points2).add_params(
        _zoom2
    ).properties(
        title='Count Statistics by Run ID',
        width=600,
        height=400
    )

    chart2
    return


if __name__ == "__main__":
    app.run()
