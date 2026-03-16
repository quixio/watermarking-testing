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
    SELECT run_id, min(count), mean(count), max(count)
    FROM carcoloursv2
    WHERE run_id = 'run_2026-03-16 12:15:03.055043+00:00'
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
    zoom = alt.selection_interval(bind='scales', name='zoom_selection')

    base = alt.Chart(df).add_params(zoom)

    # Background range area
    range_area = base.mark_area(
        opacity=0.3,
        color='lightblue'
    ).encode(
        x=alt.X('run_id:T', title='Run ID'),
        y=alt.Y('min(count):Q', title='Count'),
        y2=alt.Y2('max(count):Q'),
        tooltip=['run_id:T', 'min(count):Q', 'max(count):Q']
    )

    # Mean line
    mean_line = base.mark_line(
        color='red',
        strokeWidth=3
    ).encode(
        x='run_id:T',
        y='mean(count):Q',
        tooltip=['run_id:T', 'mean(count):Q']
    )

    # Mean points for better visibility
    mean_points = base.mark_circle(
        color='red',
        size=100
    ).encode(
        x='run_id:T',
        y='mean(count):Q',
        tooltip=['run_id:T', 'mean(count):Q', 'min(count):Q', 'max(count):Q']
    )

    # Combine layers
    chart = (range_area + mean_line + mean_points).properties(
        title='Count Statistics by Run ID',
        width=600,
        height=400
    )

    chart
    return


if __name__ == "__main__":
    app.run()
