import numpy as np
import plotly.graph_objs as go
import networkx as nx
import plotly.express as px
import pandas as pd
from sqlalchemy import Engine


def build_edge_list(schema: str, industry: str, start_date: str, end_date: str, countries: list[str] | None, engine: Engine
                    ) -> pd.DataFrame:
    conditions: list[str] = ["ec.source_country <> ec.target_country"]
    params: dict = {}

    if schema is not None and schema != "":
        conditions.append('ec.schema = %(s)s')
        params["s"] = schema

    if industry is not None and industry != "":
        conditions.append('ec.industry = %(i)s')
        params["i"] = industry

    if start_date is not None and start_date != "":
        conditions.append('ec.first_seen >= %(sd)s')
        params["sd"] = start_date

    if end_date is not None and end_date != "":
        conditions.append('ec.first_seen <= %(ed)s')
        params["ed"] = end_date

    if countries is not None and len(countries) > 0:
        conditions.append('ec.source_country = ANY(%(countries)s) AND ec.target_country = ANY(%(countries)s)')
        params["countries"] = countries

    condition: str = ' AND '.join(conditions)

    sql: str = f"""SELECT
        COALESCE(NULLIF(s.name, ''), NULLIF(s.description, ''), ec.source_country) AS source,
        COALESCE(NULLIF(t.name, ''), NULLIF(t.description, ''), ec.target_country) AS target,
        count(DISTINCT ec.id) AS weight
    FROM entities_countries ec
    LEFT JOIN countries s ON s.alpha_2 = ec.source_country
    LEFT JOIN countries t ON t.alpha_2 = ec.target_country
    WHERE {condition}
    GROUP BY 1, 2"""

    return pd.read_sql(sql, params=params, con=engine)


def build_graph(df) -> nx.DiGraph:
    graph: nx.DiGraph = nx.DiGraph()
    graph: nx.DiGraph = nx.from_pandas_edgelist(df, source="source", target="target", edge_attr=["weight"],
                                                create_using=graph)
    return graph


def build_output(schema: str, industry: str, start_date: str, end_date: str, countries: list[str] | None, engine: Engine
                 ) -> (go.Figure, pd.DataFrame):
    df = build_edge_list(schema, industry, start_date, end_date, countries, engine)
    graph = build_graph(df)

    if len(df) == 0 or graph.number_of_nodes() == 0:
        fig = px.scatter(title='No Data')
        fig.update_layout(annotations=[
            dict(x=0.5, y=0.5, xref="paper", yref="paper", text="No data", showarrow=False, font=dict(size=20), )])
        return fig, []

    return plot_network(graph), get_centralises(graph).to_dict("records")


def plot_network(graph: nx.Graph) -> go.Figure:
    pos: dict = nx.spring_layout(graph, weight='weight')

    weights: list[float] = [d["weight"] for (_, _, d) in graph.edges(data=True)]
    min_weights: float = min(weights)
    max_weights: float = max(weights)

    nodes_x: list[float] = []
    nodes_y: list[float] = []
    nodes_color: list[float] = []
    nodes_text: list[str] = []
    annotations: list[go.Scatter] = []

    for node, adj in graph.adjacency():
        nodes_x.append(pos[node][0])
        nodes_y.append(pos[node][1])

        total_weights: float = 0
        for edge in adj:
            weight: float = adj[edge]["weight"]
            total_weights += weight

            try:
                weight_norm: float = (weight - min_weights) / (max_weights - min_weights)
            except ZeroDivisionError as _:
                weight_norm: float = 0

            annotations.append(go.Scatter(x=[pos[node][0], pos[edge][0]], y=[pos[node][1], pos[edge][1]],
                                          hoverinfo='skip',
                                          marker={"size": 5+(10*weight_norm),
                                                  "symbol": "arrow-bar-up", "angleref": "previous",
                                                  "color": "rgba(0,0,0,.5)"}))

        nodes_color.append(total_weights)
        nodes_text.append(f"{node} is sanctioning {total_weights} entities ({len(adj)} Countries)")

    node_trace = go.Scatter(x=nodes_x, y=nodes_y, text=nodes_text, mode='markers', hoverinfo='text', opacity=0.5,
                            marker={
                                "showscale": True, "colorscale": 'RdBu', "reversescale": True, "color": nodes_color,
                                "size": 15, "colorbar": {"thickness": 10, "title": 'Number of sanctioned entities',
                                                         "xanchor": 'left', "titleside": 'right'}, "line": {"width": 0}
                            })

    ticks: dict = {"showgrid": True, "zeroline": True, "showticklabels": False}

    fig: go.Figure = go.Figure(data=annotations + [node_trace], layout=go.Layout(
                         titlefont={"size": 16}, showlegend=False, hovermode='closest',
                         margin={"b": 20, "l": 5, "r": 5, "t": 40}, xaxis=ticks, yaxis=ticks))
    return fig


def get_centralises(graph: nx.Graph) -> pd.DataFrame:
    metrics: dict = {
        "Degree": nx.degree_centrality(graph),
        "In-Degree": nx.in_degree_centrality(graph),
        "Out-Degree": nx.out_degree_centrality(graph),
        "Closeness": nx.closeness_centrality(graph),
        "Betweenness": nx.betweenness_centrality(graph, weight="weight"),
        "Clustering": nx.clustering(graph, weight="weight"),
        "Pagerank": nx.pagerank(graph, weight="weight")
    }

    metric_names: list[str] = list(metrics.keys())
    data: list[list[float]] = []

    for country in sorted(graph.nodes, reverse=True):
        row = [country]

        for metric in metric_names:
            if country in metrics[metric]:
                row.append(np.array(metrics[metric][country]).round(2))
            else:
                row.append(None)

        data.append(row)

    return pd.DataFrame(data, columns=["Country"] + metric_names)
