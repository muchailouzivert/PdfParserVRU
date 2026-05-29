import json
import pandas as pd
import pytest

network_yake = pytest.importorskip("analysis.build_network_yake")


def make_test_df():
    return pd.DataFrame(
        {
            "doc_id": ["doc_1", "doc_2", "doc_3"],
            "keywords_yake": [
                json.dumps(["податок", "бюджет", "закон"], ensure_ascii=False),
                json.dumps(["податок", "бюджет", "фінанси"], ensure_ascii=False),
                json.dumps(["оборона", "безпека"], ensure_ascii=False),
            ],
            "rubric": ["Економічна політика", "Економічна політика", "Безпека і оборона"],
            "outcome": ["in_progress", "accepted", "rejected"],
            "num_stages": [2, 4, 3],
            "length": [1000, 1200, 900],
        }
    )


def test_build_inverted_index_groups_documents_by_keywords():
    df = make_test_df()

    index = network_yake.build_inverted_index(df)

    assert index["податок"] == {"doc_1", "doc_2"}
    assert index["бюджет"] == {"doc_1", "doc_2"}
    assert index["оборона"] == {"doc_3"}


def test_get_edges_creates_edges_when_min_shared_is_reached():
    df = make_test_df()
    index = network_yake.build_inverted_index(df)

    edges = network_yake.get_edges(index, min_shared=2)

    assert edges == {("doc_1", "doc_2"): 2}


def test_get_edges_returns_empty_when_threshold_is_too_high():
    df = make_test_df()
    index = network_yake.build_inverted_index(df)

    edges = network_yake.get_edges(index, min_shared=3)

    assert edges == {}


def test_build_graph_creates_nodes_and_weighted_edges():
    df = make_test_df()
    edges = {("doc_1", "doc_2"): 2}

    graph, node_idx = network_yake.build_graph(df, edges)

    assert graph.num_nodes() == 3
    assert graph.num_edges() == 1
    assert set(node_idx.keys()) == {"doc_1", "doc_2", "doc_3"}


def test_compute_metrics_for_small_graph():
    df = make_test_df()
    edges = {("doc_1", "doc_2"): 2}
    graph, _ = network_yake.build_graph(df, edges)

    metrics, components, degrees = network_yake.compute_metrics(graph)

    assert metrics["num_nodes"] == 3
    assert metrics["num_edges"] == 1
    assert metrics["num_components"] == 2
    assert metrics["largest_component_size"] == 2
    assert metrics["isolated_nodes"] == 1
    assert metrics["max_degree"] == 1
    assert metrics["min_degree"] == 0
    assert metrics["degree_distribution"] == {"0": 1, "1": 2}
    assert len(components) == 2
    assert sorted(degrees) == [0, 1, 1]