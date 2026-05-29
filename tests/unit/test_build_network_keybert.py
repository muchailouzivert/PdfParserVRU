import json
import pandas as pd
import pytest

network_keybert = pytest.importorskip("analysis.build_network_keyBERT")


def make_test_df():
    return pd.DataFrame(
        {
            "doc_id": ["doc_1", "doc_2", "doc_3", "doc_4"],
            "keywords_keybert": [
                json.dumps(["податок", "бюджет", "закон"], ensure_ascii=False),
                json.dumps(["податок", "бюджет", "фінанси"], ensure_ascii=False),
                json.dumps(["оборона", "безпека"], ensure_ascii=False),
                "",
            ],
            "rubric": [
                "Економічна політика",
                "Економічна політика",
                "Безпека і оборона",
                "Правова політика",
            ],
            "outcome": ["in_progress", "accepted", "rejected", "in_progress"],
            "num_stages": [2, 4, 3, 1],
            "length": [1000, 1200, 900, 700],
        }
    )


def test_parse_keywords_handles_json_string():
    value = json.dumps(["Податок", "Бюджет"], ensure_ascii=False)

    result = network_keybert.parse_keywords(value)

    assert result == ["Податок", "Бюджет"]


def test_parse_keywords_handles_empty_and_invalid_values():
    assert network_keybert.parse_keywords(None) == []
    assert network_keybert.parse_keywords("") == []
    assert network_keybert.parse_keywords("not-json") == []


def test_parse_keywords_handles_list_tuple_and_set():
    assert network_keybert.parse_keywords(["a", "b"]) == ["a", "b"]
    assert network_keybert.parse_keywords(("a", "b")) == ["a", "b"]
    assert sorted(network_keybert.parse_keywords({"a", "b"})) == ["a", "b"]


def test_build_inverted_index_ignores_empty_keyword_rows():
    df = make_test_df()

    index = network_keybert.build_inverted_index(df)

    assert index["податок"] == {"doc_1", "doc_2"}
    assert index["бюджет"] == {"doc_1", "doc_2"}
    assert index["оборона"] == {"doc_3"}


def test_get_edges_creates_weighted_edges():
    df = make_test_df()
    index = network_keybert.build_inverted_index(df)

    edges = network_keybert.get_edges(index, min_shared=2)

    assert edges == {("doc_1", "doc_2"): 2}


def test_build_graph_creates_all_nodes_and_edges():
    df = make_test_df()
    edges = {("doc_1", "doc_2"): 2}

    graph, node_idx = network_keybert.build_graph(df, edges)

    assert graph.num_nodes() == 4
    assert graph.num_edges() == 1
    assert set(node_idx.keys()) == {"doc_1", "doc_2", "doc_3", "doc_4"}


def test_compute_metrics_for_small_graph():
    df = make_test_df()
    edges = {("doc_1", "doc_2"): 2}
    graph, _ = network_keybert.build_graph(df, edges)

    metrics, components, degrees = network_keybert.compute_metrics(graph)

    assert metrics["num_nodes"] == 4
    assert metrics["num_edges"] == 1
    assert metrics["num_components"] == 3
    assert metrics["largest_component_size"] == 2
    assert metrics["isolated_nodes"] == 2
    assert metrics["max_degree"] == 1
    assert metrics["min_degree"] == 0
    assert metrics["degree_distribution"] == {"0": 2, "1": 2}
    assert sorted(degrees) == [0, 0, 1, 1]