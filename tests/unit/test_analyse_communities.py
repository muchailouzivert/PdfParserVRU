import pandas as pd
import networkx as nx
import pytest

communities = pytest.importorskip("analysis.analyse_communities")


def test_parse_top_vals_parses_name_count_pairs():
    value = "Економічна політика(12); Правова політика(7); Безпека і оборона(3)"

    result = communities._parse_top_vals(value)

    assert result == {
        "Економічна політика": 12,
        "Правова політика": 7,
        "Безпека і оборона": 3,
    }


def test_parse_top_vals_returns_empty_dict_for_invalid_input():
    assert communities._parse_top_vals("") == {}
    assert communities._parse_top_vals(None) == {}
    assert communities._parse_top_vals("без числа") == {}


def test_get_community_map_uses_existing_node_attribute():
    graph = nx.Graph()
    graph.add_node(1, community=0)
    graph.add_node(2, community=0)
    graph.add_node(3, community=1)

    result = communities.get_community_map(graph)

    assert result == {
        1: 0,
        2: 0,
        3: 1,
    }


def test_community_stats_calculates_basic_statistics():
    graph = nx.Graph()
    graph.add_edges_from([(1, 2), (2, 3), (4, 5)])

    comm_map = {
        1: 0,
        2: 0,
        3: 0,
        4: 1,
        5: 1,
    }

    meta = pd.DataFrame(
        {
            "node_key": [1, 2, 3, 4, 5],
            "rubric": [
                "Економічна політика",
                "Економічна політика",
                "Правова політика",
                "Безпека і оборона",
                "Безпека і оборона",
            ],
            "main_committee": [
                "Комітет фінансів",
                "Комітет фінансів",
                "Комітет правової політики",
                "Комітет оборони",
                "Комітет оборони",
            ],
            "outcome": [
                "in_progress",
                "accepted",
                "rejected",
                "in_progress",
                "in_progress",
            ],
            "reg_year": [2020, 2021, 2022, 2023, 2024],
        }
    )

    stats = communities.community_stats(graph, comm_map, meta)

    assert len(stats) == 2
    assert list(stats["size"]) == [3, 2]
    assert "top_rubrics" in stats.columns
    assert "top_committees" in stats.columns
    assert "outcome_dist" in stats.columns
    assert "year_range" in stats.columns

    first = stats.iloc[0]
    assert first["community"] == 0
    assert first["size"] == 3
    assert first["edges"] == 2
    assert first["year_range"] == "2020–2022"


def test_build_initiator_counts_splits_and_counts_initiators():
    meta = pd.DataFrame(
        {
            "node_key": [1, 2, 3],
            "initiators": [
                "Іваненко І. І.; Петренко П. П.",
                "Іваненко І. І., Сидоренко С. С.",
                None,
            ],
        }
    )

    comm_map = {
        1: 0,
        2: 0,
        3: 1,
    }

    result = communities.build_initiator_counts(meta, comm_map)

    assert not result.empty

    counts = dict(zip(result["initiator"], result["count"]))

    assert counts["Іваненко І. І."] == 2
    assert counts["Петренко П. П."] == 1
    assert counts["Сидоренко С. С."] == 1
    assert "rank" in result.columns


def test_build_initiator_counts_returns_empty_when_column_missing():
    meta = pd.DataFrame({"node_key": [1, 2, 3]})
    comm_map = {1: 0, 2: 0, 3: 1}

    result = communities.build_initiator_counts(meta, comm_map)

    assert result.empty