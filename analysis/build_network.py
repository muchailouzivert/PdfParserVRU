from pathlib import Path
import pandas as pd
import json
import rustworkx as rx
import networkx as nx
from itertools import combinations
from collections import defaultdict
import pickle
import community as community_louvain

BASE_DIR    = Path("../dataset_comparative_2020_2026")
input_path   = BASE_DIR / "comparative_files" / "keywords_yake.parquet"
graph_dir    = BASE_DIR / "comparative_files"

# Поріг мінімальної кількості спільних ключових слів для створення ребра між документами
# MIN_SHARED=1 — максимальна щільність (2.8M ребер), слабка модульність (7 спільнот)
# MIN_SHARED=2 — баланс між щільністю і структурою (883k ребер)
# MIN_SHARED=3 — розріджена мережа, лише найсильніші тематичні зв'язки

MIN_SHARED = 3


def build_inverted_index(df: pd.DataFrame) -> dict:
    """keyword → set of doc_ids"""
    index = defaultdict(set)
    for _, row in df.iterrows():
        try:
            kw_list = json.loads(row["keywords_yake"])
            for kw in kw_list:
                index[kw.lower().strip()].add(row["doc_id"])
        except:
            pass
    return index


def get_edges(index: dict, min_shared: int) -> dict:
    """Підраховуємо спільні KW між парами документів"""
    pair_shared = defaultdict(int)
    for kw, doc_ids in index.items():
        if len(doc_ids) < 2:
            continue
        for a, b in combinations(sorted(doc_ids), 2):
            pair_shared[(a, b)] += 1

    return {
        pair: cnt
        for pair, cnt in pair_shared.items()
        if cnt >= min_shared
    }


def build_graph(df: pd.DataFrame, edges: dict) -> tuple:
    """Будуємо граф у rustworkx"""
    G = rx.PyGraph()

    doc_meta = df.drop_duplicates("doc_id").set_index("doc_id")[["rubric", "outcome", "num_stages", "length"]].to_dict("index")

    node_idx = {}
    for doc_id in df["doc_id"]:
        meta = doc_meta.get(doc_id, {})
        idx = G.add_node({
            "doc_id":     doc_id,
            "rubric":     meta.get("rubric", ""),
            "outcome":    meta.get("outcome", ""),
            "num_stages": meta.get("num_stages", 0),
            "length":     meta.get("length", 0),
        })
        node_idx[doc_id] = idx

    for (a, b), weight in edges.items():
        if a in node_idx and b in node_idx:
            G.add_edge(node_idx[a], node_idx[b], weight)

    return G, node_idx


def compute_metrics(G: rx.PyGraph) -> dict:
    """Базові метрики через rustworkx"""
    n = G.num_nodes()
    e = G.num_edges()
    max_edges = n * (n - 1) / 2

    metrics = {
        "num_nodes": n,
        "num_edges": e,
        "density":   round(e / max_edges if max_edges > 0 else 0, 6),
    }

    degrees = [G.degree(i) for i in range(G.num_nodes())]
    metrics["avg_degree"] = round(sum(degrees) / len(degrees), 4)
    metrics["max_degree"] = max(degrees)
    metrics["min_degree"] = min(degrees)

    components = rx.connected_components(G)
    metrics["num_components"]           = len(components)
    metrics["largest_component_size"]   = max(len(c) for c in components)
    metrics["isolated_nodes"]           = sum(1 for c in components if len(c) == 1)

    return metrics, components


def compute_nx_metrics(G: rx.PyGraph, components: list) -> dict:
    """Метрики через networkx — clustering, assortativity, communities"""
    print("  Конвертуємо у networkx...")
    largest_nodes = set(max(components, key=len))

    H = nx.Graph()
    for n in largest_nodes:
        H.add_node(n)
    for u, v in G.edge_list():
        if u in largest_nodes and v in largest_nodes:
            H.add_edge(u, v)

    print(f"  NetworkX підграф: {H.number_of_nodes():,} вершин, {H.number_of_edges():,} ребер")

    nx_metrics = {}
    nx_metrics["clustering_coefficient"] = round(nx.average_clustering(H), 6)
    nx_metrics["transitivity"]           = round(nx.transitivity(H), 6)
    nx_metrics["degree_assortativity"]   = round(nx.degree_assortativity_coefficient(H), 6)

    partition  = community_louvain.best_partition(H)
    num_comm   = len(set(partition.values()))
    modularity = community_louvain.modularity(partition, H)
    nx_metrics["num_communities_louvain"] = num_comm
    nx_metrics["modularity"]              = round(modularity, 6)
    print(f"  Спільнот (Louvain): {num_comm}")

    return nx_metrics, H


def main():
    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)
    print(f"  Документів: {len(df):,}")

    print("\nБудуємо інвертований індекс...")
    index = build_inverted_index(df)
    print(f"  Унікальних KW: {len(index):,}")

    print(f"\nРахуємо ребра (MIN_SHARED={MIN_SHARED})...")
    edges = get_edges(index, MIN_SHARED)
    print(f"  Ребер: {len(edges):,}")

    print("\nБудуємо граф (rustworkx)...")
    G, node_idx = build_graph(df, edges)
    print(f"  Вершин: {G.num_nodes():,}  |  Ребер: {G.num_edges():,}")

    print("\nРахуємо метрики...")
    metrics, components = compute_metrics(G)
    nx_metrics, H = compute_nx_metrics(G, components)
    all_metrics = {**metrics, **nx_metrics, "min_shared_keywords": MIN_SHARED}

    print("\nМетрики ")
    for k, v in all_metrics.items():
        print(f"  {k:40s}: {v}")

    graph_dir.mkdir(parents=True, exist_ok=True)

    graph_file   = graph_dir / f"graph_min{MIN_SHARED}.pkl"
    nx_file      = graph_dir / f"graph_nx_min{MIN_SHARED}.pkl"
    metrics_file = graph_dir / f"metrics_min{MIN_SHARED}.json"

    with open(graph_file, "wb") as f:
        pickle.dump(G, f)

    with open(nx_file, "wb") as f:
        pickle.dump(H, f)

    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(all_metrics, f, ensure_ascii=False, indent=2)

    print(f"\nГраф (rustworkx) → {graph_file}")
    print(f"Граф (networkx)  → {nx_file}")
    print(f"Метрики          → {metrics_file}")


if __name__ == "__main__":
    main()