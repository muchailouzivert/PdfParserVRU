from pathlib import Path
import pandas as pd
import json
import rustworkx as rx
import networkx as nx
from itertools import combinations
from collections import defaultdict
import pickle
import time
import community as community_louvain
import math
import random

BASE_DIR     = Path("D:/DIPLOM/PdfParserVRU/dataset_comparative_2020_2026")
input_path   = BASE_DIR / "comparative_files" / "yake_filter" / "keywords_yake_filter_2.parquet"
# input_path   = BASE_DIR / "comparative_files" / "keywords_yake_filtered.parquet"
graph_dir    = BASE_DIR / "comparative_files"

# Поріг мінімальної кількості спільних KW для створення ребра
MIN_SHARED = 2


def build_inverted_index(df: pd.DataFrame) -> dict:
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
    G = rx.PyGraph()
    doc_meta = df.drop_duplicates("doc_id").set_index("doc_id")[
        ["rubric", "outcome", "num_stages", "length"]
    ].to_dict("index")

    node_idx = {}
    for doc_id in df["doc_id"]:
        meta = doc_meta.get(doc_id, {})
        idx  = G.add_node({
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


def compute_metrics(G: rx.PyGraph) -> tuple:
    n         = G.num_nodes()
    e         = G.num_edges()
    max_edges = n * (n - 1) / 2

    degrees    = [G.degree(i) for i in range(n)]
    components = rx.connected_components(G)

    # Зв'язність 
    num_comp      = len(components)
    largest_comp  = max(len(c) for c in components)
    isolated      = sum(1 for c in components if len(c) == 1)
    is_connected  = num_comp == 1

    metrics = {
        # Базові
        "num_nodes":               n,
        "num_edges":               e,
        "density":                 round(e / max_edges if max_edges > 0 else 0, 6),
        "avg_degree":              round(sum(degrees) / len(degrees), 4),
        "max_degree":              max(degrees),
        "min_degree":              min(degrees),

        # Зв'язність
        "is_connected":            is_connected,
        "num_components":          num_comp,
        "largest_component_size":  largest_comp,
        "largest_component_pct":   round(largest_comp / n * 100, 2),
        "isolated_nodes":          isolated,

        # Розподіл ступенів — для переважного приєднання
        "degree_distribution": {
            str(d): degrees.count(d)
            for d in sorted(set(degrees))
        },
    }

    return metrics, components, degrees


def compute_nx_metrics(G: rx.PyGraph, components: list) -> tuple:
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

    # Кластеризація
    print("  Рахуємо clustering...")
    start = time.time()
    nx_metrics["clustering_coefficient"] = round(nx.average_clustering(H), 6)
    print(f"  clustering — {time.time()-start:.1f}с")

    print("  Рахуємо transitivity...")
    start = time.time()
    nx_metrics["transitivity"] = round(nx.transitivity(H), 6)
    print(f"  transitivity — {time.time()-start:.1f}с")

    # Тісний світ — середня довжина найкоротшого шляху 
    # Рахуємо на вибірці 500 вузлів
    print("  Рахуємо avg shortest path (вибірка 500 вузлів)...")
    start        = time.time()
    sample_nodes = random.sample(list(H.nodes()), min(500, H.number_of_nodes()))
    path_lengths = []
    for node in sample_nodes:
        lengths = nx.single_source_shortest_path_length(H, node)
        path_lengths.extend(lengths.values())
    avg_path = round(sum(path_lengths) / len(path_lengths), 4)
    nx_metrics["avg_shortest_path_length"] = avg_path
    print(f"  avg_shortest_path — {avg_path} — {time.time()-start:.1f}с")

    # Перевірка тісного світу
    # Випадковий граф Erdos-Renyi для порівняння
    n_nodes  = H.number_of_nodes()
    n_edges  = H.number_of_edges()
    p        = 2 * n_edges / (n_nodes * (n_nodes - 1))
    avg_path_random = math.log(n_nodes) / math.log(n_nodes * p) if n_nodes * p > 1 else None
    clust_random    = p
    nx_metrics["random_graph_avg_path"]    = round(avg_path_random, 4) if avg_path_random else None
    nx_metrics["random_graph_clustering"]  = round(clust_random, 6)
    nx_metrics["is_small_world"] = (
        avg_path <= avg_path_random * 1.5
        and nx_metrics["clustering_coefficient"] > clust_random * 2
        if avg_path_random else False
    )

    # Асортативність 
    print("  Рахуємо assortativity...")
    start = time.time()
    try:
        nx_metrics["degree_assortativity"] = round(
            nx.degree_assortativity_coefficient(H), 6
        )
    except Exception:
        nx_metrics["degree_assortativity"] = 0.0
    print(f"  assortativity — {time.time()-start:.1f}с")

    # Спільноти Louvain 
    print("  Louvain community detection...")
    start     = time.time()
    partition = community_louvain.best_partition(H)
    num_comm  = len(set(partition.values()))
    nx_metrics["num_communities_louvain"] = num_comm
    if H.number_of_edges() > 0:
        nx_metrics["modularity"] = round(
            community_louvain.modularity(partition, H), 6
        )
    else:
        nx_metrics["modularity"] = 0.0
    print(f"  Louvain — {time.time()-start:.1f}с, спільнот: {num_comm}")

    return nx_metrics, H, partition


def main():
    total_start = time.time()

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
    metrics, components, degrees = compute_metrics(G)
    nx_metrics, H, partition     = compute_nx_metrics(G, components)

    # Прибираємо degree_distribution з основних метрик — зберігаємо окремо
    degree_dist = metrics.pop("degree_distribution")

    all_metrics = {
        **metrics,
        **nx_metrics,
        "min_shared_keywords": MIN_SHARED,
    }

    print("\n Метрики ")
    print(f"\n  [Зв'язність]")
    print(f"  {'is_connected':<40}: {all_metrics['is_connected']}")
    print(f"  {'num_components':<40}: {all_metrics['num_components']}")
    print(f"  {'largest_component_size':<40}: {all_metrics['largest_component_size']} ({all_metrics['largest_component_pct']}%)")
    print(f"  {'isolated_nodes':<40}: {all_metrics['isolated_nodes']}")

    print(f"\n  [Тісний світ]")
    print(f"  {'clustering_coefficient':<40}: {all_metrics['clustering_coefficient']}")
    print(f"  {'transitivity':<40}: {all_metrics['transitivity']}")
    print(f"  {'avg_shortest_path_length':<40}: {all_metrics['avg_shortest_path_length']}")
    print(f"  {'random_graph_avg_path':<40}: {all_metrics['random_graph_avg_path']}")
    print(f"  {'random_graph_clustering':<40}: {all_metrics['random_graph_clustering']}")
    print(f"  {'is_small_world':<40}: {all_metrics['is_small_world']}")

    print(f"\n  [Переважне приєднання]")
    print(f"  {'avg_degree':<40}: {all_metrics['avg_degree']}")
    print(f"  {'max_degree':<40}: {all_metrics['max_degree']}")
    print(f"  {'min_degree':<40}: {all_metrics['min_degree']}")

    print(f"\n  [Асортативність]")
    print(f"  {'degree_assortativity':<40}: {all_metrics['degree_assortativity']}")

    print(f"\n  [Спільноти]")
    print(f"  {'num_communities_louvain':<40}: {all_metrics['num_communities_louvain']}")
    print(f"  {'modularity':<40}: {all_metrics['modularity']}")

    print(f"\n  [Загальні]")
    print(f"  {'num_nodes':<40}: {all_metrics['num_nodes']}")
    print(f"  {'num_edges':<40}: {all_metrics['num_edges']}")
    print(f"  {'density':<40}: {all_metrics['density']}")

    total_time = time.time() - total_start
    print(f"\n── Загальний час: {total_time:.1f}с ({total_time/60:.1f} хв) ──")

    graph_dir = BASE_DIR / "comparative_files" / "yake_metrics"
    graph_dir.mkdir(parents=True, exist_ok=True)

    graph_file   = graph_dir / f"graph_yake_min2_{MIN_SHARED}.pkl"
    nx_file      = graph_dir / f"graph_nx_yake_min2_{MIN_SHARED}.pkl"
    metrics_file = graph_dir / f"metrics_yake_min2_{MIN_SHARED}.json"
    degree_file  = graph_dir / f"degree_dist_yake_min2_{MIN_SHARED}.json"

    with open(graph_file, "wb") as f:
        pickle.dump(G, f)
    with open(nx_file, "wb") as f:
        pickle.dump(H, f)
    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(all_metrics, f, ensure_ascii=False, indent=2)
    with open(degree_file, "w", encoding="utf-8") as f:
        json.dump(degree_dist, f, ensure_ascii=False, indent=2)

    # Зберігаємо partition для візуалізації спільнот
    partition_file = graph_dir / f"partition_yake_min2_{MIN_SHARED}.json"
    with open(partition_file, "w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in partition.items()}, f, indent=2)

    print(f"\nГраф (rustworkx) → {graph_file}")
    print(f"Граф (networkx)  → {nx_file}")
    print(f"Метрики          → {metrics_file}")
    print(f"Розподіл ступенів → {degree_file}")
    print(f"Partition        → {partition_file}")


if __name__ == "__main__":
    main()