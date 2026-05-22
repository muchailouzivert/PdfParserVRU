from pathlib import Path
import pandas as pd
import json
import rustworkx as rx
import networkx as nx
from itertools import combinations
from collections import defaultdict
import pickle
import time
import math
import random
import community as community_louvain

BASE_DIR = Path("D:/DIPLOM/PdfParserVRU/dataset_comparative_2020_2026")

KEYWORD_COL = "keywords_keybert"
MIN_SHARED = 2
BATCH_SIZE = 500

# MODE:
# "original"  — звичайний KeyBERT
# "filtered"  — KeyBERT після фільтрації частих KW

MODE = "original"
# MODE = "filtered"

if MODE == "original":
    INPUT_DIR = BASE_DIR / "comparative_files" / "keyBert"
    OUTPUT_DIR = BASE_DIR / "comparative_files" / "keyBert_metrics"

    INPUT_FILES = [
        INPUT_DIR / "keywords_keybert_2.parquet",
        INPUT_DIR / "keywords_keybert_4.parquet",
        INPUT_DIR / "keywords_keybert_6.parquet",
        INPUT_DIR / "keywords_keybert_8.parquet",
        INPUT_DIR / "keywords_keybert_10.parquet",
    ]

elif MODE == "filtered":
    INPUT_DIR = BASE_DIR / "comparative_files" / "keywords_keybert_filtered"
    OUTPUT_DIR = BASE_DIR / "comparative_files" / "keyBert_Filtred_metrics"

    INPUT_FILES = [
        INPUT_DIR / "keywords_keybert_filtered_2pct.parquet",
        INPUT_DIR / "keywords_keybert_filtered_4pct.parquet",
        INPUT_DIR / "keywords_keybert_filtered_6pct.parquet",
        INPUT_DIR / "keywords_keybert_filtered_8pct.parquet",
        INPUT_DIR / "keywords_keybert_filtered_10pct.parquet",
    ]

else:
    raise ValueError("MODE має бути 'original' або 'filtered'")


def parse_keywords(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple, set)):
        return list(value)
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except:
            pass
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return []
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except:
            return []
    return []


def build_inverted_index(df: pd.DataFrame) -> dict:
    index      = defaultdict(set)
    total      = len(df)
    start      = time.time()
    empty_rows = 0

    for i, (_, row) in enumerate(df.iterrows()):
        try:
            kw_list = parse_keywords(row[KEYWORD_COL])
            if not kw_list:
                empty_rows += 1
            for kw in kw_list:
                kw_norm = str(kw).lower().strip()
                if kw_norm:
                    index[kw_norm].add(row["doc_id"])
        except:
            empty_rows += 1

        if (i + 1) % BATCH_SIZE == 0:
            elapsed   = time.time() - start
            per_doc   = elapsed / (i + 1)
            remaining = per_doc * (total - i - 1)
            print(f"  Індекс: {i+1:,}/{total:,} — {elapsed:.0f}с, ~{remaining/60:.1f}хв залишилось")

    print(f"  Індекс: {total:,}/{total:,} — готово за {time.time()-start:.1f}с")
    print(f"  Порожніх рядків KW: {empty_rows:,}")
    print(f"  Унікальних KW в індексі: {len(index):,}")
    return index


def get_edges(index: dict, min_shared: int) -> dict:
    pair_shared = defaultdict(int)
    total_kw    = len(index)
    start       = time.time()

    for i, (kw, doc_ids) in enumerate(index.items()):
        if len(doc_ids) < 2:
            continue
        for a, b in combinations(sorted(doc_ids), 2):
            pair_shared[(a, b)] += 1

        if (i + 1) % 1000 == 0:
            elapsed   = time.time() - start
            per_kw    = elapsed / (i + 1)
            remaining = per_kw * (total_kw - i - 1)
            print(f"  Ребра: {i+1:,}/{total_kw:,} KW — {elapsed:.0f}с, ~{remaining/60:.1f}хв залишилось")

    edges = {pair: cnt for pair, cnt in pair_shared.items() if cnt >= min_shared}
    print(f"  Ребра: готово за {time.time()-start:.1f}с")
    return edges


def build_graph(df: pd.DataFrame, edges: dict) -> tuple:
    G        = rx.PyGraph()
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

    total = len(edges)
    start = time.time()
    done  = 0

    for (a, b), weight in edges.items():
        if a in node_idx and b in node_idx:
            G.add_edge(node_idx[a], node_idx[b], weight)
        done += 1
        if done % 500000 == 0:
            elapsed   = time.time() - start
            per_edge  = elapsed / done
            remaining = per_edge * (total - done)
            print(f"  Граф: {done:,}/{total:,} ребер — {elapsed:.0f}с, ~{remaining/60:.1f}хв залишилось")

    print(f"  Граф: готово за {time.time()-start:.1f}с")
    return G, node_idx


def compute_metrics(G: rx.PyGraph) -> tuple:
    n         = G.num_nodes()
    e         = G.num_edges()
    max_edges = n * (n - 1) / 2
    degrees   = [G.degree(i) for i in range(n)]
    components = rx.connected_components(G)

    num_comp     = len(components)
    largest_comp = max(len(c) for c in components) if components else 0
    isolated     = sum(1 for c in components if len(c) == 1)

    metrics = {
        "num_nodes":              n,
        "num_edges":              e,
        "density":                round(e / max_edges if max_edges > 0 else 0, 6),
        "avg_degree":             round(sum(degrees) / len(degrees), 4) if degrees else 0,
        "max_degree":             max(degrees) if degrees else 0,
        "min_degree":             min(degrees) if degrees else 0,
        # Зв'язність
        "is_connected":           num_comp == 1,
        "num_components":         num_comp,
        "largest_component_size": largest_comp,
        "largest_component_pct":  round(largest_comp / n * 100, 2) if n > 0 else 0,
        "isolated_nodes":         isolated,
        # Розподіл ступенів
        "degree_distribution":    {str(d): degrees.count(d) for d in sorted(set(degrees))},
    }

    return metrics, components, degrees


def compute_nx_metrics(G: rx.PyGraph, components: list) -> tuple:
    print("  Конвертуємо у networkx...")
    start         = time.time()
    largest_nodes = set(max(components, key=len))

    H = nx.Graph()
    for n in largest_nodes:
        H.add_node(n)
    for u, v in G.edge_list():
        if u in largest_nodes and v in largest_nodes:
            H.add_edge(u, v)

    print(f"  NetworkX підграф: {H.number_of_nodes():,} вершин, {H.number_of_edges():,} ребер — {time.time()-start:.1f}с")

    nx_metrics = {}

    # Кластеризація
    print("  Рахуємо clustering...")
    start = time.time()
    nx_metrics["clustering_coefficient"] = round(nx.average_clustering(H), 6) if H.number_of_nodes() > 0 else 0.0
    print(f"  clustering — {time.time()-start:.1f}с")

    print("  Рахуємо transitivity...")
    start = time.time()
    nx_metrics["transitivity"] = round(nx.transitivity(H), 6) if H.number_of_edges() > 0 else 0.0
    print(f"  transitivity — {time.time()-start:.1f}с")

    # Тісний світ — avg shortest path на вибірці
    print("  Рахуємо avg shortest path (вибірка 500 вузлів)...")
    start        = time.time()
    sample_nodes = random.sample(list(H.nodes()), min(500, H.number_of_nodes()))
    path_lengths = []
    for node in sample_nodes:
        lengths = nx.single_source_shortest_path_length(H, node)
        path_lengths.extend(lengths.values())
    avg_path = round(sum(path_lengths) / len(path_lengths), 4) if path_lengths else 0
    nx_metrics["avg_shortest_path_length"] = avg_path
    print(f"  avg_shortest_path — {avg_path} — {time.time()-start:.1f}с")

    # Порівняння з випадковим графом
    n_nodes = H.number_of_nodes()
    n_edges = H.number_of_edges()
    p       = 2 * n_edges / (n_nodes * (n_nodes - 1)) if n_nodes > 1 else 0
    avg_path_random = round(math.log(n_nodes) / math.log(n_nodes * p), 4) if n_nodes * p > 1 else None
    nx_metrics["random_graph_avg_path"]   = avg_path_random
    nx_metrics["random_graph_clustering"] = round(p, 6)
    nx_metrics["is_small_world"]          = (
        avg_path <= avg_path_random * 1.5
        and nx_metrics["clustering_coefficient"] > p * 2
        if avg_path_random else False
    )

    # Асортативність
    print("  Рахуємо assortativity...")
    start = time.time()
    try:
        nx_metrics["degree_assortativity"] = round(nx.degree_assortativity_coefficient(H), 6) if H.number_of_edges() > 0 else 0.0
    except:
        nx_metrics["degree_assortativity"] = 0.0
    print(f"  assortativity — {time.time()-start:.1f}с")

    # Louvain
    print("  Louvain community detection...")
    start = time.time()
    if H.number_of_edges() > 0:
        partition = community_louvain.best_partition(H)
        num_comm  = len(set(partition.values()))
        nx_metrics["num_communities_louvain"] = num_comm
        nx_metrics["modularity"]              = round(community_louvain.modularity(partition, H), 6)
    else:
        partition = {}
        nx_metrics["num_communities_louvain"] = 0
        nx_metrics["modularity"]              = 0.0
    print(f"  Louvain — {time.time()-start:.1f}с, спільнот: {nx_metrics['num_communities_louvain']}")

    return nx_metrics, H, partition


def process_dataset(input_path: Path):
    total_start = time.time()

    filter_name = input_path.stem
    output_prefix = f"{filter_name}_{MIN_SHARED}"
    
    print("\n" + "=" * 80)
    print(f"START: {input_path.name}")
    print("=" * 80)

    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)

    print(f"  Файл: {input_path}")
    print(f"  Документів: {len(df):,}")
    print(f"  Колонка KW: {KEYWORD_COL}")

    if KEYWORD_COL not in df.columns:
        raise ValueError(f"У файлі немає колонки: {KEYWORD_COL}")

    print("\nБудуємо інвертований індекс...")
    index = build_inverted_index(df)

    print(f"\nРахуємо ребра (MIN_SHARED={MIN_SHARED})...")
    edges = get_edges(index, MIN_SHARED)
    print(f"  Ребер: {len(edges):,}")

    print("\nБудуємо граф (rustworkx)...")
    G, node_idx = build_graph(df, edges)
    print(f"  Вершин: {G.num_nodes():,}  |  Ребер: {G.num_edges():,}")

    print("\nРахуємо метрики...")
    metrics, components, degrees = compute_metrics(G)
    degree_dist = metrics.pop("degree_distribution")
    nx_metrics, H, partition = compute_nx_metrics(G, components)

    all_metrics = {
        **metrics,
        **nx_metrics,
        "min_shared_keywords": MIN_SHARED,
        "input_file": str(input_path),
        "filter_name": filter_name,
        "keyword_column": KEYWORD_COL,
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

    total_time = time.time() - total_start
    all_metrics["total_time_seconds"] = round(total_time, 2)

    print(f"\n── Загальний час: {total_time:.1f}с ({total_time / 60:.1f} хв) ──")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    graph_file = OUTPUT_DIR / f"{output_prefix}_rustworkx.pkl"
    nx_file = OUTPUT_DIR / f"{output_prefix}_networkx.pkl"
    metrics_file = OUTPUT_DIR / f"{output_prefix}_metrics.json"
    degree_file = OUTPUT_DIR / f"{output_prefix}_degree_distribution.json"
    partition_file = OUTPUT_DIR / f"{output_prefix}_partition.json"

    with open(graph_file, "wb") as f:
        pickle.dump(G, f)

    with open(nx_file, "wb") as f:
        pickle.dump(H, f)

    with open(metrics_file, "w", encoding="utf-8") as f:
        json.dump(all_metrics, f, ensure_ascii=False, indent=2)

    with open(degree_file, "w", encoding="utf-8") as f:
        json.dump(degree_dist, f, ensure_ascii=False, indent=2)

    with open(partition_file, "w", encoding="utf-8") as f:
        json.dump({str(k): v for k, v in partition.items()}, f, ensure_ascii=False, indent=2)

    print(f"\nГраф (rustworkx)     → {graph_file}")
    print(f"Граф (networkx)      → {nx_file}")
    print(f"Метрики              → {metrics_file}")
    print(f"Розподіл ступенів    → {degree_file}")
    print(f"Partition            → {partition_file}")

    return all_metrics


def main():
    all_results = []

    for input_path in INPUT_FILES:
        if not input_path.exists():
            print("\n" + "!" * 80)
            print(f"Файл не знайдено: {input_path}")
            print("Пропускаємо...")
            print("!" * 80)
            continue

        try:
            result = process_dataset(input_path)
            all_results.append(result)

        except Exception as e:
            print("\n" + "!" * 80)
            print(f"Помилка при обробці файлу: {input_path.name}")
            print(e)
            print("!" * 80)

    if all_results:
        summary_df = pd.DataFrame(all_results)

        summary_file_csv = OUTPUT_DIR / f"keybert_all_metrics_min_shared_{MIN_SHARED}.csv"
        summary_file_json = OUTPUT_DIR / f"keybert_all_metrics_min_shared_{MIN_SHARED}.json"

        summary_df.to_csv(summary_file_csv, index=False, encoding="utf-8-sig")

        with open(summary_file_json, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)

        print("\n" + "=" * 80)
        print("УСІ ФАЙЛИ ОБРОБЛЕНО")
        print("=" * 80)
        print(f"Зведена таблиця CSV  → {summary_file_csv}")
        print(f"Зведений JSON        → {summary_file_json}")

        print("\nКороткий підсумок:")
        print(
            summary_df[
                [
                    "filter_name",
                    "num_nodes",
                    "num_edges",
                    "largest_component_pct",
                    "isolated_nodes",
                    "clustering_coefficient",
                    "avg_degree",
                    "modularity",
                    "is_small_world",
                ]
            ]
        )
    else:
        print("Жоден файл не був оброблений.")

if __name__ == "__main__":
    main()