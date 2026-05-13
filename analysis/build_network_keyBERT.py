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


BASE_DIR = Path("D:/DIPLOM/PdfParserVRU/dataset_comparative_2020_2026")

# Беремо фінально очищені keywords
input_path = BASE_DIR / "comparative_files" / "keywords_keybert_final_clean.parquet"
graph_dir = BASE_DIR / "comparative_files"

KEYWORD_COL = "keywords_keybert_final_clean"

# MIN_SHARED=1 — ребро між документами, якщо є хоча б 1 спільне KW
# MIN_SHARED=2 — сильніші зв'язки
# MIN_SHARED=3 — ще більш розріджена мережа
MIN_SHARED = 1
BATCH_SIZE = 500


def parse_keywords(value):
    """
    Підтримує різні формати збереження keywords:
    - Python list
    - tuple / set
    - numpy.ndarray / pandas array з parquet
    - JSON-рядок з CSV
    - порожні значення
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, tuple) or isinstance(value, set):
        return list(value)

    # Parquet часто повертає list-подібні значення як numpy.ndarray
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except Exception:
            pass

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return []

        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
            return []
        except Exception:
            return []

    return []


def build_inverted_index(df: pd.DataFrame) -> dict:
    """
    Будує інвертований індекс:
    keyword -> set(doc_id)

    Тобто для кожного ключового слова зберігаємо документи,
    у яких воно зустрічається.
    """
    index = defaultdict(set)
    total = len(df)
    start = time.time()

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

        except Exception:
            empty_rows += 1

        if (i + 1) % BATCH_SIZE == 0:
            elapsed = time.time() - start
            per_doc = elapsed / (i + 1)
            remaining = per_doc * (total - i - 1)

            print(
                f"  Індекс: {i + 1:,}/{total:,} — "
                f"{elapsed:.0f}с, ~{remaining / 60:.1f}хв залишилось"
            )

    print(f"  Індекс: {total:,}/{total:,} — готово за {time.time() - start:.1f}с")
    print(f"  Порожніх рядків KW: {empty_rows:,}")
    print(f"  Унікальних KW в індексі: {len(index):,}")

    return index


def print_top_keywords(index: dict, top_n: int = 20):
    """
    Показує топ ключових слів за кількістю документів.
    Це потрібно для перевірки, що індекс реально заповнився.
    """
    print("\nТоп KW за кількістю документів:")

    top_kw = sorted(
        [(kw, len(doc_ids)) for kw, doc_ids in index.items()],
        key=lambda x: x[1],
        reverse=True
    )[:top_n]

    if not top_kw:
        print("  Немає KW в індексі")
        return

    for kw, cnt in top_kw:
        print(f"  {kw:<50} {cnt}")


def get_edges(index: dict, min_shared: int) -> dict:
    """
    Підраховує кількість спільних ключових слів між парами документів.

    Якщо два документи мають спільне ключове слово,
    між ними створюється ребро.

    Вага ребра = кількість спільних ключових слів.
    """
    pair_shared = defaultdict(int)
    total_kw = len(index)
    start = time.time()

    for i, (kw, doc_ids) in enumerate(index.items()):
        if len(doc_ids) < 2:
            continue

        for a, b in combinations(sorted(doc_ids), 2):
            pair_shared[(a, b)] += 1

        if (i + 1) % 1000 == 0:
            elapsed = time.time() - start
            per_kw = elapsed / (i + 1)
            remaining = per_kw * (total_kw - i - 1)

            print(
                f"  Ребра: {i + 1:,}/{total_kw:,} KW — "
                f"{elapsed:.0f}с, ~{remaining / 60:.1f}хв залишилось"
            )

    edges = {
        pair: cnt
        for pair, cnt in pair_shared.items()
        if cnt >= min_shared
    }

    print(f"  Ребра: готово за {time.time() - start:.1f}с")

    return edges


def build_graph(df: pd.DataFrame, edges: dict) -> tuple:
    """
    Будує граф документів у rustworkx.

    Вершина = документ.
    Ребро = спільні ключові слова між двома документами.
    Вага ребра = кількість спільних ключових слів.
    """
    G = rx.PyGraph()

    doc_meta = df.drop_duplicates("doc_id").set_index("doc_id")[
        ["rubric", "outcome", "num_stages", "length"]
    ].to_dict("index")

    node_idx = {}

    for doc_id in df["doc_id"]:
        meta = doc_meta.get(doc_id, {})

        idx = G.add_node({
            "doc_id": doc_id,
            "rubric": meta.get("rubric", ""),
            "outcome": meta.get("outcome", ""),
            "num_stages": meta.get("num_stages", 0),
            "length": meta.get("length", 0),
        })

        node_idx[doc_id] = idx

    total = len(edges)
    start = time.time()
    done = 0

    for (a, b), weight in edges.items():
        if a in node_idx and b in node_idx:
            G.add_edge(node_idx[a], node_idx[b], weight)

        done += 1

        if done % 500000 == 0:
            elapsed = time.time() - start
            per_edge = elapsed / done
            remaining = per_edge * (total - done)

            print(
                f"  Граф: {done:,}/{total:,} ребер — "
                f"{elapsed:.0f}с, ~{remaining / 60:.1f}хв залишилось"
            )

    print(f"  Граф: готово за {time.time() - start:.1f}с")

    return G, node_idx


def compute_metrics(G: rx.PyGraph) -> tuple:
    """
    Базові метрики графа через rustworkx.
    """
    n = G.num_nodes()
    e = G.num_edges()
    max_edges = n * (n - 1) / 2

    metrics = {
        "num_nodes": n,
        "num_edges": e,
        "density": round(e / max_edges if max_edges > 0 else 0, 6),
    }

    degrees = [G.degree(i) for i in range(G.num_nodes())]

    metrics["avg_degree"] = round(sum(degrees) / len(degrees), 4) if degrees else 0
    metrics["max_degree"] = max(degrees) if degrees else 0
    metrics["min_degree"] = min(degrees) if degrees else 0

    components = rx.connected_components(G)

    metrics["num_components"] = len(components)
    metrics["largest_component_size"] = max(len(c) for c in components) if components else 0
    metrics["isolated_nodes"] = sum(1 for c in components if len(c) == 1)

    return metrics, components


def compute_nx_metrics(G: rx.PyGraph, components: list) -> tuple:
    """
    Додаткові метрики через networkx:
    - clustering coefficient
    - transitivity
    - degree assortativity
    - Louvain communities
    - modularity

    Рахуємо тільки на найбільшій компоненті.
    """
    print("  Конвертуємо у networkx...")

    start = time.time()

    if not components:
        empty_metrics = {
            "clustering_coefficient": 0.0,
            "transitivity": 0.0,
            "degree_assortativity": 0.0,
            "num_communities_louvain": 0,
            "modularity": 0.0,
        }
        return empty_metrics, nx.Graph()

    largest_nodes = set(max(components, key=len))

    H = nx.Graph()

    for n in largest_nodes:
        H.add_node(n)

    for u, v in G.edge_list():
        if u in largest_nodes and v in largest_nodes:
            H.add_edge(u, v)

    print(
        f"  NetworkX підграф: {H.number_of_nodes():,} вершин, "
        f"{H.number_of_edges():,} ребер — {time.time() - start:.1f}с"
    )

    nx_metrics = {}

    print("  Рахуємо clustering...")
    start = time.time()

    if H.number_of_nodes() > 0:
        nx_metrics["clustering_coefficient"] = round(nx.average_clustering(H), 6)
    else:
        nx_metrics["clustering_coefficient"] = 0.0

    print(f"  clustering — {time.time() - start:.1f}с")

    print("  Рахуємо transitivity...")
    start = time.time()

    if H.number_of_edges() > 0:
        nx_metrics["transitivity"] = round(nx.transitivity(H), 6)
    else:
        nx_metrics["transitivity"] = 0.0

    print(f"  transitivity — {time.time() - start:.1f}с")

    print("  Рахуємо assortativity...")
    start = time.time()

    try:
        if H.number_of_edges() > 0:
            nx_metrics["degree_assortativity"] = round(
                nx.degree_assortativity_coefficient(H), 6
            )
        else:
            nx_metrics["degree_assortativity"] = 0.0
    except Exception:
        nx_metrics["degree_assortativity"] = 0.0

    print(f"  assortativity — {time.time() - start:.1f}с")

    print("  Louvain community detection...")
    start = time.time()

    if H.number_of_edges() > 0:
        partition = community_louvain.best_partition(H)
        num_comm = len(set(partition.values()))

        nx_metrics["num_communities_louvain"] = num_comm
        nx_metrics["modularity"] = round(
            community_louvain.modularity(partition, H), 6
        )
    else:
        nx_metrics["num_communities_louvain"] = 0
        nx_metrics["modularity"] = 0.0

    print(
        f"  Louvain — {time.time() - start:.1f}с, "
        f"спільнот: {nx_metrics['num_communities_louvain']}"
    )

    return nx_metrics, H


def main():
    total_start = time.time()

    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)

    print(f"  Файл: {input_path}")
    print(f"  Документів: {len(df):,}")
    print(f"  Колонка KW: {KEYWORD_COL}")

    if KEYWORD_COL not in df.columns:
        raise ValueError(f"У файлі немає колонки: {KEYWORD_COL}")

    print("\nПеревірка першого рядка keywords:")
    first_value = df[KEYWORD_COL].iloc[0]
    print(f"  Тип: {type(first_value)}")
    print(f"  Значення: {first_value}")

    parsed_sample = parse_keywords(first_value)
    print(f"  Після parse_keywords: {parsed_sample}")
    print(f"  К-сть KW у першому документі: {len(parsed_sample)}")

    print("\nБудуємо інвертований індекс...")
    index = build_inverted_index(df)

    print(f"\nРахуємо ребра (MIN_SHARED={MIN_SHARED})...")
    edges = get_edges(index, MIN_SHARED)

    print(f"  Ребер: {len(edges):,}")

    print("\nБудуємо граф (rustworkx)...")
    G, node_idx = build_graph(df, edges)

    print(f"  Вершин: {G.num_nodes():,}  |  Ребер: {G.num_edges():,}")

    print("\nРахуємо метрики...")
    metrics, components = compute_metrics(G)
    nx_metrics, H = compute_nx_metrics(G, components)

    all_metrics = {
        **metrics,
        **nx_metrics,
        "min_shared_keywords": MIN_SHARED,
    }

    print("\n── Метрики ──────────────────────────────────────────")

    for k, v in all_metrics.items():
        print(f"  {k:40s}: {v}")

    total_time = time.time() - total_start

    print(f"\n── Загальний час: {total_time:.1f}с ({total_time / 60:.1f} хв) ──")

    graph_dir.mkdir(parents=True, exist_ok=True)

    graph_file = graph_dir / f"graph_keybert{MIN_SHARED}.pkl"
    nx_file = graph_dir / f"graph_nx_keybert{MIN_SHARED}.pkl"
    metrics_file = graph_dir / f"metrics_keybert{MIN_SHARED}.json"

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