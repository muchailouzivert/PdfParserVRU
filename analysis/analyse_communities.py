"""
Аналіз Louvain-спільнот для KeyBERT filtered 6%.

Вхід:
- networkx .pkl граф із Louvain-спільнотами;
- enriched_docs.parquet з метаданими документів;
- вузли графа зіставляються з документами через node_key.

Вихід:
- community_analysis/results/keybert_6pct/community_stats.csv
- PNG-графіки для інтерпретації спільнот.
"""

import pickle
import warnings
import textwrap
from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.gridspec import GridSpec
warnings.filterwarnings("ignore")

BASE_DIR = Path("D:/DIPLOM/PdfParserVRU/dataset_comparative_2020_2026/comparative_files")

ENRICHED = BASE_DIR / "enriched_docs.parquet"
GRAPH_DIR = BASE_DIR / "keyBert_Filtred_metrics"

METHOD = "keybert"
PCT = 6

OUT_DIR = Path("community_analysis/results") / f"{METHOD}_{PCT}pct"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N_COMMUNITIES = 8
TOP_K = 6

PALETTE = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3",
    "#937860", "#DA8BC3", "#8C8C8C", "#CCB974", "#64B5CD",
]

def load_graph(path: Path) -> nx.Graph:
    if not path.exists():
        raise FileNotFoundError(f"Не знайдено граф: {path}")

    with open(path, "rb") as f:
        G = pickle.load(f)

    if not isinstance(G, nx.Graph):
        raise TypeError("Очікувався networkx.Graph")

    return G

def load_enriched(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Не знайдено enriched файл: {path}")

    df = pd.read_parquet(path)
    df["doc_id"] = df["doc_id"].astype(str)

    # Важливо: у твоєму графі вузол 1 відповідає рядку 0.
    # Тому node_key = iloc_index + 1.
    df["node_key"] = range(1, len(df) + 1)

    if "reg_date" in df.columns:
        df["reg_year"] = pd.to_datetime(df["reg_date"], errors="coerce").dt.year

    return df

def find_keybert_graph_file() -> Path:
    candidates = list(GRAPH_DIR.glob(f"*{PCT}pct*networkx*.pkl"))

    if not candidates:
        candidates = [
            p for p in GRAPH_DIR.glob(f"*{PCT}pct*.pkl")
            if "rustworkx" not in p.name.lower()
        ]

    if not candidates:
        raise FileNotFoundError(f"Не знайдено KeyBERT graph для {PCT}% у {GRAPH_DIR}")

    return sorted(candidates)[0]

def get_community_map(G: nx.Graph) -> dict:
    """
    Повертає {node: community_id}.
    Якщо community вже є у вузлах — використовує його.
    Якщо немає — рахує Louvain.
    """
    for attr in ("community", "louvain", "community_id"):
        sample = [d.get(attr) for _, d in list(G.nodes(data=True))[:20]]

        if any(v is not None for v in sample):
            return {n: d.get(attr) for n, d in G.nodes(data=True)}

    try:
        from community import best_partition
        return best_partition(G)
    except ImportError:
        comms = nx.algorithms.community.louvain_communities(G, seed=42)
        return {n: i for i, c in enumerate(comms) for n in c}


def _wrap(text, width=22):
    return "\n".join(textwrap.wrap(str(text), width))

def _parse_top_vals(cell: str, k=6) -> dict:
    """
    'Рубрика А(12); Рубрика Б(5)' -> {'Рубрика А': 12, ...}
    """
    result = {}

    if not isinstance(cell, str) or not cell.strip():
        return result

    for part in cell.split(";"):
        part = part.strip()

        if "(" in part and part.endswith(")"):
            name = part[:part.rfind("(")].strip()

            try:
                count = int(part[part.rfind("(") + 1:-1])
                result[name] = count
            except ValueError:
                pass

    return dict(list(result.items())[:k])

def community_stats(G: nx.Graph, comm_map: dict, meta: pd.DataFrame) -> pd.DataFrame:
    """
    Для кожної Louvain-спільноти рахує:
    - розмір;
    - кількість ребер;
    - density;
    - avg degree;
    - top hubs;
    - top rubrics;
    - top committees;
    - outcome distribution;
    - year range.
    """
    rows = []
    nodes_series = pd.Series(comm_map)

    for community_id, group in nodes_series.groupby(nodes_series):
        members = list(group.index)
        subgraph = G.subgraph(members)

        size = len(members)
        edges = subgraph.number_of_edges()
        max_edges = size * (size - 1) / 2
        density = edges / max_edges if max_edges > 0 else 0.0

        degrees = dict(G.degree(members))
        avg_degree = np.mean(list(degrees.values())) if degrees else 0
        top_hubs = sorted(degrees, key=degrees.get, reverse=True)[:5]

        sub_meta = meta[meta["node_key"].isin(members)]

        def top_vals(col, k=TOP_K):
            if col not in sub_meta.columns or sub_meta[col].isna().all():
                return ""

            return "; ".join(
                f"{value}({count})"
                for value, count in sub_meta[col].value_counts().head(k).items()
            )

        if "reg_year" in sub_meta.columns and not sub_meta["reg_year"].isna().all():
            year_range = f"{int(sub_meta['reg_year'].min())}–{int(sub_meta['reg_year'].max())}"
        else:
            year_range = ""

        rows.append({
            "community": community_id,
            "size": size,
            "edges": edges,
            "density": round(density, 5),
            "avg_degree": round(avg_degree, 2),
            "top_hubs": "; ".join(str(h) for h in top_hubs),
            "top_rubrics": top_vals("rubric"),
            "top_committees": top_vals("main_committee"),
            "outcome_dist": top_vals("outcome"),
            "year_range": year_range,
        })

    result = (
        pd.DataFrame(rows)
        .sort_values("size", ascending=False)
        .reset_index(drop=True)
    )

    result.insert(0, "rank", range(1, len(result) + 1))

    return result


def build_initiator_counts(meta: pd.DataFrame, comm_map: dict) -> pd.DataFrame:
    if "initiators" not in meta.columns:
        return pd.DataFrame()

    graph_nodes = set(int(n) for n in comm_map.keys())
    work_df = meta[meta["node_key"].isin(graph_nodes)].copy()

    counter = Counter()

    for value in work_df["initiators"].dropna():
        value = str(value).strip()

        if not value:
            continue

        for chunk in value.replace(",", ";").split(";"):
            initiator = chunk.strip()

            if initiator:
                counter.update([initiator])

    result = pd.DataFrame(
        counter.most_common(),
        columns=["initiator", "count"]
    )

    if result.empty:
        return result

    result["rank"] = range(1, len(result) + 1)

    return result

def plot_community_overview(stats: pd.DataFrame):
    top = stats.head(TOP_N_COMMUNITIES).copy()
    top["label"] = top["community"].astype(str).apply(lambda x: f"C{x}")

    fig = plt.figure(figsize=(18, 12))
    fig.suptitle(
        f"KEYBERT | {PCT}% відкинутих — огляд спільнот",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )

    gs = GridSpec(2, 2, figure=fig, hspace=0.42, wspace=0.35)

    ax1 = fig.add_subplot(gs[0, 0])
    bars = ax1.bar(
        top["label"],
        top["size"],
        color=[PALETTE[i % len(PALETTE)] for i in range(len(top))],
        edgecolor="white",
        linewidth=0.5,
    )
    ax1.bar_label(bars, fmt="%d", fontsize=8, padding=2)
    ax1.set_title("Кількість документів у спільноті")
    ax1.set_xlabel("Спільнота")
    ax1.set_ylabel("Документів")

    ax2 = fig.add_subplot(gs[0, 1])
    ax2.bar(
        top["label"],
        top["density"],
        color=[PALETTE[i % len(PALETTE)] for i in range(len(top))],
        edgecolor="white",
        linewidth=0.5,
    )
    ax2.set_title("Щільність підграфу")
    ax2.set_xlabel("Спільнота")
    ax2.set_ylabel("Density")
    ax2.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.4f"))

    ax3 = fig.add_subplot(gs[1, 0])
    ax3.bar(
        top["label"],
        top["avg_degree"],
        color=[PALETTE[i % len(PALETTE)] for i in range(len(top))],
        edgecolor="white",
        linewidth=0.5,
    )
    ax3.set_title("Середній ступінь вузла")
    ax3.set_xlabel("Спільнота")
    ax3.set_ylabel("Avg degree")

    ax4 = fig.add_subplot(gs[1, 1])
    plot_rubric_heatmap(ax4, top)

    fig.savefig(OUT_DIR / "01_community_overview.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("01_community_overview.png")


def plot_rubric_heatmap(ax, top_stats: pd.DataFrame):
    all_rubrics = Counter()

    for _, row in top_stats.iterrows():
        all_rubrics.update(_parse_top_vals(row["top_rubrics"]))

    top_rubrics = [r for r, _ in all_rubrics.most_common(8)]

    if not top_rubrics:
        ax.text(0.5, 0.5, "Рубрики відсутні", ha="center", va="center")
        ax.set_title("Топ-рубрики × спільноти")
        return

    matrix = []

    for _, row in top_stats.iterrows():
        rubric_counts = _parse_top_vals(row["top_rubrics"])
        matrix.append([rubric_counts.get(r, 0) for r in top_rubrics])

    mat = np.array(matrix, dtype=float)

    row_sums = mat.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1

    mat_norm = mat / row_sums

    im = ax.imshow(mat_norm, aspect="auto", cmap="YlOrRd", vmin=0, vmax=1)

    ax.set_xticks(range(len(top_rubrics)))
    ax.set_xticklabels(
        [_wrap(r, 18) for r in top_rubrics],
        fontsize=7,
        rotation=35,
        ha="right",
    )

    ax.set_yticks(range(len(top_stats)))
    ax.set_yticklabels(
        [f"C{row['community']}" for _, row in top_stats.iterrows()],
        fontsize=8,
    )

    ax.set_title("Частка рубрик у спільноті (норм.)")
    plt.colorbar(im, ax=ax, fraction=0.03, pad=0.04)


def plot_rubric_distribution(stats: pd.DataFrame):
    top = stats.head(TOP_N_COMMUNITIES)
    cols = 2
    rows = (len(top) + 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(16, rows * 3.5))
    fig.suptitle(
        f"KEYBERT | {PCT}% — рубрики по спільнотах",
        fontsize=13,
        fontweight="bold",
    )

    axes = axes.flatten()

    for idx, (_, row) in enumerate(top.iterrows()):
        ax = axes[idx]
        rubric_counts = _parse_top_vals(row["top_rubrics"], k=TOP_K)

        if not rubric_counts:
            ax.text(0.5, 0.5, "немає даних", ha="center", va="center")
            continue

        labels = [_wrap(k, 20) for k in rubric_counts.keys()]
        values = list(rubric_counts.values())

        bars = ax.barh(
            labels,
            values,
            color=PALETTE[idx % len(PALETTE)],
            edgecolor="white",
            linewidth=0.4,
        )

        ax.bar_label(bars, fmt="%d", fontsize=8, padding=2)
        ax.invert_yaxis()
        ax.set_title(f"C{row['community']} n={row['size']} ρ={row['density']:.4f}")

    for j in range(idx + 1, len(axes)):
        axes[j].set_visible(False)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(OUT_DIR / "02_rubric_per_community.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("02_rubric_per_community.png")


def plot_status_outcomes(stats: pd.DataFrame):
    top = stats.head(TOP_N_COMMUNITIES)

    all_outcomes = Counter()

    for _, row in top.iterrows():
        all_outcomes.update(_parse_top_vals(row["outcome_dist"]))

    outcomes = [s for s, _ in all_outcomes.most_common(6)]

    if not outcomes:
        print("03_outcome.png — немає outcome")
        return

    matrix = []
    labels = []

    for _, row in top.iterrows():
        outcome_counts = _parse_top_vals(row["outcome_dist"])
        matrix.append([outcome_counts.get(o, 0) for o in outcomes])
        labels.append(f"C{row['community']}\n(n={row['size']})")

    mat = np.array(matrix, dtype=float)
    row_sums = mat.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    mat_pct = mat / row_sums * 100

    fig, ax = plt.subplots(figsize=(12, 5))
    bottom = np.zeros(len(top))

    for j, outcome in enumerate(outcomes):
        ax.bar(
            labels,
            mat_pct[:, j],
            bottom=bottom,
            label=_wrap(outcome, 25),
            color=PALETTE[j % len(PALETTE)],
            edgecolor="white",
            linewidth=0.4,
        )
        bottom += mat_pct[:, j]

    ax.set_title(f"KEYBERT | {PCT}% — розподіл outcome по спільнотах")
    ax.set_ylim(0, 110)
    ax.set_ylabel("% документів")
    ax.legend(fontsize=9, loc="upper right", framealpha=0.7)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "03_outcome.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("03_outcome.png")


def plot_year_timeline(comm_map: dict, meta: pd.DataFrame):
    if "reg_year" not in meta.columns:
        print("04_year_timeline.png — немає reg_year")
        return

    top_comms = (
        pd.Series(comm_map)
        .value_counts()
        .head(TOP_N_COMMUNITIES)
        .index
        .tolist()
    )

    nodes_df = pd.Series(comm_map).reset_index()
    nodes_df.columns = ["node_key", "community"]
    nodes_df["node_key"] = nodes_df["node_key"].astype(int)

    merged = nodes_df.merge(meta[["node_key", "reg_year"]], on="node_key", how="left")
    merged = merged[merged["community"].isin(top_comms)]

    pivot = (
        merged
        .groupby(["reg_year", "community"])
        .size()
        .unstack(fill_value=0)
    )

    fig, ax = plt.subplots(figsize=(14, 5))

    for i, community_id in enumerate(top_comms):
        if community_id in pivot.columns:
            ax.plot(
                pivot.index,
                pivot[community_id],
                marker="o",
                markersize=4,
                color=PALETTE[i % len(PALETTE)],
                label=f"C{community_id}",
                linewidth=1.8,
            )

    ax.set_title(f"KEYBERT | {PCT}% — динаміка документів по роках")
    ax.set_xlabel("Рік реєстрації")
    ax.set_ylabel("Кількість документів")
    ax.legend(fontsize=9, ncol=4, loc="upper left")
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", alpha=0.4)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "04_year_timeline.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("04_year_timeline.png")

def plot_committee_heatmap(stats: pd.DataFrame):
    top = stats.head(TOP_N_COMMUNITIES)

    all_committees = Counter()

    for _, row in top.iterrows():
        all_committees.update(_parse_top_vals(row["top_committees"]))

    committees = [c for c, _ in all_committees.most_common(8)]

    if not committees:
        print("05_committee_heatmap.png — немає комітетів")
        return

    matrix = []

    for _, row in top.iterrows():
        committee_counts = _parse_top_vals(row["top_committees"])
        matrix.append([committee_counts.get(c, 0) for c in committees])

    mat = np.array(matrix, dtype=float)

    row_sums = mat.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1

    mat_norm = mat / row_sums

    fig, ax = plt.subplots(figsize=(14, 5))

    im = ax.imshow(mat_norm, aspect="auto", cmap="Blues", vmin=0, vmax=1)

    ax.set_xticks(range(len(committees)))
    ax.set_xticklabels(
        [_wrap(c, 20) for c in committees],
        fontsize=8,
        rotation=40,
        ha="right",
    )

    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(
        [f"C{row['community']} n={row['size']}" for _, row in top.iterrows()],
        fontsize=9,
    )

    ax.set_title(f"KEYBERT | {PCT}% — частка комітетів у спільноті")

    plt.colorbar(im, ax=ax, fraction=0.02, pad=0.04, label="Частка (норм.)")

    fig.tight_layout()
    fig.savefig(OUT_DIR / "05_committee_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("05_committee_heatmap.png")

def plot_initiator_rank_distribution(initiator_counts: pd.DataFrame):
    if initiator_counts.empty:
        print("07_initiator_rank_distribution.png — немає ініціаторів")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.scatter(
        initiator_counts["rank"],
        initiator_counts["count"],
        s=18,
        alpha=0.8,
        color=PALETTE[0],
    )

    ax.set_title(f"KEYBERT | {PCT}% — ранговий розподіл ініціаторів")
    ax.set_xlabel("Ранг ініціатора")
    ax.set_ylabel("Кількість ініційованих документів")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "06_initiator_rank_distribution.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("06_initiator_rank_distribution.png")


def plot_top20_initiators(initiator_counts: pd.DataFrame):
    if initiator_counts.empty:
        print("08_top20_initiators.png — немає ініціаторів")
        return

    top20 = initiator_counts.head(20).copy()

    fig, ax = plt.subplots(figsize=(12, 7))

    bars = ax.barh(
        top20["initiator"][::-1],
        top20["count"][::-1],
        color=PALETTE[3],
        edgecolor="white",
        linewidth=0.5,
    )

    ax.bar_label(bars, fmt="%d", fontsize=8, padding=2)

    ax.set_title(f"KEYBERT | {PCT}% — топ-20 ініціаторів")
    ax.set_xlabel("Кількість ініційованих документів")
    ax.set_ylabel("Ініціатор")
    ax.grid(axis="x", alpha=0.3)

    fig.tight_layout()
    fig.savefig(OUT_DIR / "07_top20_initiators.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    print("07_top20_initiators.png")

def main():
    print("Аналіз Louvain-спільнот для KeyBERT filtered 6%")

    meta = load_enriched(ENRICHED)

    graph_file = find_keybert_graph_file()
    print("Граф:", graph_file)

    G = load_graph(graph_file)

    print(f"Вузлів: {G.number_of_nodes():,}")
    print(f"Ребер: {G.number_of_edges():,}")

    comm_map = get_community_map(G)
    print(f"Спільнот: {len(set(comm_map.values()))}")

    stats = community_stats(G, comm_map, meta)

    stats_path = OUT_DIR / "community_stats.csv"
    stats.to_csv(stats_path, index=False, encoding="utf-8-sig")
    print("community_stats.csv")

    plot_community_overview(stats)
    plot_rubric_distribution(stats)
    plot_status_outcomes(stats)
    plot_year_timeline(comm_map, meta)
    plot_committee_heatmap(stats)

    initiator_counts = build_initiator_counts(meta, comm_map)

    plot_initiator_rank_distribution(initiator_counts)
    plot_top20_initiators(initiator_counts)

    print("\nАналіз завершено.")
    print("Результати:", OUT_DIR)

if __name__ == "__main__":
    main()