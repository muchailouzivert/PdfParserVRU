from pathlib import Path
import pandas as pd
import json
import time
from keybert import KeyBERT
from sentence_transformers import SentenceTransformer

BASE_DIR    = Path("D:/DIPLOM/PdfParserVRU/dataset_comparative_2020_2026")
input_path  = BASE_DIR / "comparative_files" / "enriched_docs.parquet"
output_path = BASE_DIR / "comparative_files" / "keywords_keybert.parquet"
output_csv  = BASE_DIR / "comparative_files" / "keywords_keybert_meta.csv"

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
BATCH_SIZE = 16

def num_keywords(word_count: int) -> int:
    return max(5, min(50, word_count // 100))

def process_batch(batch: list, kw_model: KeyBERT) -> list:
    doc_ids     = [item[0] for item in batch]
    texts       = [item[1] for item in batch]
    word_counts = [item[2] for item in batch]

    k_max = max(num_keywords(wc) for wc in word_counts)

    try:
        all_keywords = kw_model.extract_keywords(
            texts,
            keyphrase_ngram_range=(1, 2),
            stop_words=None,
            top_n=k_max,
            use_mmr=True,
            diversity=0.5,
        )
    except Exception as e:
        print(f"Помилка батчу: {e}")
        all_keywords = [[] for _ in batch]

    results = []
    for doc_id, word_count, keywords in zip(doc_ids, word_counts, all_keywords):
        k = num_keywords(word_count)
        kw_list = [kw for kw, score in keywords[:k]]
        results.append({
            "doc_id":           doc_id,
            "keywords_keybert": json.dumps(kw_list, ensure_ascii=False),
            "num_keywords":     len(kw_list),
        })

    return results

def main():
    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)
    print(f"  Документів: {len(df):,}")

    print(f"\nЗавантажуємо модель {MODEL_NAME}...")
    model_start     = time.time()
    model           = SentenceTransformer(MODEL_NAME)
    kw_model        = KeyBERT(model=model)
    model_load_time = time.time() - model_start
    print(f"  Модель завантажена за {model_load_time:.1f}с")

    args_list = [
        (row["doc_id"], row["text"], row["length"])
        for _, row in df.iterrows()
    ]

    total_batches = (len(args_list) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nВитягуємо ключові слова (KeyBERT, batch_size={BATCH_SIZE})...")

    start_time = time.time()
    results    = []

    for batch_num in range(total_batches):
        batch         = args_list[batch_num * BATCH_SIZE:(batch_num + 1) * BATCH_SIZE]
        batch_results = process_batch(batch, kw_model)
        results.extend(batch_results)

        elapsed   = time.time() - start_time
        per_batch = elapsed / (batch_num + 1)
        remaining = per_batch * (total_batches - batch_num - 1)
        print(f"  Батч {batch_num+1:,}/{total_batches:,} — "
              f"{elapsed:.0f}с, ~{remaining/60:.1f}хв залишилось")

    total_time = time.time() - start_time

    print(f"\nЧас виконання")
    print(f"  Завантаження моделі: {model_load_time:.1f}с")
    print(f"  Всього: {total_time:.1f}с ({total_time/60:.1f} хв)")
    print(f"  На документ: {total_time/len(args_list):.3f}с")

    kw_df = pd.DataFrame(results)

    meta_cols = ["doc_id", "length", "rubric", "outcome", "num_stages",
                 "main_committee", "card_id", "reg_num", "reg_date"]
    final_df = df[meta_cols].merge(kw_df, on="doc_id", how="left")

    print(f"\nСередня к-сть KW: {final_df['num_keywords'].mean():.1f}")
    print(f"Мін / Макс KW:    {final_df['num_keywords'].min()} / {final_df['num_keywords'].max()}")

    timing = {
        "method":        "KeyBERT",
        "model":         MODEL_NAME,
        "batch_size":    BATCH_SIZE,
        "model_load_s":  round(model_load_time, 2),
        "total_s":       round(total_time, 2),
        "total_min":     round(total_time / 60, 2),
        "per_doc_s":     round(total_time / len(args_list), 4),
        "num_docs":      len(args_list),
    }
    with open(BASE_DIR / "comparative_files" / "keybert_timing.json", "w") as f:
        json.dump(timing, f, indent=2)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(output_path, index=False)
    final_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"\nЗбережено → {output_path}")
    print(f"Збережено → {output_csv}")
    print(f"Час → keybert_timing.json")

    sample = final_df.iloc[0]
    print(f"\nПриклад — {sample['doc_id']}:")
    print(f"  KW: {json.loads(sample['keywords_keybert'])}")


if __name__ == "__main__":
    main()