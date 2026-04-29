from pathlib import Path
import pandas as pd
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

BASE_DIR    = Path("../dataset_comparative_2020_2026")
input_path  = BASE_DIR / "comparative_files" / "enriched_docs.parquet"
output_path = BASE_DIR / "comparative_files" / "keywords_yake.parquet"
output_csv  = BASE_DIR / "comparative_files" / "keywords_yake_meta.csv"
MAX_WORKERS = max(1, os.cpu_count() - 1)

def num_keywords(word_count: int) -> int:
    """Кількість ключових слів залежно від довжини тексту."""
    return max(5, min(50, word_count // 100))


def extract_yake(args: tuple) -> dict:
    doc_id, text, word_count = args
    k = num_keywords(word_count)

    extractor = yake.KeywordExtractor(
        lan="uk",
        n=2,          # до біграм
        dedupLim=0.7, # дедублікація схожих KW
        top=k,
    )
    try:
        keywords = extractor.extract_keywords(text)
        # YAKE: менший score = важливіший
        kw_list = [kw for kw, score in keywords]
    except Exception as e:
        print(f"Помилка YAKE ({doc_id}): {e}")
        kw_list = []

    return {
        "doc_id":        doc_id,
        "keywords_yake": json.dumps(kw_list, ensure_ascii=False),
        "num_keywords":  len(kw_list),
    }


def main():
    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)
    print(f"  Документів: {len(df):,}")

    args_list = [
        (row["doc_id"], row["text"], row["length"])
        for _, row in df.iterrows()
    ]

    print(f"Витягуємо ключові слова (YAKE, workers={MAX_WORKERS})...")
    results = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(extract_yake, args): args[0]
            for args in args_list
        }
        done = 0
        for future in as_completed(futures):
            results.append(future.result())
            done += 1
            if done % 500 == 0:
                print(f"  {done:,} / {len(args_list):,} готово")

    print(f"  {len(results):,} / {len(args_list):,} готово")

    kw_df = pd.DataFrame(results)

    # Приєднуємо метадані (без тексту)
    meta_cols = ["doc_id", "length", "rubric", "outcome", "num_stages",
                 "main_committee", "card_id", "reg_num", "reg_date"]
    final_df = df[meta_cols].merge(kw_df, on="doc_id", how="left")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    final_df.to_parquet(output_path, index=False)
    final_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"\nСередня к-сть KW: {final_df['num_keywords'].mean():.1f}")
    print(f"Мін / Макс KW:    {final_df['num_keywords'].min()} / {final_df['num_keywords'].max()}")

    print(f"\nЗбережено → {output_path}")
    print(f"Збережено → {output_csv}")

    sample = final_df.iloc[0]
    print(f"\nПриклад — {sample['doc_id']}:")
    print(f"  KW: {json.loads(sample['keywords_yake'])}")


if __name__ == "__main__":
    main()