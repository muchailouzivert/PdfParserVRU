from pathlib import Path
import pandas as pd

BASE_DIR     = Path("../dataset_comparative_2020_2026")
parquet_path = BASE_DIR / "comparative_files" / "parsed_pdfs.parquet"
bills_path   = BASE_DIR / "bills.csv"
comp_path    = BASE_DIR / "comparative_tables.csv"
passage_path = BASE_DIR / "passage.csv"

output_parquet = BASE_DIR / "comparative_files" / "comparative_tables_dataset.parquet"
output_csv     = BASE_DIR / "comparative_files" / "comparative_tables_dataset.csv"


def load_data():
    print("Завантажуємо дані...")
    docs    = pd.read_parquet(parquet_path)
    bills   = pd.read_csv(bills_path,   dtype={"card_id": int})
    comp    = pd.read_csv(comp_path,    dtype={"card_id": int})
    passage = pd.read_csv(passage_path, dtype={"card_id": int})

    print(f"  docs:    {len(docs):,}")
    print(f"  bills:   {len(bills):,}")
    print(f"  comp:    {len(comp):,}")
    print(f"  passage: {len(passage):,}")

    return docs, bills, comp, passage


def get_final_status(passage: pd.DataFrame) -> pd.DataFrame:
    ACCEPTED = ["прийнято", "підписано", "опублікування", "набрав чинності"]
    REJECTED = ["відхилено", "відкликано", "знято з розгляду"]

    def classify(status: str) -> str:
        if pd.isna(status):
            return "unknown"
        s = status.lower()
        if any(p in s for p in ACCEPTED):
            return "accepted"
        if any(p in s for p in REJECTED):
            return "rejected"
        return "in_progress"

    passage["status_date"] = pd.to_datetime(
        passage["status_date"], dayfirst=True, errors="coerce"
    )

    final = (
        passage
        .sort_values("status_date")
        .groupby("card_id")
        .agg(
            final_status      = ("status_text", "last"),
            final_status_date = ("status_date", "last"),
            num_stages        = ("status_text", "count"),
        )
        .reset_index()
    )
    final["outcome"] = final["final_status"].apply(classify)
    return final


def main():
    docs, bills, comp, passage = load_data()

    docs["reg_num_str"] = docs["doc_id"].str.extract(r'^([\d][\d\-а-яА-ЯіІїЇєЄ]*)_')
    comp["reg_num_str"] = comp["reg_num"].astype(str)

    comp_slim = (
        comp[["reg_num_str", "card_id", "reg_num", "reg_date"]]
        .drop_duplicates("reg_num_str")
    )

    print("\nВизначаємо фінальний статус законопроєктів...")
    final_status = get_final_status(passage)

    print(f"\nРозподіл результатів:")
    print(final_status["outcome"].value_counts().to_string())

    print("\nЗ'єднуємо таблиці...")

    merged = docs.merge(comp_slim, on="reg_num_str", how="left")
    print(f"  З card_id: {merged['card_id'].notna().sum():,} / {len(merged):,}")

    # Прибираємо дублікати doc_id якщо виникли
    merged = merged.drop_duplicates("doc_id")
    print(f"  Після дедублікації: {len(merged):,}")

    merged["card_id"] = merged["card_id"].astype("Int64")

    merged = merged.merge(
        bills[["card_id", "rubric", "initiators", "main_committee"]],
        on="card_id", how="left"
    )
    merged = merged.merge(
        final_status[["card_id", "final_status", "final_status_date", "num_stages", "outcome"]],
        on="card_id", how="left"
    )

    print(f"\nКолонки: {merged.columns.tolist()}")
    print(f"Пропущені card_id: {merged['card_id'].isna().sum():,}")
    print(f"Пропущені rubric:  {merged['rubric'].isna().sum():,}")
    print(f"Пропущені outcome: {merged['outcome'].isna().sum():,}")

    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    merged.to_parquet(output_parquet, index=False)
    merged.drop(columns=["text"]).to_csv(
        output_csv, index=False, encoding="utf-8-sig"
    )

    print(f"\nЗбережено → {output_parquet}")
    print(f"Збережено → {output_csv}")
    print(f"Всього документів: {len(merged):,}")


if __name__ == "__main__":
    main()