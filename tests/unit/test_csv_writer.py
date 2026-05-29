import csv

import storage.csv_writer as csv_writer


def read_csv(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.reader(f))


def test_ensure_csv_headers_creates_all_files(monkeypatch, tmp_path):
    bills_csv = tmp_path / "bills.csv"
    comp_csv = tmp_path / "comparative_tables.csv"
    passage_csv = tmp_path / "passage.csv"

    monkeypatch.setattr(csv_writer, "BILLS_CSV", str(bills_csv))
    monkeypatch.setattr(csv_writer, "COMP_CSV", str(comp_csv))
    monkeypatch.setattr(csv_writer, "PASSAGE_CSV", str(passage_csv))

    csv_writer.ensure_csv_headers()

    assert bills_csv.exists()
    assert comp_csv.exists()
    assert passage_csv.exists()


def test_ensure_csv_headers_writes_bills_header(monkeypatch, tmp_path):
    bills_csv = tmp_path / "bills.csv"
    comp_csv = tmp_path / "comparative_tables.csv"
    passage_csv = tmp_path / "passage.csv"

    monkeypatch.setattr(csv_writer, "BILLS_CSV", str(bills_csv))
    monkeypatch.setattr(csv_writer, "COMP_CSV", str(comp_csv))
    monkeypatch.setattr(csv_writer, "PASSAGE_CSV", str(passage_csv))

    csv_writer.ensure_csv_headers()

    rows = read_csv(bills_csv)

    assert rows[0] == [
        "card_id",
        "reg_num",
        "reg_date",
        "rubric",
        "initiators",
        "main_committee",
        "has_comparative_table",
    ]


def test_ensure_csv_headers_writes_comparative_tables_header(monkeypatch, tmp_path):
    bills_csv = tmp_path / "bills.csv"
    comp_csv = tmp_path / "comparative_tables.csv"
    passage_csv = tmp_path / "passage.csv"

    monkeypatch.setattr(csv_writer, "BILLS_CSV", str(bills_csv))
    monkeypatch.setattr(csv_writer, "COMP_CSV", str(comp_csv))
    monkeypatch.setattr(csv_writer, "PASSAGE_CSV", str(passage_csv))

    csv_writer.ensure_csv_headers()

    rows = read_csv(comp_csv)

    assert rows[0] == [
        "card_id",
        "reg_num",
        "reg_date",
        "file_id",
        "ext",
        "file_name",
        "title",
        "tmp_path",
        "download_status",
        "mime_type",
        "pdf_path",
        "convert_status",
    ]


def test_ensure_csv_headers_writes_passage_header(monkeypatch, tmp_path):
    bills_csv = tmp_path / "bills.csv"
    comp_csv = tmp_path / "comparative_tables.csv"
    passage_csv = tmp_path / "passage.csv"

    monkeypatch.setattr(csv_writer, "BILLS_CSV", str(bills_csv))
    monkeypatch.setattr(csv_writer, "COMP_CSV", str(comp_csv))
    monkeypatch.setattr(csv_writer, "PASSAGE_CSV", str(passage_csv))

    csv_writer.ensure_csv_headers()

    rows = read_csv(passage_csv)

    assert rows[0] == [
        "card_id",
        "reg_num",
        "reg_date",
        "status_date",
        "status_text",
    ]


def test_ensure_csv_headers_does_not_overwrite_existing_file(monkeypatch, tmp_path):
    bills_csv = tmp_path / "bills.csv"
    comp_csv = tmp_path / "comparative_tables.csv"
    passage_csv = tmp_path / "passage.csv"

    bills_csv.write_text("custom,data\n1,2\n", encoding="utf-8")

    monkeypatch.setattr(csv_writer, "BILLS_CSV", str(bills_csv))
    monkeypatch.setattr(csv_writer, "COMP_CSV", str(comp_csv))
    monkeypatch.setattr(csv_writer, "PASSAGE_CSV", str(passage_csv))

    csv_writer.ensure_csv_headers()

    rows = read_csv(bills_csv)

    assert rows == [
        ["custom", "data"],
        ["1", "2"],
    ]


def test_append_rows_appends_rows_to_csv(tmp_path):
    path = tmp_path / "output.csv"
    path.write_text("a,b\n", encoding="utf-8")

    rows_to_append = [
        [1, "one"],
        [2, "two"],
    ]

    csv_writer.append_rows(str(path), rows_to_append)

    rows = read_csv(path)

    assert rows == [
        ["a", "b"],
        ["1", "one"],
        ["2", "two"],
    ]


def test_append_rows_does_nothing_for_empty_rows(tmp_path):
    path = tmp_path / "output.csv"
    path.write_text("a,b\n", encoding="utf-8")

    csv_writer.append_rows(str(path), [])

    rows = read_csv(path)

    assert rows == [["a", "b"]]