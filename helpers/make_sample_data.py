import argparse, json, csv
from pathlib import Path

def norm(s: str) -> str:
    return (s or "").strip().casefold()

def find_repo_root(script_path: Path) -> Path:
    """
    Assume this script lives in <repo_root>/helpers/make_sample_data.py.
    To be safer, walk upwards until we find a 'util' folder or 'requirements.txt'.
    """
    for p in [script_path.parent] + list(script_path.parents):
        if (p / "util").is_dir() and (p / "sql").is_dir():
            return p
        if (p / "requirements.txt").exists():
            return p
    # Fallback: parent of helpers
    return script_path.parent.parent

def resolve_dir(repo_root: Path, maybe_relative: str) -> Path:
    p = Path(maybe_relative)
    return p if p.is_absolute() else (repo_root / p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", default="full_data", help="Folder with full source files (relative to repo root unless absolute).")
    ap.add_argument("--out", default="data", help="Folder to write sampled files to (relative to repo root unless absolute).")
    ap.add_argument("--entities", type=int, default=200_000, help="How many entity lines to keep.")
    ap.add_argument("--companies", type=int, default=300_000, help="How many company rows to keep (including header).")
    ap.add_argument("--companies-match", action="store_true", help="Prefer company rows whose name matches a Company entity caption.")
    ap.add_argument("--require-country", action="store_true", help="Keep only entities that have country/jurisdiction in properties.")
    args = ap.parse_args()

    script_path = Path(__file__).resolve()
    repo_root = find_repo_root(script_path)

    full = resolve_dir(repo_root, args.full)
    out = resolve_dir(repo_root, args.out)
    out.mkdir(parents=True, exist_ok=True)

    full_index = full / "index.json"
    full_entities = full / "entities.ftm.json"
    full_companies = full / "companies_sorted.csv"

    if not full_index.exists():
        raise SystemExit(f"Missing: {full_index}")
    if not full_entities.exists():
        raise SystemExit(f"Missing: {full_entities}")

    # 1) Build reduced entities.ftm.json + collect used datasets + company captions
    used_datasets = set()
    company_captions = set()

    out_entities = out / "entities.ftm.json"
    kept = 0

    with full_entities.open("r", encoding="utf-8") as fin, out_entities.open("w", encoding="utf-8") as fout:
        for line in fin:
            if kept >= args.entities:
                break

            obj = json.loads(line)

            if args.require_country:
                props = obj.get("properties") or {}
                if "country" not in props and "jurisdiction" not in props:
                    continue

            for d in (obj.get("datasets") or []):
                used_datasets.add(d)

            if obj.get("schema") == "Company":
                cap = obj.get("caption")
                if cap:
                    company_captions.add(norm(cap))

            fout.write(line)
            kept += 1

            if kept % 10000 == 0:
                print(f"\rKept entities: {kept}", end="")

    print(f"\nEntities kept: {kept}")
    print(f"Datasets referenced: {len(used_datasets)}")

    # 2) Write reduced index.json (same index, but datasets list limited to those referenced)
    idx = json.loads(full_index.read_text(encoding="utf-8"))
    idx["datasets"] = sorted(used_datasets)
    (out / "index.json").write_text(json.dumps(idx, ensure_ascii=False), encoding="utf-8")

    # 3) Write reduced companies_sorted.csv
    if full_companies.exists():
        out_companies = out / "companies_sorted.csv"

        with full_companies.open("r", encoding="utf-8", newline="") as fin, out_companies.open("w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(fin)
            writer = csv.writer(fout)

            header = next(reader, None)
            if header:
                writer.writerow(header)
                rows_written = 1
            else:
                rows_written = 0

            if args.companies_match and company_captions:
                # keep matching names first (best chance to make industry join meaningful)
                for row in reader:
                    if rows_written >= args.companies:
                        break
                    name = row[1] if len(row) > 1 else ""
                    if norm(name) in company_captions:
                        writer.writerow(row)
                        rows_written += 1

                # If too few matches, top up with the first rows
                if rows_written < min(args.companies, 10_000):
                    fin.seek(0)
                    reader2 = csv.reader(fin)
                    _ = next(reader2, None)
                    for row in reader2:
                        if rows_written >= args.companies:
                            break
                        writer.writerow(row)
                        rows_written += 1
            else:
                for row in reader:
                    if rows_written >= args.companies:
                        break
                    writer.writerow(row)
                    rows_written += 1

        print(f"Companies rows written: {rows_written}")
    else:
        print(f"Note: {full_companies} not found. Skipping company sampling.")

    print("\nWrote:")
    print(f"  {out / 'entities.ftm.json'}")
    print(f"  {out / 'index.json'}")
    if (out / "companies_sorted.csv").exists():
        print(f"  {out / 'companies_sorted.csv'}")

if __name__ == "__main__":
    main()