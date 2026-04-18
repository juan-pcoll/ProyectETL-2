import glob
import re
import pandas as pd


def extract_data():

    print("Extracting data...")

    csv_files = sorted(glob.glob("data/raw/Personas(Departamental)*.csv"))
    if not csv_files:
        raise FileNotFoundError("No se encontraron archivos CSV en data/raw")

    frames = []
    for file_path in csv_files:
        df = pd.read_csv(
            file_path,
            sep=None,
            engine="python",
            encoding="latin1"
        )
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
        )
        if "nivel_educativo_aprobado.1" in df.columns:
            df = df.rename(columns={"nivel_educativo_aprobado.1": "nivel_educativo_matriculado"})

        year_match = re.search(r"(2023|2024)", file_path)
        if year_match is None:
            raise ValueError(f"No se pudo extraer el año del nombre de archivo: {file_path}")

        df["anio"] = int(year_match.group(1))
        frames.append(df)

    df = pd.concat(frames, ignore_index=True, sort=False)

    print(f"Data extracted successfully from {len(frames)} file(s)")

    return df