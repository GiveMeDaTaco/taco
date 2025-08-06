"""
Output file writer utilities.
"""
import os
from pathlib import Path
from tlptaco.utils.fs import grant_group_rwx

def write_dataframe(df, path: str, fmt: str, **kwargs):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.to_csv(path, index=False, **kwargs)
    elif fmt in ("excel", "xlsx"):
        df.to_excel(path, index=False, **kwargs)
    elif fmt == "parquet":
        df.to_parquet(path, index=False, **kwargs)
    # create .end file
    end_path = str(p.with_suffix(".end"))
    with open(end_path, "w") as f:
        f.write(str(len(df)))

    # Adjust permissions
    grant_group_rwx(path)
    grant_group_rwx(end_path)