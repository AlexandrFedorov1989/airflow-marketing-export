import os

# пути вида data/raw/marketing_events/dt=YYYY-MM-DD/


def build_export_dir(base_dir: str, ds: str) -> str:
    return os.path.join(base_dir, "raw", "marketing_events", f"dt={ds}")


def build_export_path(base_dir: str, ds: str, filename: str = "export.jsonl") -> str:
    return os.path.join(build_export_dir(base_dir, ds), filename)


def build_success_marker_path(base_dir: str, ds: str) -> str:
    return os.path.join(build_export_dir(base_dir, ds), "_SUCCESS")
