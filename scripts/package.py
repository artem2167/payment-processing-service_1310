"""Build the source archive from an explicit list of project files."""

from hashlib import sha256
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    output = root / "output"
    output.mkdir(exist_ok=True)
    archive_path = output / "payments-service.zip"
    files = [
        root / name
        for name in (
            "README.md",
            "VERIFICATION.md",
            "pyproject.toml",
            "uv.lock",
            "Dockerfile",
            "compose.yaml",
            "compose.test.yaml",
            "alembic.ini",
            ".env.example",
            ".gitignore",
            ".dockerignore",
        )
    ]
    for folder in ("app", "migrations", "tests", "examples", "scripts"):
        files.extend(
            path
            for path in (root / folder).rglob("*")
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix in {".py", ".json", ".mako"}
        )
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(files):
            archive.write(path, f"payments-service/{path.relative_to(root).as_posix()}")
    with ZipFile(archive_path) as archive:
        if archive.testzip() is not None:
            raise RuntimeError("Source archive failed its integrity check")
    checksum = sha256(archive_path.read_bytes()).hexdigest()
    (output / "payments-service.zip.sha256").write_text(
        f"{checksum}  payments-service.zip\n", encoding="utf-8"
    )
    print(f"Archive verified: {len(files)} files, {archive_path.stat().st_size} bytes")
    print(f"SHA256: {checksum}")


if __name__ == "__main__":
    main()
