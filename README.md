# data-from-wrds

data-from-wrds — scaffolded by **newpackage**.

**Author:** Mihai Ion  
**License:** MIT (c) 2025 Mihai Ion

## Layout
```
data-from-wrds/
  ├─ pyproject.toml
  ├─ README.md
  ├─ LICENSE
  ├─ .gitignore
  └─ src/
     └─ data_from_wrds/
        └─ __init__.py
```

## Development install
```bash
pip install -e .
```

## Notes
- MIT license included.
- `src` layout with explicit package map in `pyproject.toml`.
- A console script entry point is pre-wired to `data_from_wrds.cli:main` and exposes the `data_from_wrds` command. Create `src/data_from_wrds/cli.py` with a `main()` to activate it.