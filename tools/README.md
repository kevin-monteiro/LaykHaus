# tools/

Standalone utilities that are not part of the runtime — use them from the repo root.



### Requirements

- Python 3.9+
- [`cryptography`](https://pypi.org/project/cryptography/) — install with `pip install cryptography`.

### Usage

```bash
# Pack: encrypt a folder into one file
python3 tools/vault.py pack <source-dir> <output-file>

# Unpack: decrypt back into a folder
python3 tools/vault.py unpack <input-file> <output-dir>
```