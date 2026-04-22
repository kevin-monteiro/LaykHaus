#!/usr/bin/env python3
"""
vault — pack any folder into an encrypted file; unpack with the same key.

Two output modes, auto-detected from the destination's extension:
    *.pdf  → embeds the encrypted payload inside a valid-looking PDF skeleton.
             `file` reports "PDF document", opens (blank) in a PDF viewer.
    other  → writes a raw encrypted binary blob (small, efficient).

Usage:
    python tools/vault.py pack   <source-dir>  <output-file>
    python tools/vault.py unpack <input-file>  <output-dir>

Crypto (identical in both modes):
    - AES-256-GCM for authenticated encryption.
    - Scrypt for key derivation (N=2^17, r=8, p=1).
    - Wrong passphrase or modified file bytes → decryption fails loudly.

Passphrase:
    - Read interactively via getpass, or from the LAYKHAUS_VAULT_PW env var.

Note on PDF mode: the file is a valid PDF that renders as a blank 1-page
document. The encrypted payload is stored inside a custom PDF stream object
with subtype /Laykhaus.Vault, base64-encoded and fenced with sentinel markers
so parsing is trivial. The content remains strongly encrypted; the PDF is
only a wrapper to hide the *fact* that the file is a vault from casual
inspection and automated file-type scanners.
"""
import argparse
import base64
import getpass
import io
import os
import sys
import tarfile
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

MAGIC = b"LAYKVLT\x00"
VERSION = 1
SALT_LEN = 16
NONCE_LEN = 12
KEY_LEN = 32
SCRYPT_N, SCRYPT_R, SCRYPT_P = 2**17, 8, 1

# PDF-mode markers (appear inside a PDF stream; not visible when rendered).
PDF_BEGIN = b"---BEGIN-LHV---\n"
PDF_END = b"\n---END-LHV---"


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=KEY_LEN, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P)
    return kdf.derive(passphrase.encode("utf-8"))


def _read_passphrase(confirm: bool) -> str:
    env = os.environ.get("LAYKHAUS_VAULT_PW")
    if env:
        return env
    pw = getpass.getpass("Passphrase: ")
    if not pw:
        sys.exit("Empty passphrase rejected.")
    if confirm:
        again = getpass.getpass("Confirm passphrase: ")
        if pw != again:
            sys.exit("Passphrases do not match.")
    return pw


def _encrypt_folder(src: Path, passphrase: str) -> tuple[bytes, int]:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(src, arcname=src.name)
    plaintext = buf.getvalue()

    salt = os.urandom(SALT_LEN)
    nonce = os.urandom(NONCE_LEN)
    key = _derive_key(passphrase, salt)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, associated_data=MAGIC)
    sealed = MAGIC + bytes([VERSION]) + salt + nonce + ciphertext
    return sealed, len(plaintext)


def _decrypt_sealed(sealed: bytes, passphrase: str) -> bytes:
    if sealed[: len(MAGIC)] != MAGIC:
        sys.exit("Not a LaykHaus vault (magic mismatch).")
    if sealed[len(MAGIC)] != VERSION:
        sys.exit(f"Unsupported vault version: {sealed[len(MAGIC)]}")

    offset = len(MAGIC) + 1
    salt = sealed[offset : offset + SALT_LEN]
    offset += SALT_LEN
    nonce = sealed[offset : offset + NONCE_LEN]
    offset += NONCE_LEN
    ciphertext = sealed[offset:]

    key = _derive_key(passphrase, salt)
    try:
        return AESGCM(key).decrypt(nonce, ciphertext, associated_data=MAGIC)
    except Exception:
        sys.exit("Decryption failed (wrong passphrase or corrupted file).")


def _extract_to(plaintext: bytes, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    extract_kwargs = {}
    if sys.version_info >= (3, 12):
        extract_kwargs["filter"] = "data"
    with tarfile.open(fileobj=io.BytesIO(plaintext), mode="r:gz") as tar:
        tar.extractall(dst, **extract_kwargs)


def _build_pdf_wrapper(sealed: bytes) -> bytes:
    """Wrap the sealed blob inside a minimal valid 1-page PDF."""
    payload = PDF_BEGIN + base64.b64encode(sealed) + PDF_END

    header = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"

    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << >> /Contents 4 0 R >>",
        b"<< /Length 4 >>\nstream\nq\nQ\nendstream",
        b"<< /Length "
        + str(len(payload)).encode()
        + b" /Type /Metadata /Subtype /Laykhaus.Vault >>\nstream\n"
        + payload
        + b"\nendstream",
    ]

    pdf = bytearray(header)
    offsets: list[int] = []
    for i, body in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"

    xref_offset = len(pdf)
    pdf += f"xref\n0 {len(objects) + 1}\n".encode()
    pdf += b"0000000000 65535 f \n"
    for off in offsets:
        pdf += f"{off:010d} 00000 n \n".encode()

    pdf += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF\n"
    ).encode()

    return bytes(pdf)


def _extract_sealed_from_pdf(data: bytes) -> bytes:
    if not data.startswith(b"%PDF-"):
        sys.exit("Not a PDF-wrapped vault (missing %PDF- header).")
    start = data.find(PDF_BEGIN)
    end = data.find(PDF_END, start + len(PDF_BEGIN)) if start >= 0 else -1
    if start < 0 or end < 0:
        sys.exit("PDF does not contain a LaykHaus vault payload.")
    b64 = data[start + len(PDF_BEGIN) : end]
    try:
        return base64.b64decode(b64, validate=True)
    except Exception:
        sys.exit("Vault payload inside PDF is corrupted.")


def pack(src: Path, dst: Path) -> None:
    if not src.is_dir():
        sys.exit(f"source is not a directory: {src}")

    passphrase = _read_passphrase(confirm=True)
    sealed, raw = _encrypt_folder(src, passphrase)

    pdf_mode = dst.suffix.lower() == ".pdf"
    output = _build_pdf_wrapper(sealed) if pdf_mode else sealed

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(output)

    mode = "PDF-wrapped" if pdf_mode else "raw"
    print(
        f"Packed {src} — {raw:,} B raw → "
        f"{dst.stat().st_size:,} B sealed ({mode}) → {dst}"
    )


def unpack(src: Path, dst: Path) -> None:
    if not src.is_file():
        sys.exit(f"input file not found: {src}")
    if dst.exists() and dst.is_dir() and any(dst.iterdir()):
        sys.exit(f"destination exists and is not empty: {dst}")

    data = src.read_bytes()
    if data.startswith(b"%PDF-"):
        sealed = _extract_sealed_from_pdf(data)
    else:
        sealed = data

    passphrase = _read_passphrase(confirm=False)
    plaintext = _decrypt_sealed(sealed, passphrase)
    _extract_to(plaintext, dst)

    print(f"Unpacked {src} → {dst}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Pack/unpack an encrypted folder vault.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("pack", help="compress + encrypt a directory into a single file")
    p.add_argument("src", type=Path)
    p.add_argument("dst", type=Path)

    u = sub.add_parser("unpack", help="decrypt + extract a vault back to a directory")
    u.add_argument("src", type=Path)
    u.add_argument("dst", type=Path)

    args = ap.parse_args()
    if args.cmd == "pack":
        pack(args.src, args.dst)
    else:
        unpack(args.src, args.dst)


if __name__ == "__main__":
    main()
