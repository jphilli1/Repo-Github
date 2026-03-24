import shutil
import os
import tarfile
import base64
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

staging = "package_staging"
if os.path.exists(staging):
    shutil.rmtree(staging)
os.makedirs(staging)

# Copy ONLY the allowed files
shutil.copytree("corp_etl", os.path.join(staging, "corp_etl"),
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "tests", "output"))
shutil.copy2("output/WMLC_Dashboard.xlsm", os.path.join(staging, "WMLC_Dashboard.xlsm"))
shutil.copy2("output/CORP_DEPLOYMENT.md", os.path.join(staging, "CORP_DEPLOYMENT.md"))
shutil.copy2("output/UNPACK_INSTRUCTIONS.txt", os.path.join(staging, "UNPACK_INSTRUCTIONS.txt"))

# Verify — list everything
print("=== Staging contents ===")
for root, dirs, files in os.walk(staging):
    for f in files:
        path = os.path.join(root, f)
        size = os.path.getsize(path)
        rel = os.path.relpath(path, staging)
        print(f"  {rel} ({size:,} bytes)")

file_count = sum(len(files) for _, _, files in os.walk(staging))
print(f"\nTotal files: {file_count}")
assert file_count <= 25, f"Too many files ({file_count})"

# Check for banned content
for root, dirs, files in os.walk(staging):
    for f in files:
        rel = os.path.relpath(os.path.join(root, f), staging)
        banned_patterns = ["log", "proxy", "scripts", "templates", "specs", "skills",
                          "agents", "CLAUDE", "pycache", ".pyc", "TODO", "test_report"]
        for b in banned_patterns:
            assert b.lower() not in rel.lower(), f"BANNED file in package: {rel}"

# Create tar.gz
tar_path = "output/corp_etl_package.tar.gz"
with tarfile.open(tar_path, "w:gz") as tar:
    tar.add(staging, arcname=".")
print(f"\nCreated: {tar_path} ({os.path.getsize(tar_path):,} bytes)")

# Create base64 payload — single line, no wrapping
with open(tar_path, "rb") as f:
    encoded = base64.b64encode(f.read()).decode("ascii")
payload_path = "output/corp_etl_payload.txt"
with open(payload_path, "w") as f:
    f.write(encoded)  # Single line, no wrapping
print(f"Created: {payload_path} ({len(encoded):,} chars, {len(encoded)/1024:.1f} KB)")

# Verify — no line breaks in payload
with open(payload_path, "r") as f:
    content = f.read()
assert "\n" not in content.strip(), "Payload contains line breaks!"
assert "\r" not in content.strip(), "Payload contains carriage returns!"
print("Payload is single-line (no whitespace issues)")

# Verify package contents
with tarfile.open(tar_path, "r:gz") as tar:
    names = sorted(tar.getnames())
    print(f"\n=== Package contents ({len(names)} entries) ===")
    for n in names:
        print(f"  {n}")
    banned = [n for n in names if any(b in n.lower() for b in
              ["log", "proxy", "script", "template", "spec", "skill",
               "agent", "claude", "pycache", ".pyc", "todo", "test_report"])]
    assert len(banned) == 0, f"BANNED files in package: {banned}"
print("\nPackage validation PASSED")

# Cleanup staging
shutil.rmtree(staging)
print("Cleaned up staging directory")
