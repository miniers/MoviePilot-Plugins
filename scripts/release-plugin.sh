#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
用法:
  scripts/release-plugin.sh --plugin-key MPPlexTools --version 0.1.7 --notes "更新日志"

参数:
  --plugin-key   package.v2.json 中的插件键，例如 MPPlexTools
  --version      新版本号，例如 0.1.7
  --notes        更新日志内容
  --gh-user      可选，创建 release 使用的 gh 账号，默认 miniers

说明:
  - 自动更新 package.v2.json 中对应插件的 version/history/release
  - 自动更新插件 __init__.py 中的 plugin_version
  - 自动生成 zip 资产、commit、push、打 tag、创建 release 并上传资产
  - tag 使用 package 键原始大小写，例如 MPPlexTools_v0.1.7
  - zip 文件名使用插件目录小写名，例如 mpplextools_v0.1.7.zip
EOF
}

PLUGIN_KEY=""
VERSION=""
NOTES=""
GH_USER="miniers"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --plugin-key)
      PLUGIN_KEY="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --notes)
      NOTES="${2:-}"
      shift 2
      ;;
    --gh-user)
      GH_USER="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$PLUGIN_KEY" || -z "$VERSION" || -z "$NOTES" ]]; then
  echo "缺少必填参数" >&2
  usage >&2
  exit 1
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "版本号格式错误: $VERSION，要求 x.y.z" >&2
  exit 1
fi

STATUS_OUTPUT="$(git status --porcelain)"
if [[ -n "$STATUS_OUTPUT" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    path="${line:3}"
    case "$path" in
      package.v2.json|scripts|scripts/*|plugins.v2/*)
        ;;
      *)
        echo "存在与发布无关的未提交改动，请先清理后再运行脚本" >&2
        git status --short >&2
        exit 1
        ;;
    esac
  done <<< "$STATUS_OUTPUT"
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
export PLUGIN_KEY VERSION NOTES ROOT_DIR

PLUGIN_INFO="$($PYTHON_BIN - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
plugin_key = os.environ["PLUGIN_KEY"]
version = os.environ["VERSION"]
notes = os.environ["NOTES"]

package_path = root / "package.v2.json"
data = json.loads(package_path.read_text(encoding="utf-8"))

if plugin_key not in data:
    raise SystemExit(f"package.v2.json 中不存在插件键: {plugin_key}")

entry = data[plugin_key]
entry["version"] = version
entry["release"] = True
history = entry.setdefault("history", {})
new_history = {f"v{version}": notes}
for k, v in history.items():
    if k != f"v{version}":
        new_history[k] = v
entry["history"] = new_history
package_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

plugin_dir = root / "plugins.v2" / plugin_key.lower()
init_path = plugin_dir / "__init__.py"
if not init_path.exists():
    raise SystemExit(f"插件入口不存在: {init_path}")

text = init_path.read_text(encoding="utf-8")
old = 'plugin_version = "'
start = text.find(old)
if start == -1:
    raise SystemExit(f"未在 {init_path} 中找到 plugin_version")
value_start = start + len(old)
value_end = text.find('"', value_start)
text = text[:value_start] + version + text[value_end:]
init_path.write_text(text, encoding="utf-8")

print(plugin_dir.name)
PY
)"

PLUGIN_DIR_NAME="$PLUGIN_INFO"
ZIP_NAME="${PLUGIN_DIR_NAME}_v${VERSION}.zip"
ZIP_PATH="/tmp/${ZIP_NAME}"
TAG_NAME="${PLUGIN_KEY}_v${VERSION}"
RELEASE_TITLE="$TAG_NAME"
REPO_SLUG="$(git remote get-url origin | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"

rm -f "$ZIP_PATH"
(
  cd "plugins.v2/$PLUGIN_DIR_NAME"
  zip -r "$ZIP_PATH" . -x '__pycache__/*' '*.pyc' >/dev/null
)

git add package.v2.json "plugins.v2/$PLUGIN_DIR_NAME/__init__.py"
git commit -m "release(${PLUGIN_DIR_NAME}): bump to v${VERSION}"
git push origin main
git tag -f "$TAG_NAME"
git push origin "refs/tags/$TAG_NAME" --force

gh auth switch -u "$GH_USER" >/dev/null

if gh release view "$TAG_NAME" --repo "$REPO_SLUG" >/dev/null 2>&1; then
  gh release upload "$TAG_NAME" "$ZIP_PATH" --repo "$REPO_SLUG" --clobber >/dev/null
else
  gh release create "$TAG_NAME" "$ZIP_PATH" --repo "$REPO_SLUG" --verify-tag --title "$RELEASE_TITLE" --notes "$NOTES" >/dev/null
fi

echo "已发布: $PLUGIN_KEY v$VERSION"
echo "Tag: $TAG_NAME"
echo "Asset: $ZIP_PATH"
