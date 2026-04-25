#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TRAILER="Co-authored-by: Codex <noreply@openai.com>"

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
  - 若目标插件目录存在未提交源码改动，脚本会先自动提交这些源码改动，再继续版本发布
  - 自动更新 package.v2.json 中对应插件的 version/history/release
  - 自动更新插件 __init__.py 中的 plugin_version
  - 自动生成 zip 资产、commit、push、打 tag、创建 release 并上传资产
  - tag 使用 package 键原始大小写，例如 MPPlexTools_v0.1.7
  - zip 文件名使用插件目录小写名，例如 mpplextools_v0.1.7.zip
  - 为避免覆盖错误版本，若目标 tag 或 GitHub Release 已存在，脚本会直接失败
EOF
}

die() {
  echo "$*" >&2
  exit 1
}

commit_with_trailer() {
  local subject="$1"
  git commit -m "$subject" -m "$TRAILER"
}

validate_clean_scope() {
  local plugin_dir_name="$1"
  local status_output path normalized
  status_output="$(git status --porcelain)"
  if [[ -z "$status_output" ]]; then
    return 0
  fi

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    path="${line:3}"
    normalized="${path%/}"
    case "$normalized" in
      "plugins.v2/$plugin_dir_name"|"plugins.v2/$plugin_dir_name"/*)
        ;;
      *)
        echo "存在目标插件之外的未提交改动，请先清理后再运行脚本" >&2
        git status --short >&2
        exit 1
        ;;
    esac
  done <<< "$status_output"
}

ensure_version_not_exists() {
  local tag_name="$1"
  local repo_slug="$2"

  if git rev-parse -q --verify "refs/tags/$tag_name" >/dev/null; then
    die "本地已存在 tag: $tag_name，请改用新版本号"
  fi

  if git ls-remote --exit-code --tags origin "refs/tags/$tag_name" >/dev/null 2>&1; then
    die "远端已存在 tag: $tag_name，请改用新版本号"
  fi

  if gh release view "$tag_name" --repo "$repo_slug" >/dev/null 2>&1; then
    die "GitHub Release 已存在: $tag_name，请改用新版本号"
  fi
}

ensure_required_tools() {
  command -v git >/dev/null 2>&1 || die "未找到 git，无法发布"
  command -v gh >/dev/null 2>&1 || die "未找到 gh，无法创建 GitHub Release"
  command -v zip >/dev/null 2>&1 || die "未找到 zip，无法生成发布资产"
  command -v "$PYTHON_BIN" >/dev/null 2>&1 || die "未找到 Python 解释器: $PYTHON_BIN"
}

compile_plugin_python() {
  local plugin_dir_name="$1"
  local -a python_files=()
  while IFS= read -r -d '' file; do
    python_files+=("$file")
  done < <(find "plugins.v2/$plugin_dir_name" -type f -name '*.py' -print0)

  if [[ ${#python_files[@]} -gt 0 ]]; then
    "$PYTHON_BIN" -m py_compile "${python_files[@]}"
  fi
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
  die "缺少必填参数"
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  die "版本号格式错误: $VERSION，要求 x.y.z"
fi

CURRENT_BRANCH="$(git branch --show-current)"
if [[ "$CURRENT_BRANCH" != "main" ]]; then
  die "当前分支不是 main，发布脚本只允许在 main 上运行"
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
export PLUGIN_KEY VERSION NOTES ROOT_DIR

ensure_required_tools
gh auth switch -u "$GH_USER" >/dev/null

PLUGIN_INFO="$($PYTHON_BIN - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
plugin_key = os.environ["PLUGIN_KEY"]

package_path = root / "package.v2.json"
data = json.loads(package_path.read_text(encoding="utf-8"))
if plugin_key not in data:
    raise SystemExit(f"package.v2.json 中不存在插件键: {plugin_key}")

plugin_dir = root / "plugins.v2" / plugin_key.lower()
init_path = plugin_dir / "__init__.py"
if not init_path.exists():
    raise SystemExit(f"插件入口不存在: {init_path}")

print(plugin_dir.name)
PY
)"

PLUGIN_DIR_NAME="$PLUGIN_INFO"
ZIP_NAME="${PLUGIN_DIR_NAME}_v${VERSION}.zip"
ZIP_PATH="/tmp/${ZIP_NAME}"
TAG_NAME="${PLUGIN_KEY}_v${VERSION}"
RELEASE_TITLE="$TAG_NAME"
REPO_SLUG="$(git remote get-url origin | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"

validate_clean_scope "$PLUGIN_DIR_NAME"
ensure_version_not_exists "$TAG_NAME" "$REPO_SLUG"
compile_plugin_python "$PLUGIN_DIR_NAME"

if [[ -n "$(git status --porcelain -- "plugins.v2/$PLUGIN_DIR_NAME")" ]]; then
  git add -A "plugins.v2/$PLUGIN_DIR_NAME"
  if ! git diff --cached --quiet; then
    commit_with_trailer "chore(${PLUGIN_DIR_NAME}): prepare release v${VERSION}"
  fi
fi

if [[ -n "$(git status --porcelain)" ]]; then
  validate_clean_scope "$PLUGIN_DIR_NAME"
fi

$PYTHON_BIN - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["ROOT_DIR"])
plugin_key = os.environ["PLUGIN_KEY"]
version = os.environ["VERSION"]
notes = os.environ["NOTES"]

package_path = root / "package.v2.json"
data = json.loads(package_path.read_text(encoding="utf-8"))
entry = data[plugin_key]
entry["version"] = version
entry["release"] = True
history = entry.setdefault("history", {})
new_history = {f"v{version}": notes}
for key, value in history.items():
    if key != f"v{version}":
        new_history[key] = value
entry["history"] = new_history
package_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

plugin_dir = root / "plugins.v2" / plugin_key.lower()
init_path = plugin_dir / "__init__.py"
text = init_path.read_text(encoding="utf-8")
needle = 'plugin_version = "'
start = text.find(needle)
if start == -1:
    raise SystemExit(f"未在 {init_path} 中找到 plugin_version")
value_start = start + len(needle)
value_end = text.find('"', value_start)
if value_end == -1:
    raise SystemExit(f"未能解析 {init_path} 中的 plugin_version")
text = text[:value_start] + version + text[value_end:]
init_path.write_text(text, encoding="utf-8")
PY

"$PYTHON_BIN" -m py_compile "plugins.v2/$PLUGIN_DIR_NAME/__init__.py"

git add package.v2.json "plugins.v2/$PLUGIN_DIR_NAME/__init__.py"
if git diff --cached --quiet; then
  die "未检测到版本发布所需改动，已取消发布"
fi
commit_with_trailer "release(${PLUGIN_DIR_NAME}): bump to v${VERSION}"

rm -f "$ZIP_PATH"
(
  cd "plugins.v2/$PLUGIN_DIR_NAME"
  zip -r "$ZIP_PATH" . -x '__pycache__/*' '*.pyc' >/dev/null
)

[[ -f "$ZIP_PATH" ]] || die "zip 资产生成失败: $ZIP_PATH"

git push origin main
git tag "$TAG_NAME"
git push origin "refs/tags/$TAG_NAME"

gh release create "$TAG_NAME" "$ZIP_PATH" --repo "$REPO_SLUG" --verify-tag --title "$RELEASE_TITLE" --notes "$NOTES" >/dev/null

if [[ -n "$(git status --porcelain)" ]]; then
  die "发布完成后工作区不干净，请检查异常状态"
fi

echo "已发布: $PLUGIN_KEY v$VERSION"
echo "Tag: $TAG_NAME"
echo "Asset: $ZIP_PATH"
