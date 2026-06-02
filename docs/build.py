#!/usr/bin/env python3
# Copyright 2026 Wanjia He
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate standalone, styled HTML from the project Markdown docs.

Usage:

    pip install "markdown>=3.5" "pygments>=2.16"
    python docs/build.py

Renders FORMAT.md and DESIGN.md into docs/*.html (plus an index page). The
HTML is self-contained (CSS embedded) so it can be opened directly or hosted
as static files.
"""

import pathlib
import re

import markdown
from pygments.formatters import HtmlFormatter

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# source markdown -> (output html, page title)
PAGES = {
    "FORMAT.md": ("format.html", "Format Specification"),
    "DESIGN.md": ("design.html", "Design Notes"),
}

NAV = [
    ("index.html", "Home"),
    ("format.html", "Format Spec"),
    ("design.html", "Design Notes"),
    ("https://github.com/opheadacheh/turbodata", "GitHub"),
]

CSS = """
:root {
  --fg: #1f2328; --muted: #59636e; --bg: #ffffff; --soft: #f6f8fa;
  --border: #d1d9e0; --accent: #0969da; --code-bg: #f6f8fa;
}
* { box-sizing: border-box; }
body {
  margin: 0; color: var(--fg); background: var(--bg);
  font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
header.topbar {
  position: sticky; top: 0; z-index: 10; background: var(--bg);
  border-bottom: 1px solid var(--border); padding: 12px 24px;
  display: flex; align-items: center; gap: 20px;
}
header.topbar .brand { font-weight: 700; font-size: 18px; }
header.topbar nav { display: flex; gap: 16px; flex-wrap: wrap; }
.layout { display: flex; max-width: 1180px; margin: 0 auto; }
aside.toc {
  flex: 0 0 240px; padding: 28px 16px; position: sticky; top: 49px;
  align-self: flex-start; max-height: calc(100vh - 49px); overflow: auto;
  font-size: 14px; border-right: 1px solid var(--border);
}
aside.toc ul { list-style: none; padding-left: 14px; margin: 4px 0; }
aside.toc > ul { padding-left: 0; }
aside.toc a { color: var(--muted); display: block; padding: 2px 0; }
aside.toc a:hover { color: var(--accent); }
main { flex: 1 1 auto; min-width: 0; padding: 28px 36px; max-width: 820px; }
main h1 { font-size: 30px; padding-bottom: .3em; border-bottom: 1px solid var(--border); }
main h2 { font-size: 23px; margin-top: 2em; padding-bottom: .3em; border-bottom: 1px solid var(--border); }
main h3 { font-size: 18px; margin-top: 1.6em; }
code, pre, kbd { font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace; }
:not(pre) > code {
  background: var(--code-bg); padding: .15em .35em; border-radius: 5px; font-size: 85%;
}
pre {
  background: var(--code-bg); border: 1px solid var(--border); border-radius: 8px;
  padding: 14px 16px; overflow: auto; font-size: 85%; line-height: 1.45;
}
pre code { background: none; padding: 0; }
table { border-collapse: collapse; width: 100%; margin: 1em 0; display: block; overflow: auto; }
th, td { border: 1px solid var(--border); padding: 7px 13px; text-align: left; }
th { background: var(--soft); }
tr:nth-child(2n) td { background: var(--soft); }
blockquote {
  margin: 1em 0; padding: 0 1em; color: var(--muted);
  border-left: .25em solid var(--border);
}
.headerlink { visibility: hidden; margin-left: .35em; font-size: .8em; }
h1:hover .headerlink, h2:hover .headerlink, h3:hover .headerlink { visibility: visible; }
hr { border: 0; border-top: 1px solid var(--border); margin: 2em 0; }
footer.pagefoot { max-width: 1180px; margin: 0 auto; padding: 24px 36px; color: var(--muted); font-size: 14px; border-top: 1px solid var(--border); }
@media (max-width: 900px) { aside.toc { display: none; } main { padding: 20px; } }
"""

PYGMENTS_CSS = HtmlFormatter(style="default").get_style_defs(".codehilite")

TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} · turbodata</title>
<style>{css}</style>
<style>{pygments}</style>
</head>
<body>
<header class="topbar">
  <span class="brand"><a href="index.html">turbodata</a></span>
  <nav>{nav}</nav>
</header>
<div class="layout">
  <aside class="toc">{toc}</aside>
  <main>{content}</main>
</div>
<footer class="pagefoot">turbodata · Apache-2.0 · generated from Markdown by docs/build.py</footer>
</body>
</html>
"""


def nav_html(active: str) -> str:
    parts = []
    for href, label in NAV:
        cls = ' style="font-weight:700"' if href == active else ""
        parts.append(f'<a href="{href}"{cls}>{label}</a>')
    return "".join(parts)


def rewrite_links(html: str) -> str:
    html = re.sub(r'href="(?:\./)?FORMAT\.md', 'href="format.html', html)
    html = re.sub(r'href="(?:\./)?DESIGN\.md', 'href="design.html', html)
    html = re.sub(r'href="(?:\./)?README\.md', 'href="index.html', html)
    html = re.sub(r'href="\./(LICENSE|NOTICE)"', r'href="../\1"', html)
    return html


def render(md_text: str):
    md = markdown.Markdown(
        extensions=["extra", "codehilite", "toc", "sane_lists"],
        extension_configs={"codehilite": {"guess_lang": False, "css_class": "codehilite"}},
    )
    body = md.convert(md_text)
    return rewrite_links(body), md.toc


def write_page(out_name: str, title: str, body: str, toc: str):
    page = TEMPLATE.format(
        title=title, css=CSS, pygments=PYGMENTS_CSS,
        nav=nav_html(out_name), toc=toc, content=body,
    )
    (DOCS / out_name).write_text(page, encoding="utf-8")
    print(f"wrote docs/{out_name}")


INDEX_MD = """# turbodata documentation

**turbodata** is a container format for storing multi-topic, timestamped
messages, optimized for read-efficient random access over local disk and
object storage.

- **[Format Specification](format.html)** — the precise on-disk layout.
- **[Design Notes](design.html)** — why the format is shaped the way it is.

See the [GitHub repository](https://github.com/opheadacheh/turbodata) for the
SDKs (Go, Python, TypeScript) and usage examples.
"""


def main():
    DOCS.mkdir(exist_ok=True)
    for src, (out_name, title) in PAGES.items():
        body, toc = render((ROOT / src).read_text(encoding="utf-8"))
        write_page(out_name, title, body, toc)
    body, toc = render(INDEX_MD)
    write_page("index.html", "Documentation", body, toc)


if __name__ == "__main__":
    main()
