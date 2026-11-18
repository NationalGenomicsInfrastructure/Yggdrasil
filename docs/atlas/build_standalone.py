"""Build the standalone Yggdrasil Atlas from the artifact source.

The artifact source (``Yggdrasil_Atlas.html``) is a page body: the artifact
host adds the document shell around it and it loads its fonts from Google
Fonts. This script turns it into one self-contained HTML document that makes
no network request, for embedding in Bifrost, the documentation site, or for
opening from disk.

It changes three things and nothing else:

1. Wraps the page in a document shell (doctype, head, body) with the small
   reset the artifact host would otherwise provide.
2. Replaces the Google Fonts stylesheet with the same font files, embedded.
3. Adds a theme bridge, so a host page can tell the Atlas which theme to use.

Usage::

    python3 build_standalone.py [SOURCE] [OUTPUT] [FONTS_DIR]

All three default to the files beside this script.
"""

from __future__ import annotations

import base64
import pathlib
import re
import sys

HERE = pathlib.Path(__file__).resolve().parent

# (family, CSS font-weight, file). IBM Plex Sans is one variable font file.
FONTS: list[tuple[str, str, str]] = [
    ("Chakra Petch", "500", "ChakraPetch-500.woff2"),
    ("Chakra Petch", "600", "ChakraPetch-600.woff2"),
    ("Chakra Petch", "700", "ChakraPetch-700.woff2"),
    ("IBM Plex Mono", "400", "IBMPlexMono-400.woff2"),
    ("IBM Plex Mono", "500", "IBMPlexMono-500.woff2"),
    ("IBM Plex Mono", "600", "IBMPlexMono-600.woff2"),
    ("IBM Plex Sans", "400 600", "IBMPlexSans-variable.woff2"),
]

BODY_START = '<div class="wrap">'
FONT_LINK = re.compile(
    r'<link rel="stylesheet" href="https://fonts\.googleapis\.com/[^"]*">\n?'
)
HOST_META = re.compile(r'<meta name="claude:viewport-fit-sides"[^>]*>\n?')

NOTES = """<!--
Yggdrasil Atlas, standalone build. Self-contained: it makes no network request.
Built by build_standalone.py from Yggdrasil_Atlas.html; edit that file, not this one.

Embed   <iframe src="atlas.html" title="Yggdrasil Atlas" allow="fullscreen"
                style="width:100%;height:100%;border:0"></iframe>
        Give the frame its own full-height area. The page scrolls inside it.

Theme   Follows the operating system unless told otherwise.
        At load:      atlas.html?theme=dark        (dark, light or auto)
        While open:   frame.contentWindow.postMessage(
                          {type: "yggdrasil-atlas:theme", theme: "dark"},
                          location.origin)
        Messages are accepted from the page's own origin only.

Fonts   Chakra Petch (The Chakra Petch Project Authors) and IBM Plex Sans and
        IBM Plex Mono (IBM Corp.), Latin subsets, embedded under the SIL Open
        Font License 1.1.
-->"""

RESET = (
    "<style>:root{color-scheme:light}body{margin:0}img{max-width:100%}"
    "[hidden]{display:none!important}</style>"
)

BRIDGE = """<script>
(function(){
  var root=document.documentElement;
  function set(t){
    if(t==='dark'||t==='light'){root.setAttribute('data-theme',t);}
    else if(t==='auto'){root.removeAttribute('data-theme');}
  }
  var m=/[?&]theme=(dark|light|auto)(?:&|$)/.exec(location.search);
  if(m){set(m[1]);}
  window.addEventListener('message',function(e){
    if(e.origin!==location.origin){return;}
    var d=e.data;
    if(d&&d.type==='yggdrasil-atlas:theme'){set(d.theme);}
  });
})();
</script>"""


def font_css(fonts_dir: pathlib.Path) -> str:
    """Return @font-face rules with every font file embedded as a data URI.

    Args:
        fonts_dir: Directory holding the woff2 files named in ``FONTS``.

    Returns:
        str: A ``<style>`` element.

    Raises:
        FileNotFoundError: If a font file is missing.
        ValueError: If a file is not a woff2 font.
    """
    rules = []
    for family, weight, name in FONTS:
        data = (fonts_dir / name).read_bytes()
        if data[:4] != b"wOF2":
            raise ValueError(f"{name} is not a woff2 font")
        encoded = base64.b64encode(data).decode("ascii")
        rules.append(
            f'@font-face{{font-family:"{family}";font-style:normal;'
            f"font-weight:{weight};font-display:swap;"
            f'src:url(data:font/woff2;base64,{encoded}) format("woff2")}}'
        )
    return "<style>\n" + "\n".join(rules) + "\n</style>"


def build(source: pathlib.Path, output: pathlib.Path, fonts_dir: pathlib.Path) -> None:
    """Write the standalone document.

    Args:
        source: The artifact source, a page body.
        output: Where to write the standalone document.
        fonts_dir: Directory holding the font files.

    Raises:
        ValueError: If the source does not have the expected shape, so that a
            changed source fails loudly instead of producing a broken page.
    """
    text = source.read_text(encoding="utf-8")
    if text.count(BODY_START) != 1:
        raise ValueError(f"expected exactly one {BODY_START!r} in {source.name}")
    head, body = text.split(BODY_START, 1)
    body = BODY_START + body

    title = re.search(r"<title>.*?</title>", head, re.S)
    if title is None:
        raise ValueError("the source has no <title>")
    head = head.replace(title.group(0), "", 1)
    head, links = FONT_LINK.subn("", head)
    if links != 1:
        raise ValueError(f"expected one Google Fonts stylesheet link, found {links}")
    head = HOST_META.sub("", head).strip()
    if not (head.startswith("<style>") and head.endswith("</style>")):
        raise ValueError("expected only a <style> block to remain in the source head")

    document = "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">',
            title.group(0),
            NOTES,
            RESET,
            font_css(fonts_dir),
            head,
            BRIDGE,
            "</head>",
            "<body>",
            body.rstrip(),
            "</body>",
            "</html>",
            "",
        ]
    )

    external = sorted(set(re.findall(r"https?://[^\s\"')<>]+", document)))
    allowed = {"http://www.w3.org/2000/svg"}  # an XML namespace, never fetched
    unexpected = [url for url in external if url not in allowed]
    if unexpected:
        raise ValueError(f"the build still refers to: {unexpected}")

    output.write_text(document, encoding="utf-8")
    print(f"wrote {output} ({output.stat().st_size / 1024:.0f} KB)")


def main(argv: list[str]) -> None:
    """Run the build from the command line."""
    source = pathlib.Path(argv[1]) if len(argv) > 1 else HERE / "Yggdrasil_Atlas.html"
    output = (
        pathlib.Path(argv[2])
        if len(argv) > 2
        else HERE / "Yggdrasil_Atlas.standalone.html"
    )
    fonts_dir = pathlib.Path(argv[3]) if len(argv) > 3 else HERE / "fonts"
    build(source, output, fonts_dir)


if __name__ == "__main__":
    main(sys.argv)
