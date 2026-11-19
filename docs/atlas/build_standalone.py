"""Build the Yggdrasil Atlas page from its source.

The source (``Yggdrasil_Atlas.html``) is a page body: the artifact host adds
the document shell around it and it loads its fonts from Google Fonts. This
script turns it into one self-contained HTML document.

By default it writes the documentation site's Atlas page,
``docs/architecture/atlas/index.html``, which MkDocs publishes as it is. In
that build the Atlas logo and a "Docs" item in its menu lead to the
documentation home, the Atlas and the documentation share the reader's light
or dark choice, and the browser tab shows the documentation's icon, the only
file the page asks the server for.

``--plain`` writes a copy without those, for embedding elsewhere (such as
Bifrost) or opening from disk. A plain copy makes no network request.

Both builds change the source in only three ways:

1. Wrap the page in a document shell (doctype, head, body) with the small
   reset the artifact host would otherwise provide.
2. Replace the Google Fonts stylesheet with the same font files, embedded.
3. Add a theme bridge, so a host page can tell the Atlas which theme to use.

Usage::

    python3 build_standalone.py                  # the documentation page
    python3 build_standalone.py --plain OUTPUT   # a plain copy
"""

from __future__ import annotations

import argparse
import base64
import json
import pathlib
import re

HERE = pathlib.Path(__file__).resolve().parent
DOCS_PAGE = HERE.parent / "architecture" / "atlas" / "index.html"

# Both relative to the documentation page's address, .../architecture/atlas/.
DOCS_HOME = "../../"
DOCS_ICON = "../../assets/ygg_logo-dark_mode.png"  # theme.favicon in mkdocs.yml

DESCRIPTION = (
    "An interactive map of how Yggdrasil works. Follow one change from a watcher "
    "to execution, look inside the coordinator and the engine, and try approving "
    "and rerunning a sample plan."
)

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

# The parts of the source's header that the documentation build changes.
BRAND = '<a class="brand" href="#glance">'
MENU_END = '<a href="#rules">Rules</a>\n  </nav>'

FONT_NOTE = """Fonts   Chakra Petch (The Chakra Petch Project Authors) and IBM Plex Sans and
        IBM Plex Mono (IBM Corp.), Latin subsets, embedded under the SIL Open
        Font License 1.1."""

PLAIN_NOTES = f"""<!--
Yggdrasil Atlas, plain build. Self-contained: it makes no network request.
Built by build_standalone.py --plain from Yggdrasil_Atlas.html; edit that
file, not this one.

Embed   <iframe src="atlas.html" title="Yggdrasil Atlas" allow="fullscreen"
                style="width:100%;height:100%;border:0"></iframe>
        Give the frame its own full-height area. The page scrolls inside it.

Theme   Follows the operating system unless told otherwise. The switch in
        the header changes it for the visit.
        At load:      atlas.html?theme=dark        (dark, light or auto)
        While open:   frame.contentWindow.postMessage(
                          {{type: "yggdrasil-atlas:theme", theme: "dark"}},
                          location.origin)
        Messages are accepted from the page's own origin only.

{FONT_NOTE}
-->"""

DOCS_NOTES = f"""<!--
Yggdrasil Atlas, the documentation site's page. Built by
docs/atlas/build_standalone.py from docs/atlas/Yggdrasil_Atlas.html; edit that
file, not this one.

Theme   Uses the light or dark choice saved by the documentation site, or the
        operating system's when there is none, and the switch in the header
        saves its choice there too. ?theme=dark, light or auto in the address
        overrides both.

{FONT_NOTE} The licence texts are in docs/atlas/fonts/.
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

# Material for MkDocs saves the reader's choice as JSON under
# "<site path>.__palette"; "slate" is its dark scheme. Reading it again when
# the page comes back from the browser's back-forward cache, or when another
# tab changes it, keeps the Atlas in step with the documentation.
#
# The Atlas's theme switch saves its choice there too, as Material would:
# "index" is the choice's place in the palette list in mkdocs.yml (automatic,
# light, dark), which Material uses to pick it on the next page it loads, and
# "color" lets Material show it before its own script runs.
DOCS_THEME = """<script>
(function(){
  if(/[?&]theme=/.test(location.search)){return;}
  var root=document.documentElement;
  var key=new URL(__HOME__,location.href).pathname+'.__palette';
  function apply(){
    var color=null;
    try{color=(JSON.parse(localStorage.getItem(key))||{}).color||null;}catch(e){}
    if(!color||color.media==='(prefers-color-scheme)'){root.removeAttribute('data-theme');}
    else{root.setAttribute('data-theme',color.scheme==='slate'?'dark':'light');}
  }
  apply();
  window.addEventListener('storage',function(e){if(e.key===key){apply();}});
  window.addEventListener('pageshow',function(e){if(e.persisted){apply();}});
  window.addEventListener('yggdrasil-atlas:theme-change',function(e){
    var t=e.detail,index={auto:0,light:1,dark:2}[t];
    if(index===undefined){return;}
    var color=t==='auto'
      ?{media:'(prefers-color-scheme)',scheme:matchMedia('(prefers-color-scheme: light)').matches?'default':'slate'}
      :{media:'(prefers-color-scheme: '+t+')',scheme:t==='dark'?'slate':'default'};
    try{localStorage.setItem(key,JSON.stringify({index:index,color:color}));}catch(err){}
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


def replace_once(text: str, old: str, new: str, what: str) -> str:
    """Replace a fragment that must occur exactly once.

    Args:
        text: The text to change.
        old: The fragment to replace.
        new: Its replacement.
        what: A short name for the fragment, for the error message.

    Returns:
        str: The changed text.

    Raises:
        ValueError: If ``old`` does not occur exactly once, so that a changed
            source fails loudly instead of producing a broken page.
    """
    found = text.count(old)
    if found != 1:
        raise ValueError(f"expected {what} once in the source, found it {found} times")
    return text.replace(old, new)


def build(
    source: pathlib.Path,
    output: pathlib.Path,
    fonts_dir: pathlib.Path,
    *,
    docs: bool,
) -> None:
    """Write the Atlas page.

    Args:
        source: The artifact source, a page body.
        output: Where to write the page.
        fonts_dir: Directory holding the font files.
        docs: Whether to build the documentation site's page, with its links
            to the documentation, its shared theme and its tab icon.

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

    if docs:
        body = replace_once(
            body,
            BRAND,
            f'<a class="brand" href="{DOCS_HOME}" title="Yggdrasil documentation">',
            "the Atlas logo link",
        )
        body = replace_once(
            body,
            MENU_END,
            f'<a href="#rules">Rules</a>\n    <a href="{DOCS_HOME}">Docs</a>\n  </nav>',
            "the end of the Atlas menu",
        )

    lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">',
        title.group(0),
        f'<meta name="description" content="{DESCRIPTION}">',
    ]
    if docs:
        lines.append(f'<link rel="icon" href="{DOCS_ICON}">')
    lines += [
        DOCS_NOTES if docs else PLAIN_NOTES,
        RESET,
        font_css(fonts_dir),
        head,
        BRIDGE,
    ]
    if docs:
        lines.append(DOCS_THEME.replace("__HOME__", json.dumps(DOCS_HOME)))
    lines += ["</head>", "<body>", body.rstrip(), "</body>", "</html>", ""]
    document = "\n".join(lines)

    external = sorted(set(re.findall(r"https?://[^\s\"')<>]+", document)))
    allowed = {"http://www.w3.org/2000/svg"}  # an XML namespace, never fetched
    unexpected = [url for url in external if url not in allowed]
    if unexpected:
        raise ValueError(f"the build still refers to: {unexpected}")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(document, encoding="utf-8")
    print(f"wrote {output} ({output.stat().st_size / 1024:.0f} KB)")


def main(argv: list[str] | None = None) -> None:
    """Run the build from the command line.

    Args:
        argv: The arguments after the program name; ``None`` reads them from
            the command line.
    """
    parser = argparse.ArgumentParser(
        description="Build the Yggdrasil Atlas page from its source."
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=pathlib.Path,
        help="where to write the page (default: the documentation site's page)",
    )
    parser.add_argument(
        "--plain",
        action="store_true",
        help="leave out the documentation links, shared theme and tab icon",
    )
    parser.add_argument(
        "--source", type=pathlib.Path, default=HERE / "Yggdrasil_Atlas.html"
    )
    parser.add_argument("--fonts", type=pathlib.Path, default=HERE / "fonts")
    args = parser.parse_args(argv)
    if args.output is None and args.plain:
        parser.error("a plain build needs an output path")
    build(args.source, args.output or DOCS_PAGE, args.fonts, docs=not args.plain)


if __name__ == "__main__":
    main()
