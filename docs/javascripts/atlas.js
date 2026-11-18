/* Keep the embedded Yggdrasil Atlas in the docs' colour scheme.
 *
 * The Atlas (docs/atlas/Yggdrasil_Atlas.standalone.html) follows the
 * operating system unless its host says otherwise: with a `theme` query
 * parameter when it loads, or with a same-origin message while it is open.
 * Material for MkDocs records its scheme as `data-md-color-scheme` on <body>,
 * where "slate" is dark.
 */
(function () {
  "use strict";

  var MESSAGE = "yggdrasil-atlas:theme";
  var observer = null;
  var observed = null;

  function theme() {
    return document.body.getAttribute("data-md-color-scheme") === "slate" ? "dark" : "light";
  }

  function frames() {
    return Array.prototype.slice.call(document.querySelectorAll("iframe.ygg-atlas__frame"));
  }

  function tell(frame) {
    try {
      if (frame.contentWindow) {
        frame.contentWindow.postMessage({ type: MESSAGE, theme: theme() }, location.origin);
      }
    } catch (error) {
      /* Without an origin, as when opened from disk, the Atlas follows the system. */
    }
  }

  function setUp() {
    var systemDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    frames().forEach(function (frame) {
      if (frame.getAttribute("data-atlas-theme-bridge")) {
        return;
      }
      frame.setAttribute("data-atlas-theme-bridge", "on");
      frame.addEventListener("load", function () {
        tell(frame);
      });
      if ((theme() === "dark") !== systemDark) {
        /* Start it in the docs' scheme instead of switching once it shows. */
        var url = new URL(frame.getAttribute("src"), location.href);
        url.searchParams.set("theme", theme());
        frame.src = url.href;
      } else {
        tell(frame);
      }
    });
    if (observed !== document.body) {
      if (observer) {
        observer.disconnect();
      }
      observer = new MutationObserver(function () {
        frames().forEach(tell);
      });
      observer.observe(document.body, { attributes: true, attributeFilter: ["data-md-color-scheme"] });
      observed = document.body;
    }
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(setUp);
  } else if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setUp);
  } else {
    setUp();
  }
})();
