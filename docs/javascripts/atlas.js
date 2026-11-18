/* The Yggdrasil Atlas (docs/architecture/atlas/index.html) is a page of its
 * own. This script does the two things it needs from the docs pages.
 */

/* Count opens of the Atlas in Google Analytics.
 *
 * The Atlas loads no analytics, so the docs record the click that opens it:
 * an `atlas_open` event whose `link_text` names the link used, "Sidebar",
 * "Home card", or the link's own text anywhere else. Opening the Atlas from a
 * bookmark or a shared address is not counted.
 *
 * Material for MkDocs loads Google Analytics (`extra.analytics` in mkdocs.yml)
 * and queues its commands in `window.dataLayer`. Without it nothing is sent.
 */
(function () {
  "use strict";

  var ATLAS = /\/architecture\/atlas\/(index\.html)?$/;

  function gtag() {
    window.dataLayer.push(arguments);
  }

  function count(event) {
    if (!window.dataLayer || (event.type === "auxclick" && event.button !== 1)) {
      return;
    }
    var link = event.target instanceof Element ? event.target.closest("a[href]") : null;
    if (!link) {
      return;
    }
    var url = new URL(link.href, location.href);
    if (url.origin !== location.origin || !ATLAS.test(url.pathname)) {
      return;
    }
    var where = link.closest(".md-nav")
      ? "Sidebar"
      : link.closest(".ygg-atlas-card")
        ? "Home card"
        : link.textContent.trim() || "Link";
    gtag("event", "atlas_open", { link_text: where });
  }

  /* Middle-clicks, which open the Atlas in a new tab, arrive as auxclick. */
  document.addEventListener("click", count);
  document.addEventListener("auxclick", count);
})();

/* Show a theme chosen in the Atlas after Back.
 *
 * The Atlas's theme switch saves its choice where Material keeps the docs'
 * own, so the next docs page loads with it. A page the browser brings back
 * from its back-forward cache still shows the scheme it had, so select the
 * saved one with Material's own palette control, as its keyboard handling
 * does.
 */
(function () {
  "use strict";

  window.addEventListener("pageshow", function (event) {
    if (!event.persisted || typeof __md_get !== "function") {
      return;
    }
    var saved = __md_get("__palette");
    var inputs = document.querySelectorAll("input[name='__palette']");
    var input = saved && inputs[saved.index];
    if (!input) {
      return;
    }
    /* Material shows the toggle of the palette in use and hides the others. */
    var label = input.nextElementSibling;
    if (label instanceof HTMLElement && !label.hidden) {
      return;
    }
    input.checked = false;
    input.click();
  });
})();
