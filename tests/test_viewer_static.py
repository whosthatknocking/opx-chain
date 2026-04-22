"""Static viewer-template checks for critical DOM hooks."""

from html.parser import HTMLParser
from pathlib import Path


STATIC_INDEX_PATH = (
    Path(__file__).resolve().parent.parent / "opx_chain" / "viewer_static" / "index.html"
)


class IdCollector(HTMLParser):
    """Collect element IDs from the static viewer HTML."""

    def __init__(self):
        super().__init__()
        self.ids: set[str] = set()

    def handle_starttag(self, _tag, attrs):
        for key, value in attrs:
            if key == "id" and value:
                self.ids.add(value)


def test_viewer_index_contains_critical_dom_hooks():
    """Critical app.js DOM hooks should remain present in the static template."""
    parser = IdCollector()
    parser.feed(STATIC_INDEX_PATH.read_text(encoding="utf-8"))

    assert {
        "dataTable",
        "tableStatus",
        "positionsTab",
        "positionsDataTable",
        "positionsTableStatus",
        "positionsRowCount",
        "positionsPageSizeSelect",
        "positionsPrevPageButton",
        "positionsNextPageButton",
        "positionsPageInfo",
        "filterPopover",
        "filterPopoverTitle",
        "filterValueSearch",
        "filterMinValue",
        "filterMaxValue",
        "clearFilterButton",
        "rowModal",
        "summaryTab",
        "tableTab",
        "chainTab",
        "readmeTab",
        "themeToggle",
    }.issubset(parser.ids)
