#!/usr/bin/env python3
"""
Mail Merge: ODS → ODT
---------------------
Reads rows from an ODS spreadsheet and merges them into an ODT template.
Placeholders in the template use the form  {{column_header}}

Output modes (--mode):
  single   — one ODT file per recipient (default)
  combined — one ODT with all recipients, each separated by a page break

Usage:
    python3 mail_merge.py --template letter.odt --data contacts.ods
    python3 mail_merge.py --template letter.odt --data contacts.ods --mode combined
    python3 mail_merge.py --template letter.odt --data contacts.ods --output-dir ./out
    python3 mail_merge.py --template letter.odt --data contacts.ods --sheet "Sheet2"

Dependencies (install once):
    pip install odfpy
"""

import argparse
import os
import re
import sys
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    from odf.opendocument import load as odf_load
    from odf.table import Table, TableRow, TableCell
    from odf import teletype
except ImportError:
    print(
        "Missing dependency. Install it with:\n"
        "    pip install odfpy\n",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# ODS reader  (pure odfpy)
# ---------------------------------------------------------------------------

def _cell_value(cell) -> str:
    """Extract the string value from an ODS table cell."""
    val = cell.getAttribute("value")
    if val is not None:
        if val.endswith(".0") and val[:-2].lstrip("-").isdigit():
            val = val[:-2]
        return val
    return teletype.extractText(cell).strip()


def read_ods(path: str, sheet_name: str | None = None) -> tuple[list[str], list[dict]]:
    """Return (headers, rows) where rows is a list of dicts keyed by header."""
    doc = odf_load(path)
    sheets = doc.spreadsheet.getElementsByType(Table)

    if not sheets:
        sys.exit("No sheets found in the ODS file.")

    if sheet_name:
        sheet = next(
            (s for s in sheets if s.getAttribute("name") == sheet_name), None
        )
        if sheet is None:
            available = [s.getAttribute("name") for s in sheets]
            sys.exit(f"Sheet '{sheet_name}' not found. Available: {available}")
    else:
        sheet = sheets[0]

    all_rows = sheet.getElementsByType(TableRow)
    if not all_rows:
        sys.exit("The sheet is empty.")

    # Parse header row
    headers = []
    for cell in all_rows[0].getElementsByType(TableCell):
        repeat = min(int(cell.getAttribute("numbercolumnsrepeated") or 1), 256)
        val = _cell_value(cell)
        for _ in range(repeat):
            headers.append(val)

    while headers and headers[-1] == "":
        headers.pop()

    if not headers:
        sys.exit("The ODS sheet appears to have no header row.")

    num_cols = len(headers)

    # Parse data rows
    rows = []
    for row_el in all_rows[1:]:
        cells = []
        for cell in row_el.getElementsByType(TableCell):
            repeat = min(int(cell.getAttribute("numbercolumnsrepeated") or 1),
                         num_cols - len(cells))
            if repeat <= 0:
                break
            val = _cell_value(cell)
            for _ in range(repeat):
                cells.append(val)
                if len(cells) >= num_cols:
                    break

        while len(cells) < num_cols:
            cells.append("")

        row = dict(zip(headers, cells[:num_cols]))
        if all(v == "" for v in row.values()):
            continue
        rows.append(row)

    return headers, rows


# ---------------------------------------------------------------------------
# ODT placeholder replacement
# The core problem: LibreOffice splits {{1}} across multiple XML text runs,
# e.g.  <text:span>{{</text:span><text:span>1</text:span><text:span>}}</text:span>
# Strategy: concatenate all text within each paragraph, replace placeholders
# there, then rebuild the paragraph as a single clean text node, discarding
# the split runs (which only carried formatting that would be wrong anyway).
# ---------------------------------------------------------------------------

# Namespaces used in ODT content.xml
NS = {
    "text":  "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "office":"urn:oasis:names:tc:opendocument:xmlns:office:1.0",
}

# Register all ODT namespaces so ElementTree round-trips them intact
_ODT_NS = {
    "office":    "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text":      "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "style":     "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "table":     "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "draw":      "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
    "fo":        "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "xlink":     "http://www.w3.org/1999/xlink",
    "dc":        "http://purl.org/dc/elements/1.1/",
    "meta":      "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "number":    "urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0",
    "svg":       "urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0",
    "chart":     "urn:oasis:names:tc:opendocument:xmlns:chart:1.0",
    "dr3d":      "urn:oasis:names:tc:opendocument:xmlns:dr3d:1.0",
    "math":      "http://www.w3.org/1998/Math/MathML",
    "form":      "urn:oasis:names:tc:opendocument:xmlns:form:1.0",
    "script":    "urn:oasis:names:tc:opendocument:xmlns:script:1.0",
    "ooo":       "http://openoffice.org/2004/office",
    "ooow":      "http://openoffice.org/2004/writer",
    "oooc":      "http://openoffice.org/2004/calc",
    "dom":       "http://www.w3.org/2001/xml-events",
    "xforms":    "http://www.w3.org/2002/xforms",
    "xsd":       "http://www.w3.org/2001/XMLSchema",
    "xsi":       "http://www.w3.org/2001/XMLSchema-instance",
    "rpt":       "http://openoffice.org/2005/report",
    "of":        "urn:oasis:names:tc:opendocument:xmlns:of:1.2",
    "xhtml":     "http://www.w3.org/1999/xhtml",
    "grddl":     "http://www.w3.org/2003/g/data-view#",
    "tableooo":  "http://openoffice.org/2009/table",
    "drawooo":   "http://openoffice.org/2010/draw",
    "calcext":   "urn:org:documentfoundation:names:experimental:calc:xmlns:calcext:1.0",
    "loext":     "urn:org:documentfoundation:names:experimental:office:xmlns:loext:1.0",
    "field":     "urn:openoffice.org/names/experimental/ooo-ms-interop/xmlns/field/1.0",
    "formx":     "urn:openoffice.org/names/experimental/ooxml-odf-interop/xmlns/form/1.0",
    "css3t":     "http://www.w3.org/TR/css3-text/",
}

for _prefix, _uri in _ODT_NS.items():
    ET.register_namespace(_prefix, _uri)


def _iter_text(element) -> str:
    """Recursively collect all text content from an XML element."""
    parts = []
    if element.text:
        parts.append(element.text)
    for child in element:
        parts.append(_iter_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _replace_in_xml(xml_bytes: bytes, mapping: dict[str, str]) -> bytes:
    """
    Parse the XML, walk every text:p element, collect its full text,
    replace {{key}} placeholders, then rebuild the paragraph with a single
    text node — eliminating the split-run problem entirely.
    """
    xml_str = xml_bytes.decode("utf-8")

    # We operate on the raw string for simple cases first (e.g. styles.xml
    # which may not have split runs), then do the structural fix for content.xml.
    # Structural fix: use regex to collapse runs inside paragraphs.
    xml_str = _collapse_and_replace(xml_str, mapping)

    return xml_str.encode("utf-8")


def _collapse_and_replace(xml_str: str, mapping: dict[str, str]) -> str:
    """
    Two-pass replacement:
    Pass 1 – simple string replace (catches placeholders that weren't split).
    Pass 2 – regex that strips XML tags *between* {{ and }} to reassemble
              placeholders that LibreOffice split across runs.
    """
    # Pass 1: direct replacement
    for key, value in mapping.items():
        safe = _xml_escape(value)
        xml_str = xml_str.replace("{{" + key + "}}", safe)

    # Pass 2: heal split placeholders.
    # Match {{ ... }} where the content may have XML tags interspersed.
    # We strip the tags to recover the key, look it up, and replace the whole match.
    def replacer(m):
        raw = m.group(0)
        # Strip all XML tags to get the bare text between {{ and }}
        inner = re.sub(r"<[^>]+>", "", raw)
        # inner should now be something like "{{1}}" or "{{ 1 }}"
        key_match = re.match(r"\{\{\s*(.+?)\s*\}\}", inner)
        if key_match:
            key = key_match.group(1)
            if key in mapping:
                return _xml_escape(mapping[key])
        return raw  # no match — leave untouched

    # Pattern: {{ optionally followed by tags/text until }}
    # We limit lookahead to 500 chars to avoid catastrophic backtracking
    xml_str = re.sub(r"\{\{(?:[^}]|<[^>]+>){0,100}\}\}", replacer, xml_str)

    return xml_str


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&apos;")
    )


# ---------------------------------------------------------------------------
# ODT merge
# ---------------------------------------------------------------------------

def merge_odt(template_path: str, mapping: dict[str, str], output_path: str) -> None:
    """Copy the ODT zip, replacing placeholders in every XML member."""
    XML_MEMBERS = {
        "content.xml", "styles.xml", "meta.xml",
        "settings.xml", "manifest.rdf",
    }
    with zipfile.ZipFile(template_path, "r") as zin, \
         zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            name = item.filename.split("/")[-1]
            if name in XML_MEMBERS or item.filename.endswith(".xml"):
                data = _replace_in_xml(data, mapping)
            zout.writestr(item, data)


# ---------------------------------------------------------------------------
# Output filename builder
# ---------------------------------------------------------------------------

def build_output_path(output_dir: str, template_path: str,
                      row: dict, row_index: int) -> str:
    first_value = next(iter(row.values()), "").strip()
    safe = re.sub(r"[^\w\-]", "_", first_value)[:40] if first_value else f"row_{row_index + 1}"
    stem = Path(template_path).stem
    return os.path.join(output_dir, f"{stem}_{safe}.odt")


# ---------------------------------------------------------------------------
# Combined (multi-page) ODT output
# ---------------------------------------------------------------------------

def _extract_body_content(content_xml: bytes) -> str:
    """Return everything inside <office:text> … </office:text>."""
    text = content_xml.decode("utf-8")
    m = re.search(r"<office:text[^>]*>(.*)</office:text>", text, re.DOTALL)
    if not m:
        raise ValueError("Cannot find <office:text> in content.xml")
    return m.group(1)


def _replace_body_content(content_xml: bytes, new_body: str) -> bytes:
    """Swap the content inside <office:text> … </office:text>."""
    text = content_xml.decode("utf-8")
    text = re.sub(
        r"(<office:text[^>]*>).*?(</office:text>)",
        r"\g<1>" + new_body + r"\g<2>",
        text, flags=re.DOTALL
    )
    return text.encode("utf-8")


def _prepend_soft_page_break(body: str) -> str:
    """
    Insert <text:soft-page-break/> just before the first <text:p …> in body.
    This is the correct ODT way to force a page break without adding an empty
    paragraph — which would cause a blank page in LibreOffice.
    """
    soft = '<text:soft-page-break xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"/>'
    return re.sub(r"(<text:p[\s>])", soft + r"\1", body, count=1)


def merge_odt_combined(template_path: str, rows: list[dict], output_path: str) -> None:
    """
    Produce a single ODT with one copy of the template per row,
    separated by page breaks — no blank pages.
    """
    with zipfile.ZipFile(template_path, "r") as zin:
        template_files = {item.filename: zin.read(item.filename)
                          for item in zin.infolist()}

    base_content_xml = template_files["content.xml"]

    merged_body_parts = []
    for i, row in enumerate(rows):
        filled_xml = _replace_in_xml(base_content_xml, row)
        body = _extract_body_content(filled_xml)
        if i > 0:
            body = _prepend_soft_page_break(body)
        merged_body_parts.append(body)

    combined_body = "\n".join(merged_body_parts)
    combined_content_xml = _replace_body_content(base_content_xml, combined_body)
    template_files["content.xml"] = combined_content_xml

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for filename, data in template_files.items():
            zout.writestr(filename, data)


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Mail-merge an ODT template with rows from an ODS spreadsheet."
    )
    parser.add_argument("--template", "-t", required=True,
                        help="Path to the ODT template file")
    parser.add_argument("--data", "-d", required=True,
                        help="Path to the ODS data file")
    parser.add_argument("--output-dir", "-o", default=None,
                        help="Directory for output files (default: same dir as template)")
    parser.add_argument("--sheet", "-s", default=None,
                        help="Sheet name to read (default: first sheet)")
    parser.add_argument(
        "--mode", "-m",
        choices=["single", "combined"],
        default="single",
        help=(
            "Output mode: 'single' = one ODT per recipient (default), "
            "'combined' = one ODT with all recipients separated by page breaks"
        ),
    )
    args = parser.parse_args()

    if not os.path.isfile(args.template):
        sys.exit(f"Template not found: {args.template}")
    if not os.path.isfile(args.data):
        sys.exit(f"Data file not found: {args.data}")

    output_dir = args.output_dir or os.path.dirname(os.path.abspath(args.template))
    os.makedirs(output_dir, exist_ok=True)

    print(f"Reading data from : {args.data}")
    headers, rows = read_ods(args.data, sheet_name=args.sheet)
    if not rows:
        sys.exit("No data rows found in the ODS file.")

    print(f"Columns detected  : {headers}")
    print(f"Rows found        : {len(rows)}")
    print(f"Output mode       : {args.mode}")

    # Warn about unmatched placeholders
    with zipfile.ZipFile(args.template, "r") as z:
        content_xml = z.read("content.xml").decode("utf-8")
    stripped = re.sub(r"<[^>]+>", "", content_xml)
    found_placeholders = set(re.findall(r"\{\{([^}]+)\}\}", stripped))
    unknown = found_placeholders - set(str(h) for h in headers)
    if unknown:
        print(f"⚠️  Unmatched placeholder(s) in template: {unknown}", file=sys.stderr)

    stem = Path(args.template).stem

    if args.mode == "combined":
        out_path = os.path.join(output_dir, f"{stem}_combined.odt")
        merge_odt_combined(args.template, rows, out_path)
        print(f"\n✅  Done — combined file written to: {out_path}")

    else:  # single (default)
        for i, row in enumerate(rows):
            out_path = build_output_path(output_dir, args.template, row, i)
            merge_odt(args.template, row, out_path)
            preview = " | ".join(f"{k}={v}" for k, v in list(row.items())[:3])
            print(f"  [{i + 1}/{len(rows)}] {preview}  →  {Path(out_path).name}")
        print(f"\n✅  Done — {len(rows)} file(s) written to: {output_dir}")


if __name__ == "__main__":
    main()