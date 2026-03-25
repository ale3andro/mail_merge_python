## Mail Merge: ODS → ODT

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
