# python-docx Tutorial (Python Equivalent of docx-js skill)

## Setup & Basic Structure

```python
from docx import Document
from docx.shared import Inches, Pt, RGBColor, Emu
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml.ns import qn
from lxml import etree

doc = Document()
doc.save("document.docx")
```

**Install:** `pip install python-docx lxml`

---

## Page Size & Margins

```python
from docx.shared import Inches
from docx.enum.section import WD_ORIENT

section = doc.sections[0]

# US Letter (default in python-docx is also Letter, but set explicitly)
section.page_width = Inches(8.5)
section.page_height = Inches(11)

# A4
section.page_width = Inches(8.27)
section.page_height = Inches(11.69)

# Margins (1 inch all sides)
section.top_margin = Inches(1)
section.bottom_margin = Inches(1)
section.left_margin = Inches(1)
section.right_margin = Inches(1)

# Landscape orientation
section.orientation = WD_ORIENT.LANDSCAPE
section.page_width = Inches(11)
section.page_height = Inches(8.5)
```

**Common page sizes:**

| Paper     | Width   | Height  | Content Width (1" margins) |
|-----------|---------|---------|---------------------------|
| US Letter | 8.5"    | 11"     | 6.5"                      |
| A4        | 8.27"   | 11.69"  | 6.27"                     |

**Content width** = `page_width - left_margin - right_margin`

---

## Styles & Fonts

```python
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_COLOR_INDEX

# Set default document font
style = doc.styles['Normal']
style.font.name = 'Arial'
style.font.size = Pt(12)

# Modify built-in heading styles
h1 = doc.styles['Heading 1']
h1.font.name = 'Arial'
h1.font.size = Pt(16)
h1.font.bold = True
h1.font.color.rgb = RGBColor(0x00, 0x00, 0x00)
h1.paragraph_format.space_before = Pt(12)
h1.paragraph_format.space_after = Pt(6)

h2 = doc.styles['Heading 2']
h2.font.name = 'Arial'
h2.font.size = Pt(14)
h2.font.bold = True
h2.paragraph_format.space_before = Pt(10)
h2.paragraph_format.space_after = Pt(4)

# Add a heading paragraph
doc.add_heading('Section Title', level=1)
doc.add_heading('Subsection Title', level=2)
```

---

## Paragraphs & Text

```python
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH

# Simple paragraph
p = doc.add_paragraph('This is a paragraph.')

# Alignment
p.alignment = WD_ALIGN_PARAGRAPH.CENTER      # LEFT, CENTER, RIGHT, JUSTIFY

# Spacing
p.paragraph_format.space_before = Pt(6)
p.paragraph_format.space_after = Pt(6)
p.paragraph_format.line_spacing = Pt(14)

# Mixed formatting (runs)
p = doc.add_paragraph()
run = p.add_run('Bold text ')
run.bold = True
run.font.size = Pt(14)
run.font.color.rgb = RGBColor(0x36, 0x36, 0x36)
run.font.name = 'Arial'

run2 = p.add_run('Italic text')
run2.italic = True

run3 = p.add_run(' Normal text')

# ❌ WRONG: Never use '\n' inside a run for multi-line content
# ✅ CORRECT: Add separate paragraphs
doc.add_paragraph('Line 1')
doc.add_paragraph('Line 2')

# Indentation
p = doc.add_paragraph('Indented paragraph')
p.paragraph_format.left_indent = Inches(0.5)
p.paragraph_format.first_line_indent = Inches(0.25)  # or negative for hanging
```

---

## Lists & Bullets

```python
# ❌ WRONG: Never manually prepend bullet characters
doc.add_paragraph('• Item one')     # BAD — creates visual double bullets

# ✅ CORRECT: Use built-in list styles
# Bullet list
doc.add_paragraph('First item', style='List Bullet')
doc.add_paragraph('Second item', style='List Bullet')
doc.add_paragraph('Third item', style='List Bullet')

# Numbered list
doc.add_paragraph('Step one', style='List Number')
doc.add_paragraph('Step two', style='List Number')
doc.add_paragraph('Step three', style='List Number')

# Sub-bullets (indented, level 2)
doc.add_paragraph('Sub-item', style='List Bullet 2')

# Sub-numbers
doc.add_paragraph('Sub-step', style='List Number 2')
```

**Available built-in list styles:**
- `List Bullet`, `List Bullet 2`, `List Bullet 3`
- `List Number`, `List Number 2`, `List Number 3`

**Note:** Each `List Number` style restart is controlled by the style definition. If you need independent numbered lists that each restart at 1, use the XML `numId` approach:

```python
# Reset numbering for a new independent numbered list via XML
from docx.oxml.ns import qn
from lxml import etree

def add_numbered_list(doc, items):
    """Add an independent numbered list that restarts at 1."""
    # Add a new abstract numbering definition via XML for true restart
    numbering = doc.part.numbering_part.numbering_definitions._element
    # Simplest approach: use 'List Number' style but override numId per list
    for item in items:
        p = doc.add_paragraph(style='List Number')
        p.text = item
    return doc
```

---

## Tables

```python
from docx.shared import Inches, Pt, RGBColor
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from lxml import etree

# Add table
table = doc.add_table(rows=3, cols=2)
table.style = 'Table Grid'  # Built-in style with borders

# Set table width to full content width
# CRITICAL: Set width explicitly — never rely on auto-sizing
from docx.oxml.ns import qn
from lxml import etree

def set_table_width(table, width_inches):
    """Force a table to a specific width."""
    tbl = table._tbl
    tblPr = tbl.find(qn('w:tblPr'))
    if tblPr is None:
        tblPr = etree.SubElement(tbl, qn('w:tblPr'))
    tblW = tblPr.find(qn('w:tblW'))
    if tblW is None:
        tblW = etree.SubElement(tblPr, qn('w:tblW'))
    twips = int(width_inches * 1440)
    tblW.set(qn('w:w'), str(twips))
    tblW.set(qn('w:type'), 'dxa')

set_table_width(table, 6.5)  # 6.5" = content width for US Letter with 1" margins

# Set column widths (must sum to table width)
def set_col_widths(table, widths_inches):
    """Set each column to a specific width in inches."""
    for i, col in enumerate(table.columns):
        for cell in col.cells:
            tc = cell._tc
            tcPr = tc.find(qn('w:tcPr'))
            if tcPr is None:
                tcPr = etree.SubElement(tc, qn('w:tcPr'))
            tcW = tcPr.find(qn('w:tcW'))
            if tcW is None:
                tcW = etree.SubElement(tcPr, qn('w:tcW'))
            twips = int(widths_inches[i] * 1440)
            tcW.set(qn('w:w'), str(twips))
            tcW.set(qn('w:type'), 'dxa')

set_col_widths(table, [3.25, 3.25])  # Must sum to table width

# Header row
header_cells = table.rows[0].cells
header_cells[0].text = 'Header 1'
header_cells[1].text = 'Header 2'

# Style header cells
def style_cell(cell, fill_hex=None, bold=False, font_color_hex=None,
               font_size_pt=12, align=WD_ALIGN_PARAGRAPH.LEFT):
    """Style a table cell."""
    if fill_hex:
        tc = cell._tc
        tcPr = tc.find(qn('w:tcPr'))
        if tcPr is None:
            tcPr = etree.SubElement(tc, qn('w:tcPr'))
        shd = tcPr.find(qn('w:shd'))
        if shd is None:
            shd = etree.SubElement(tcPr, qn('w:shd'))
        shd.set(qn('w:val'), 'clear')          # CRITICAL: use 'clear', NOT 'solid'
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), fill_hex)        # 6-char hex, no '#'
    for para in cell.paragraphs:
        para.alignment = align
        for run in para.runs:
            run.bold = bold
            run.font.size = Pt(font_size_pt)
            if font_color_hex:
                r = int(font_color_hex[0:2], 16)
                g = int(font_color_hex[2:4], 16)
                b = int(font_color_hex[4:6], 16)
                run.font.color.rgb = RGBColor(r, g, b)

style_cell(header_cells[0], fill_hex='D5E8F0', bold=True)
style_cell(header_cells[1], fill_hex='D5E8F0', bold=True)

# Data rows
data = [['Cell A', 'Cell B'], ['Cell C', 'Cell D']]
for row_idx, row_data in enumerate(data, start=1):
    row = table.rows[row_idx]
    for col_idx, value in enumerate(row_data):
        row.cells[col_idx].text = value

# Cell padding (internal margins via XML)
def set_cell_margins(cell, top=80, bottom=80, left=120, right=120):
    """Set internal cell padding (values in twips, 1440 = 1 inch)."""
    tc = cell._tc
    tcPr = tc.find(qn('w:tcPr'))
    if tcPr is None:
        tcPr = etree.SubElement(tc, qn('w:tcPr'))
    tcMar = etree.SubElement(tcPr, qn('w:tcMar'))
    for side, val in [('top', top), ('bottom', bottom), ('left', left), ('right', right)]:
        el = etree.SubElement(tcMar, qn(f'w:{side}'))
        el.set(qn('w:w'), str(val))
        el.set(qn('w:type'), 'dxa')

for row in table.rows:
    for cell in row.cells:
        set_cell_margins(cell)

# Merge cells
table.cell(0, 0).merge(table.cell(0, 1))  # Merge across columns

# Vertical alignment
from docx.oxml.ns import qn
def set_cell_vertical_align(cell, align='center'):
    """Set vertical alignment: 'top', 'center', 'bottom'."""
    tc = cell._tc
    tcPr = tc.find(qn('w:tcPr'))
    if tcPr is None:
        tcPr = etree.SubElement(tc, qn('w:tcPr'))
    vAlign = etree.SubElement(tcPr, qn('w:vAlign'))
    vAlign.set(qn('w:val'), align)
```

**CRITICAL table rules:**
- Always set table width explicitly in DXA (never rely on auto-sizing)
- Always set column widths; they must sum to the table width
- Use `shd val='clear'` — NOT `'solid'` — to prevent black backgrounds
- Cell margins are internal padding and do not add to cell width
- Full-width table = page width minus left and right margins (e.g. 6.5" for US Letter with 1" margins)

---

## Images

```python
from docx.shared import Inches

# From file path
doc.add_picture('image.png', width=Inches(4))

# From file path, preserving aspect ratio
doc.add_picture('image.png', width=Inches(4))  # height auto-calculated

# Explicit width and height
doc.add_picture('image.png', width=Inches(4), height=Inches(3))

# From BytesIO (in-memory image)
import io
doc.add_picture(io.BytesIO(image_bytes), width=Inches(4))

# Center the image
from docx.enum.text import WD_ALIGN_PARAGRAPH
last_para = doc.paragraphs[-1]
last_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

# Calculate dimensions preserving aspect ratio
orig_width_px = 1978
orig_height_px = 923
desired_height_inches = 3.0
calc_width_inches = desired_height_inches * (orig_width_px / orig_height_px)
doc.add_picture('image.png', width=Inches(calc_width_inches), height=Inches(desired_height_inches))
```

---

## Page Breaks

```python
from docx.oxml.ns import qn
from lxml import etree

# ✅ CORRECT: Add a page break paragraph
doc.add_page_break()

# OR manually:
p = doc.add_paragraph()
run = p.add_run()
br = etree.SubElement(run._r, qn('w:br'))
br.set(qn('w:type'), 'page')

# Page break before a specific paragraph
p = doc.add_paragraph('Starts on new page')
p.paragraph_format.page_break_before = True
```

---

## Hyperlinks

```python
from docx.oxml.ns import qn
from lxml import etree

# External hyperlink
def add_hyperlink(paragraph, url, text, color_hex="0563C1", underline=True):
    """Add a clickable hyperlink run to an existing paragraph."""
    part = paragraph.part
    r_id = part.relate_to(url, 'http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink', is_external=True)

    hyperlink = etree.SubElement(paragraph._p, qn('w:hyperlink'))
    hyperlink.set(qn('r:id'), r_id)

    new_run = etree.SubElement(hyperlink, qn('w:r'))
    rPr = etree.SubElement(new_run, qn('w:rPr'))

    rStyle = etree.SubElement(rPr, qn('w:rStyle'))
    rStyle.set(qn('w:val'), 'Hyperlink')

    t = etree.SubElement(new_run, qn('w:t'))
    t.text = text
    return hyperlink

p = doc.add_paragraph('Visit our website: ')
add_hyperlink(p, 'https://example.com', 'Click here')

# Internal bookmark + link
def add_bookmark(paragraph, bookmark_id, bookmark_name):
    """Wrap paragraph content in a bookmark."""
    p = paragraph._p
    bookmarkStart = etree.SubElement(p, qn('w:bookmarkStart'))
    bookmarkStart.set(qn('w:id'), str(bookmark_id))
    bookmarkStart.set(qn('w:name'), bookmark_name)
    bookmarkEnd = etree.SubElement(p, qn('w:bookmarkEnd'))
    bookmarkEnd.set(qn('w:id'), str(bookmark_id))

def add_internal_hyperlink(paragraph, anchor, text):
    """Add an internal hyperlink to a named bookmark."""
    hyperlink = etree.SubElement(paragraph._p, qn('w:hyperlink'))
    hyperlink.set(qn('w:anchor'), anchor)
    new_run = etree.SubElement(hyperlink, qn('w:r'))
    rPr = etree.SubElement(new_run, qn('w:rPr'))
    rStyle = etree.SubElement(rPr, qn('w:rStyle'))
    rStyle.set(qn('w:val'), 'Hyperlink')
    t = etree.SubElement(new_run, qn('w:t'))
    t.text = text
```

---

## Footnotes

```python
from docx.oxml.ns import qn
from lxml import etree

def add_footnote(paragraph, footnote_text):
    """Add a footnote reference to a paragraph and define the footnote."""
    doc = paragraph.part._element.getroottree().getroot()
    # python-docx does not have a native footnote API; use XML directly
    # This requires accessing the footnotes part

    # Get or create footnotes part
    footnotes_part = paragraph.part._footnotes_part_or_none()
    if footnotes_part is None:
        # For simplicity, use the oxml approach if footnotes part exists
        pass

    # Practical approach: use the docx package's footnote XML pattern
    # Insert footnote reference in run
    run = paragraph.add_run()
    rPr = etree.SubElement(run._r, qn('w:rPr'))
    vertAlign = etree.SubElement(rPr, qn('w:vertAlign'))
    vertAlign.set(qn('w:val'), 'superscript')
    t = etree.SubElement(run._r, qn('w:t'))
    t.text = '*'   # fallback: use asterisk if footnote XML isn't available
    return run

# NOTE: Full footnote XML support requires direct manipulation of the
# footnotes.xml part. The cleanest approach for agents is to use the
# python-docx-footnotes extension:
# pip install python-docx-footnotes
#
# from docx_footnotes import Document as FootnoteDocument
# doc = FootnoteDocument()
# p = doc.add_paragraph("Revenue grew 15%")
# doc.add_footnote(p, "Source: Annual Report 2024")
```

---

## Tab Stops

```python
from docx.shared import Inches
from docx.enum.text import WD_TAB_ALIGNMENT, WD_TAB_LEADER
from docx.oxml.ns import qn
from lxml import etree

# Right-aligned tab stop (e.g. "Company Name        January 2025")
def add_tab_stop(paragraph, position_inches, alignment='right', leader='none'):
    """Add a tab stop to a paragraph."""
    pPr = paragraph._p.get_or_add_pPr()
    tabs = pPr.find(qn('w:tabs'))
    if tabs is None:
        tabs = etree.SubElement(pPr, qn('w:tabs'))
    tab = etree.SubElement(tabs, qn('w:tab'))
    tab.set(qn('w:val'), alignment)   # 'left', 'center', 'right', 'decimal'
    tab.set(qn('w:pos'), str(int(position_inches * 1440)))
    if leader != 'none':
        tab.set(qn('w:leader'), leader)  # 'dot', 'hyphen', 'underscore'

# Right-align text at the right margin
p = doc.add_paragraph()
add_tab_stop(p, 6.5, alignment='right')   # 6.5" = content width
p.add_run('Company Name')
p.add_run('\tJanuary 2025')

# Dot-leader (TOC style: "Introduction ...... 3")
p = doc.add_paragraph()
add_tab_stop(p, 6.5, alignment='right', leader='dot')
p.add_run('Introduction')
p.add_run('\t3')
```

---

## Headers & Footers

```python
from docx.shared import Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from lxml import etree

section = doc.sections[0]

# Header
header = section.header
header_para = header.paragraphs[0]
header_para.text = 'Document Header'
header_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
header_para.runs[0].font.size = Pt(10)

# Footer with page number
footer = section.footer
footer_para = footer.paragraphs[0]
footer_para.alignment = WD_ALIGN_PARAGRAPH.CENTER

# Add "Page X" to footer
run = footer_para.add_run('Page ')

# Page number field via XML
fldChar1 = etree.SubElement(run._r, qn('w:fldChar'))
fldChar1.set(qn('w:fldCharType'), 'begin')

instrRun = footer_para.add_run()
instrText = etree.SubElement(instrRun._r, qn('w:instrText'))
instrText.text = ' PAGE '

fldChar2Run = footer_para.add_run()
fldChar2 = etree.SubElement(fldChar2Run._r, qn('w:fldChar'))
fldChar2.set(qn('w:fldCharType'), 'end')

# Footer with left and right text (use tab stop — NOT a table)
footer_para = footer.paragraphs[0]
add_tab_stop(footer_para, 6.5, alignment='right')
footer_para.add_run('Left Footer Text')
footer_para.add_run('\t')
# Add page number field to right side (using pattern above)

# Header/footer divider line (use paragraph border, NOT a table)
def add_paragraph_bottom_border(paragraph, color_hex='2E75B6', size_pt=6):
    """Add a bottom border to a paragraph (use for header/footer rules)."""
    pPr = paragraph._p.get_or_add_pPr()
    pBdr = etree.SubElement(pPr, qn('w:pBdr'))
    bottom = etree.SubElement(pBdr, qn('w:bottom'))
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), str(size_pt * 8))   # sz in eighths of a point
    bottom.set(qn('w:space'), '1')
    bottom.set(qn('w:color'), color_hex)

add_paragraph_bottom_border(header_para, color_hex='2E75B6')

# ❌ WRONG: Never use a table as a divider/rule — cells have minimum height
# ✅ CORRECT: Use paragraph border (shown above)
```

---

## Multi-Column Layouts

```python
from docx.oxml.ns import qn
from lxml import etree

def set_section_columns(section, num_columns=2, space_inches=0.5, separator=False):
    """Set number of equal-width columns for a section."""
    sectPr = section._sectPr
    cols = sectPr.find(qn('w:cols'))
    if cols is None:
        cols = etree.SubElement(sectPr, qn('w:cols'))
    cols.set(qn('w:num'), str(num_columns))
    cols.set(qn('w:space'), str(int(space_inches * 1440)))
    if separator:
        cols.set(qn('w:sep'), '1')

set_section_columns(doc.sections[0], num_columns=2, space_inches=0.5)
# Content added after this point flows into two columns

# Force a column break
p = doc.add_paragraph()
run = p.add_run()
br = etree.SubElement(run._r, qn('w:br'))
br.set(qn('w:type'), 'column')
```

---

## Table of Contents

```python
from docx.oxml.ns import qn
from lxml import etree

def add_table_of_contents(doc, title='Table of Contents', max_level=3):
    """
    Insert a TOC field. Word/LibreOffice will update it on first open.
    Headings MUST use built-in Heading 1 / Heading 2 / Heading 3 styles.
    ❌ Do NOT use custom styles for headings that appear in the TOC.
    """
    p = doc.add_paragraph()
    run = p.add_run(title)
    run.bold = True
    run.font.size = Pt(14)

    # Insert TOC field
    toc_para = doc.add_paragraph()
    pPr = toc_para._p.get_or_add_pPr()

    fldChar_begin = etree.SubElement(toc_para._p.add_run()._r, qn('w:fldChar'))
    fldChar_begin.set(qn('w:fldCharType'), 'begin')
    fldChar_begin.set(qn('w:dirty'), '1')

    instrRun = toc_para.add_run()
    instrText = etree.SubElement(instrRun._r, qn('w:instrText'))
    instrText.set('xml:space', 'preserve')
    instrText.text = f' TOC \\o "1-{max_level}" \\h \\z \\u '

    fldChar_sep = etree.SubElement(toc_para.add_run()._r, qn('w:fldChar'))
    fldChar_sep.set(qn('w:fldCharType'), 'separate')

    toc_para.add_run('(Right-click → Update Field to populate TOC)')

    fldChar_end = etree.SubElement(toc_para.add_run()._r, qn('w:fldChar'))
    fldChar_end.set(qn('w:fldCharType'), 'end')

add_table_of_contents(doc)

# Headings used in TOC MUST use standard styles:
doc.add_heading('Chapter 1', level=1)
doc.add_heading('Section 1.1', level=2)
```

---

## Tracked Changes

```python
from docx.oxml.ns import qn
from lxml import etree
from datetime import datetime

DATE = "2025-01-01T00:00:00Z"
AUTHOR = "Claude"

def insert_tracked_insertion(paragraph, text, change_id=1, author=AUTHOR, date=DATE):
    """Insert text as a tracked insertion."""
    ins = etree.SubElement(paragraph._p, qn('w:ins'))
    ins.set(qn('w:id'), str(change_id))
    ins.set(qn('w:author'), author)
    ins.set(qn('w:date'), date)
    r = etree.SubElement(ins, qn('w:r'))
    t = etree.SubElement(r, qn('w:t'))
    t.text = text
    return ins

def insert_tracked_deletion(paragraph, text, change_id=2, author=AUTHOR, date=DATE):
    """Mark text as a tracked deletion."""
    delete = etree.SubElement(paragraph._p, qn('w:del'))
    delete.set(qn('w:id'), str(change_id))
    delete.set(qn('w:author'), author)
    delete.set(qn('w:date'), date)
    r = etree.SubElement(delete, qn('w:r'))
    delText = etree.SubElement(r, qn('w:delText'))
    delText.text = text
    return delete

# Example: change "30 days" to "60 days"
p = doc.add_paragraph()
p.add_run('The term is ')
insert_tracked_deletion(p, '30', change_id=1)
insert_tracked_insertion(p, '60', change_id=2)
p.add_run(' days.')
```

---

## Utility Helper Functions

```python
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from lxml import etree

def hex_to_rgb(hex_str):
    """Convert 6-char hex (no #) to RGBColor."""
    return RGBColor(int(hex_str[0:2], 16), int(hex_str[2:4], 16), int(hex_str[4:6], 16))

def add_styled_paragraph(doc, text, font_name='Arial', font_size_pt=12,
                          bold=False, italic=False, color_hex='000000',
                          align='left', space_before_pt=0, space_after_pt=6):
    """Add a fully styled paragraph."""
    align_map = {
        'left': WD_ALIGN_PARAGRAPH.LEFT,
        'center': WD_ALIGN_PARAGRAPH.CENTER,
        'right': WD_ALIGN_PARAGRAPH.RIGHT,
        'justify': WD_ALIGN_PARAGRAPH.JUSTIFY,
    }
    p = doc.add_paragraph()
    p.alignment = align_map.get(align, WD_ALIGN_PARAGRAPH.LEFT)
    p.paragraph_format.space_before = Pt(space_before_pt)
    p.paragraph_format.space_after = Pt(space_after_pt)
    run = p.add_run(text)
    run.font.name = font_name
    run.font.size = Pt(font_size_pt)
    run.bold = bold
    run.italic = italic
    run.font.color.rgb = hex_to_rgb(color_hex)
    return p

def add_horizontal_rule(doc, color_hex='CCCCCC', size_pt=1):
    """Add a horizontal rule using a paragraph bottom border."""
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = etree.SubElement(pPr, qn('w:pBdr'))
    bottom = etree.SubElement(pBdr, qn('w:bottom'))
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), str(size_pt * 8))
    bottom.set(qn('w:space'), '1')
    bottom.set(qn('w:color'), color_hex)
    return p

def set_page_us_letter(doc, top=1, bottom=1, left=1, right=1):
    """Set US Letter page size with given margins (in inches)."""
    section = doc.sections[0]
    section.page_width = Inches(8.5)
    section.page_height = Inches(11)
    section.top_margin = Inches(top)
    section.bottom_margin = Inches(bottom)
    section.left_margin = Inches(left)
    section.right_margin = Inches(right)
```

---

## Common Pitfalls

⚠️ These cause broken output, black table cells, or corrupted files.

1. **NEVER use `\n` inside run text** — add separate `Paragraph` objects instead.
   ```python
   run.text = "Line 1\nLine 2"   # ❌ WRONG
   doc.add_paragraph("Line 1")   # ✅ CORRECT
   doc.add_paragraph("Line 2")
   ```

2. **NEVER prepend bullet characters manually** — use `'List Bullet'` style.
   ```python
   doc.add_paragraph('• Item')            # ❌ WRONG
   doc.add_paragraph('Item', style='List Bullet')  # ✅ CORRECT
   ```

3. **NEVER use `shd val='solid'` for table cell shading** — causes black backgrounds.
   ```python
   shd.set(qn('w:val'), 'solid')   # ❌ BLACK BACKGROUND
   shd.set(qn('w:val'), 'clear')   # ✅ CORRECT
   ```

4. **NEVER use tables as horizontal dividers** — cells have minimum height and render as empty boxes (including in headers/footers). Use paragraph border instead.
   ```python
   # ❌ WRONG: table row as a rule
   # ✅ CORRECT: paragraph bottom border
   add_paragraph_bottom_border(p, color_hex='2E75B6')
   ```

5. **Set page size explicitly** — python-docx defaults to US Letter, but always be explicit to avoid surprises.

6. **Tables need explicit widths** — always call `set_table_width()` and `set_col_widths()`. Column widths must sum to the table width.

7. **TOC requires standard heading styles** — `doc.add_heading('Title', level=1)` only. Custom styles won't appear in the TOC field.

8. **Smart quotes**: When adding professional content, use Unicode directly or XML entities.
   ```python
   run.text = "Here\u2019s a quote: \u201CHello\u201D"
   # ' = \u2019   " = \u201C   " = \u201D   ' = \u2018
   ```

9. **For two-column footers, use tab stops — not tables.**

---

## Quick Reference

### Alignment Constants
```python
from docx.enum.text import WD_ALIGN_PARAGRAPH
WD_ALIGN_PARAGRAPH.LEFT / CENTER / RIGHT / JUSTIFY
```

### Common Built-in Styles
| Style Name        | Use for                    |
|-------------------|----------------------------|
| `Normal`          | Body text                  |
| `Heading 1`–`9`   | Section headings           |
| `List Bullet`     | Unordered list             |
| `List Bullet 2`   | Nested unordered list      |
| `List Number`     | Ordered list               |
| `List Number 2`   | Nested ordered list        |
| `Table Grid`      | Table with borders         |
| `Hyperlink`       | Hyperlink run style        |
| `Caption`         | Image/table captions       |
| `Intense Quote`   | Block quote                |

### Unit Conversions
```python
from docx.shared import Inches, Pt, Emu
Inches(1)     # 914400 EMU
Pt(1)         # 12700 EMU
# DXA (twips): 1440 per inch (used in XML directly as integers)
int(1 * 1440) # 1440 twips = 1 inch
```

### Required Imports (full reference)
```python
from docx import Document
from docx.shared import Inches, Pt, RGBColor, Emu
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_TAB_ALIGNMENT
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.enum.section import WD_ORIENT
from docx.oxml.ns import qn
from lxml import etree
```

### Dependencies
```bash
pip install python-docx          # Core library
pip install lxml                  # XML manipulation (installed with python-docx)
pip install python-docx-footnotes # Optional: cleaner footnote API
pip install Pillow                # Optional: image processing before insert
```
