# python-pptx Tutorial (Python Equivalent of pptxgenjs.md)

## Setup & Basic Structure

```python
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.dml import MSO_THEME_COLOR

prs = Presentation()
prs.slide_width = Inches(10)
prs.slide_height = Inches(5.625)  # 16x9

slide_layout = prs.slide_layouts[6]  # Blank layout
slide = prs.slides.add_slide(slide_layout)

prs.save("Presentation.pptx")
```

**Install:** `pip install python-pptx`

---

## Layout Dimensions

Slide dimensions (set via `prs.slide_width` / `prs.slide_height`):

| Layout      | Width     | Height    |
|-------------|-----------|-----------|
| 16x9        | 10"       | 5.625"    |
| 16x10       | 10"       | 6.25"     |
| 4x3         | 10"       | 7.5"      |
| Wide        | 13.3"     | 7.5"      |

```python
# 16x9 (default for modern decks)
prs.slide_width = Inches(10)
prs.slide_height = Inches(5.625)

# Wide
prs.slide_width = Inches(13.3)
prs.slide_height = Inches(7.5)
```

---

## Text & Formatting

```python
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

def hex_color(hex_str):
    """Convert 6-char hex string (no #) to RGBColor."""
    return RGBColor(int(hex_str[0:2], 16), int(hex_str[2:4], 16), int(hex_str[4:6], 16))

# Add a text box
txBox = slide.shapes.add_textbox(Inches(1), Inches(1), Inches(8), Inches(2))
tf = txBox.text_frame
tf.word_wrap = True

p = tf.paragraphs[0]
p.alignment = PP_ALIGN.CENTER

run = p.add_run()
run.text = "Hello World!"
run.font.size = Pt(36)
run.font.bold = True
run.font.color.rgb = hex_color("363636")
run.font.name = "Arial"

# Add additional paragraph
from pptx.oxml.ns import qn
from lxml import etree

p2 = tf.add_paragraph()
run2 = p2.add_run()
run2.text = "Subtitle text"
run2.font.size = Pt(18)
run2.font.italic = True

# Text box internal margin (set to 0 for precise alignment with shapes)
txBox.text_frame.margin_left = 0
txBox.text_frame.margin_right = 0
txBox.text_frame.margin_top = 0
txBox.text_frame.margin_bottom = 0
```

**Tip:** Text boxes have internal margin by default. Set all margins to `0` (or `Pt(0)`) when you need text to align precisely with shapes or icons at the same x-position.

### Character Spacing

```python
# Character spacing via XML (no direct python-pptx API)
from pptx.oxml.ns import qn
from lxml import etree

run = p.add_run()
run.text = "SPACED TEXT"
# Set character spacing in hundredths of a point (600 = 6pt)
rPr = run._r.get_or_add_rPr()
rPr.set('spc', '600')
```

---

## Lists & Bullets

```python
from pptx.util import Inches, Pt
from pptx.oxml.ns import qn
from lxml import etree

txBox = slide.shapes.add_textbox(Inches(0.5), Inches(0.5), Inches(8), Inches(3))
tf = txBox.text_frame
tf.word_wrap = True

items = ["First item", "Second item", "Third item"]

for i, item in enumerate(items):
    if i == 0:
        p = tf.paragraphs[0]
    else:
        p = tf.add_paragraph()

    p.text = item
    p.font.size = Pt(16)

    # Enable bullet via XML
    pPr = p._p.get_or_add_pPr()
    buChar = etree.SubElement(pPr, qn('a:buChar'))
    buChar.set('char', '•')

# ❌ WRONG: Never manually prepend "• " to text strings — creates double bullets
# ✅ CORRECT: Use the XML buChar approach above

# Numbered list
for i, item in enumerate(items):
    p = tf.add_paragraph() if i > 0 else tf.paragraphs[0]
    p.text = item
    pPr = p._p.get_or_add_pPr()
    buAutoNum = etree.SubElement(pPr, qn('a:buAutoNum'))
    buAutoNum.set('type', 'arabicPeriod')

# Indented sub-item
p = tf.add_paragraph()
p.text = "Sub-item"
p.level = 1  # Indent level (0 = top, 1 = first indent, etc.)
pPr = p._p.get_or_add_pPr()
buChar = etree.SubElement(pPr, qn('a:buChar'))
buChar.set('char', '–')
```

---

## Shapes

```python
from pptx.util import Inches, Pt, Emu
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.dml.color import RGBColor
from pptx import shapes as pptx_shapes
from pptx.enum.shapes import MSO_CONNECTOR_TYPE
from pptx.util import Pt
import pptx.shapes.autoshape as autoshapes

# Rectangle
from pptx.enum.shapes import MSO_SHAPE_TYPE
from pptx.util import Inches
from pptx.dml.color import RGBColor

shape = slide.shapes.add_shape(
    1,  # MSO_SHAPE_TYPE.RECTANGLE = 1
    Inches(0.5), Inches(0.8), Inches(1.5), Inches(3.0)
)
shape.fill.solid()
shape.fill.fore_color.rgb = RGBColor(0xFF, 0x00, 0x00)
shape.line.color.rgb = RGBColor(0x00, 0x00, 0x00)
shape.line.width = Pt(2)

# Oval
from pptx.enum.shapes import MSO_SHAPE_TYPE
oval = slide.shapes.add_shape(
    9,  # OVAL
    Inches(4), Inches(1), Inches(2), Inches(2)
)
oval.fill.solid()
oval.fill.fore_color.rgb = RGBColor(0x00, 0x00, 0xFF)

# Line (use add_connector for lines)
from pptx.enum.shapes import MSO_CONNECTOR_TYPE
connector = slide.shapes.add_connector(
    MSO_CONNECTOR_TYPE.STRAIGHT,
    Inches(1), Inches(3), Inches(6), Inches(3)
)
connector.line.color.rgb = RGBColor(0xFF, 0x00, 0x00)
connector.line.width = Pt(3)
# Dashed line:
from pptx.enum.dml import MSO_LINE_DASH_STYLE
connector.line.dash_style = MSO_LINE_DASH_STYLE.DASH

# Rounded rectangle
rounded = slide.shapes.add_shape(
    5,  # ROUNDED_RECTANGLE
    Inches(1), Inches(1), Inches(3), Inches(2)
)
rounded.fill.solid()
rounded.fill.fore_color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
# Set corner radius via XML (adjustments)
# ⚠️ Don't pair rounded rectangles with rectangular accent overlays —
#    the rect overlay won't cover the rounded corners. Use RECTANGLE instead.

# Transparency (via XML — python-pptx doesn't expose this directly)
from lxml import etree
from pptx.oxml.ns import qn

shape = slide.shapes.add_shape(1, Inches(1), Inches(1), Inches(3), Inches(2))
shape.fill.solid()
shape.fill.fore_color.rgb = RGBColor(0x00, 0x88, 0xCC)
# Set alpha (0=opaque, 100000=fully transparent — value is in thousandths of a percent)
solidFill = shape.fill._xPr.find(qn('a:solidFill'))
if solidFill is not None:
    srgbClr = solidFill.find(qn('a:srgbClr'))
    if srgbClr is not None:
        alpha = etree.SubElement(srgbClr, qn('a:alpha'))
        alpha.set('val', '50000')  # 50% opacity

# Shadow (via XML)
def add_shadow(shape, blur_pt=6, offset_pt=2, angle=135, color_hex="000000", opacity=0.15):
    """Add outer shadow to a shape."""
    sp = shape._element
    spPr = sp.find(qn('p:spPr'))
    if spPr is None:
        spPr = etree.SubElement(sp, qn('p:spPr'))
    effectLst = spPr.find(qn('a:effectLst'))
    if effectLst is None:
        effectLst = etree.SubElement(spPr, qn('a:effectLst'))
    outerShdw = etree.SubElement(effectLst, qn('a:outerShdw'))
    blur_emu = int(blur_pt * 12700)
    offset_emu = int(offset_pt * 12700)
    alpha_val = int((1 - opacity) * 100000)
    outerShdw.set('blurRad', str(blur_emu))
    outerShdw.set('dist', str(offset_emu))
    outerShdw.set('dir', str(int(angle * 60000)))
    srgbClr = etree.SubElement(outerShdw, qn('a:srgbClr'))
    srgbClr.set('val', color_hex)
    alphaEl = etree.SubElement(srgbClr, qn('a:alpha'))
    alphaEl.set('val', str(int(opacity * 100000)))

shape = slide.shapes.add_shape(1, Inches(1), Inches(1), Inches(3), Inches(2))
shape.fill.solid()
shape.fill.fore_color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
add_shadow(shape, blur_pt=6, offset_pt=2, angle=135, color_hex="000000", opacity=0.15)
```

### Shape Type Constants

| Shape         | MSO int |
|---------------|---------|
| RECTANGLE     | 1       |
| ROUNDED_RECT  | 5       |
| OVAL          | 9       |

For lines, use `add_connector()` with `MSO_CONNECTOR_TYPE.STRAIGHT`.

**Note:** Gradient fills are not natively supported in python-pptx. Use a gradient image as a slide background instead.

---

## Images

```python
from pptx.util import Inches

# From file path
slide.shapes.add_picture("images/chart.png", Inches(1), Inches(1), Inches(5), Inches(3))

# From URL (download first)
import urllib.request, tempfile, os
url = "https://example.com/image.jpg"
with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
    urllib.request.urlretrieve(url, tmp.name)
    slide.shapes.add_picture(tmp.name, Inches(1), Inches(1), Inches(5), Inches(3))
os.unlink(tmp.name)

# From base64
import base64, io
img_data = base64.b64decode("iVBORw0KGgo...")
img_stream = io.BytesIO(img_data)
slide.shapes.add_picture(img_stream, Inches(1), Inches(1), Inches(5), Inches(3))

# Circular crop (via XML placeholder hack — use a shape with picture fill instead)
# Rounded/circular image: add an oval, set its fill to picture
from pptx.oxml.ns import qn
from lxml import etree

oval = slide.shapes.add_shape(9, Inches(1), Inches(1), Inches(2), Inches(2))
spPr = oval._element.find(qn('p:spPr'))
# Set picture fill via XML
blipFill = etree.SubElement(spPr, qn('a:blipFill'))
blip = etree.SubElement(blipFill, qn('a:blip'))
# (embed image relationship first, then set r:embed)
```

### Calculate Dimensions (preserve aspect ratio)

```python
orig_width = 1978
orig_height = 923
max_height = 3.0  # inches

calc_width = max_height * (orig_width / orig_height)
center_x = (10 - calc_width) / 2

slide.shapes.add_picture("image.png", Inches(center_x), Inches(1.2), Inches(calc_width), Inches(max_height))
```

---

## Icons

Use the `cairosvg` + `Pillow` pipeline to rasterize SVG icons to PNG for embedding.

### Setup

```bash
pip install cairosvg Pillow requests
```

### Option A: From a local SVG file

```python
import cairosvg
import io
from PIL import Image

def svg_file_to_png_bytes(svg_path, size=256, color=None):
    with open(svg_path, 'r') as f:
        svg_content = f.read()
    if color:
        svg_content = svg_content.replace('currentColor', color)
        svg_content = svg_content.replace('fill="black"', f'fill="{color}"')
    png_bytes = cairosvg.svg2png(bytestring=svg_content.encode(), output_width=size, output_height=size)
    return io.BytesIO(png_bytes)

icon_stream = svg_file_to_png_bytes("checkmark.svg", size=256, color="#4472C4")
slide.shapes.add_picture(icon_stream, Inches(1), Inches(1), Inches(0.5), Inches(0.5))
```

### Option B: Download icon from a CDN (e.g. Simple Icons, Heroicons)

```python
import requests
import cairosvg
import io

def url_svg_to_png_bytes(url, size=256, fill_color="#000000"):
    response = requests.get(url)
    svg_content = response.text
    svg_content = svg_content.replace('currentColor', fill_color)
    png_bytes = cairosvg.svg2png(bytestring=svg_content.encode(), output_width=size, output_height=size)
    return io.BytesIO(png_bytes)

# Example: a heroicon check-circle
icon_url = "https://raw.githubusercontent.com/tailwindlabs/heroicons/master/src/24/solid/check-circle.svg"
icon_stream = url_svg_to_png_bytes(icon_url, size=256, fill_color="#4472C4")
slide.shapes.add_picture(icon_stream, Inches(1), Inches(1), Inches(0.5), Inches(0.5))
```

**Note:** Use size 256 or higher for crisp icons. The size parameter controls rasterization resolution, not display size on the slide (set by width/height in inches when adding the picture).

---

## Slide Backgrounds

```python
from pptx.dml.color import RGBColor

# Solid color background
background = slide.background
fill = background.fill
fill.solid()
fill.fore_color.rgb = RGBColor(0xF1, 0xF1, 0xF1)

# Image background (stretch to fill)
from pptx.oxml.ns import qn
from lxml import etree

def set_background_image(slide, image_path):
    """Set a slide background image."""
    import pptx.parts.image as img_part
    prs = slide.part.package.presentation
    pic_rel = slide.part.relate_to(
        slide.part.package.image_part_factory(image_path),
        'http://schemas.openxmlformats.org/officeDocument/2006/relationships/image'
    )
    bg = slide.background
    fill = bg.fill
    fill.solid()  # placeholder to get element structure
    # Override with blipFill via XML
    bgPr = bg._element.find(qn('p:bgPr'))
    for child in list(bgPr):
        bgPr.remove(child)
    blipFill = etree.SubElement(bgPr, qn('a:blipFill'))
    blip = etree.SubElement(blipFill, qn('a:blip'))
    blip.set(qn('r:embed'), pic_rel)
    stretch = etree.SubElement(blipFill, qn('a:stretch'))
    etree.SubElement(stretch, qn('a:fillRect'))

# Simpler approach using add_picture at z=0 (behind all content):
def set_background_image_simple(slide, image_path, slide_width_in=10, slide_height_in=5.625):
    """Add image as background by inserting it at the bottom of the z-stack."""
    pic = slide.shapes.add_picture(image_path, 0, 0, Inches(slide_width_in), Inches(slide_height_in))
    slide.shapes._spTree.remove(pic._element)
    slide.shapes._spTree.insert(2, pic._element)  # Insert behind other elements
```

---

## Tables

```python
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

rows, cols = 3, 2
table = slide.shapes.add_table(rows, cols, Inches(1), Inches(1), Inches(8), Inches(2)).table

# Set column widths
table.columns[0].width = Inches(4)
table.columns[1].width = Inches(4)

# Header row
headers = ["Header 1", "Header 2"]
for col_idx, header in enumerate(headers):
    cell = table.cell(0, col_idx)
    cell.text = header
    cell.fill.solid()
    cell.fill.fore_color.rgb = RGBColor(0x66, 0x99, 0xCC)
    p = cell.text_frame.paragraphs[0]
    run = p.runs[0] if p.runs else p.add_run()
    run.font.bold = True
    run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
    run.font.size = Pt(14)

# Data rows
data = [["Cell 1", "Cell 2"], ["Cell 3", "Cell 4"]]
for row_idx, row_data in enumerate(data, start=1):
    for col_idx, value in enumerate(row_data):
        cell = table.cell(row_idx, col_idx)
        cell.text = value
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0xF1, 0xF1, 0xF1)
        p = cell.text_frame.paragraphs[0]
        run = p.runs[0] if p.runs else p.add_run()
        run.font.size = Pt(12)

# Cell borders (via XML)
from pptx.oxml.ns import qn
from lxml import etree

def set_cell_border(cell, color_hex="999999", width_pt=1):
    tc = cell._tc
    tcPr = tc.find(qn('a:tcPr'))
    if tcPr is None:
        tcPr = etree.SubElement(tc, qn('a:tcPr'))
    for border_tag in ['a:lnL', 'a:lnR', 'a:lnT', 'a:lnB']:
        ln = etree.SubElement(tcPr, qn(border_tag))
        ln.set('w', str(int(width_pt * 12700)))
        ln.set('cap', 'flat')
        solidFill = etree.SubElement(ln, qn('a:solidFill'))
        srgbClr = etree.SubElement(solidFill, qn('a:srgbClr'))
        srgbClr.set('val', color_hex)

for r in range(rows):
    for c in range(cols):
        set_cell_border(table.cell(r, c), color_hex="999999", width_pt=1)

# Merged cells
# python-pptx does not support merge directly; use XML:
def merge_cells(table, row1, col1, row2, col2):
    """Merge cells from (row1,col1) to (row2,col2)."""
    cell = table.cell(row1, col1)
    other = table.cell(row2, col2)
    cell.merge(other)  # Available in python-pptx >= 0.6.19
```

---

## Charts

```python
from pptx.chart.data import ChartData
from pptx.enum.chart import XL_CHART_TYPE
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor

# Bar chart (clustered column)
chart_data = ChartData()
chart_data.categories = ['Q1', 'Q2', 'Q3', 'Q4']
chart_data.add_series('Sales', (4500, 5500, 6200, 7100))

chart = slide.shapes.add_chart(
    XL_CHART_TYPE.COLUMN_CLUSTERED,
    Inches(0.5), Inches(0.6), Inches(6), Inches(3),
    chart_data
).chart

chart.has_title = True
chart.chart_title.text_frame.text = 'Quarterly Sales'

# Line chart
line_data = ChartData()
line_data.categories = ['Jan', 'Feb', 'Mar']
line_data.add_series('Temp', (32, 35, 42))

line_chart = slide.shapes.add_chart(
    XL_CHART_TYPE.LINE,
    Inches(0.5), Inches(4), Inches(6), Inches(3),
    line_data
).chart

# Pie chart
pie_data = ChartData()
pie_data.categories = ['A', 'B', 'Other']
pie_data.add_series('Share', (35, 45, 20))

pie_chart = slide.shapes.add_chart(
    XL_CHART_TYPE.PIE,
    Inches(7), Inches(1), Inches(5), Inches(4),
    pie_data
).chart
pie_chart.plots[0].has_data_labels = True
```

### Better-Looking Charts

```python
from pptx.dml.color import RGBColor
from pptx.util import Pt
from pptx.enum.chart import XL_LEGEND_POSITION

chart_data = ChartData()
chart_data.categories = ['Q1', 'Q2', 'Q3', 'Q4']
chart_data.add_series('Revenue', (4500, 5500, 6200, 7100))

chart_frame = slide.shapes.add_chart(
    XL_CHART_TYPE.COLUMN_CLUSTERED,
    Inches(0.5), Inches(1), Inches(9), Inches(4),
    chart_data
)
chart = chart_frame.chart

# Custom series color
series = chart.series[0]
series.format.fill.solid()
series.format.fill.fore_color.rgb = RGBColor(0x0D, 0x94, 0x88)

# Data labels
series.has_data_labels = True
data_labels = series.data_labels
data_labels.font.size = Pt(10)
data_labels.font.color.rgb = RGBColor(0x1E, 0x29, 0x3B)

# Axis label colors
cat_axis = chart.category_axis
cat_axis.tick_labels.font.color.rgb = RGBColor(0x64, 0x74, 0x8B)
cat_axis.tick_labels.font.size = Pt(10)

val_axis = chart.value_axis
val_axis.tick_labels.font.color.rgb = RGBColor(0x64, 0x74, 0x8B)
val_axis.tick_labels.font.size = Pt(10)

# Hide legend for single series
chart.has_legend = False

# Chart background (white)
chart.chart_area.fill.solid()
chart.chart_area.fill.fore_color.rgb = RGBColor(0xFF, 0xFF, 0xFF)

# Common XL_CHART_TYPE values:
# COLUMN_CLUSTERED, BAR_CLUSTERED, LINE, LINE_MARKERS,
# PIE, DOUGHNUT, XY_SCATTER, RADAR
```

---

## Slide Masters & Layouts

```python
# python-pptx uses the layouts already in the template .pptx.
# Blank layout (index 6 in most templates) is best for custom slides:
blank_layout = prs.slide_layouts[6]
slide = prs.slides.add_slide(blank_layout)

# To use a pre-branded template:
prs = Presentation("your_template.pptx")
slide = prs.slides.add_slide(prs.slide_layouts[6])

# To reuse master background color across slides, set each slide's background:
def apply_dark_background(slide, hex_color="283A5E"):
    bg = slide.background
    fill = bg.fill
    fill.solid()
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    fill.fore_color.rgb = RGBColor(r, g, b)
```

---

## Utility Helper Functions

These helpers reduce repetition and are recommended for any agent generating slides:

```python
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

def hex_color(hex_str):
    """Convert 6-char hex (no #) to RGBColor."""
    return RGBColor(int(hex_str[0:2], 16), int(hex_str[2:4], 16), int(hex_str[4:6], 16))

def add_text(slide, text, x, y, w, h, font_size=18, bold=False, italic=False,
             color_hex="363636", font_name="Calibri", align="left",
             valign="top", margin=None):
    """Add a simple text box to a slide."""
    from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
    align_map = {"left": PP_ALIGN.LEFT, "center": PP_ALIGN.CENTER, "right": PP_ALIGN.RIGHT}
    valign_map = {"top": MSO_ANCHOR.TOP, "middle": MSO_ANCHOR.MIDDLE, "bottom": MSO_ANCHOR.BOTTOM}

    txBox = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = txBox.text_frame
    tf.word_wrap = True
    tf.auto_size = None

    if margin is not None:
        m = Inches(margin)
        tf.margin_left = m
        tf.margin_right = m
        tf.margin_top = m
        tf.margin_bottom = m

    p = tf.paragraphs[0]
    p.alignment = align_map.get(align, PP_ALIGN.LEFT)
    run = p.add_run()
    run.text = text
    run.font.size = Pt(font_size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = hex_color(color_hex)
    run.font.name = font_name
    return txBox

def add_rect(slide, x, y, w, h, fill_hex=None, line_hex=None, line_width_pt=1, transparency=None):
    """Add a rectangle shape."""
    from pptx.oxml.ns import qn
    from lxml import etree
    shape = slide.shapes.add_shape(1, Inches(x), Inches(y), Inches(w), Inches(h))
    if fill_hex:
        shape.fill.solid()
        shape.fill.fore_color.rgb = hex_color(fill_hex)
        if transparency is not None:
            solidFill = shape.fill._xPr.find(qn('a:solidFill'))
            srgbClr = solidFill.find(qn('a:srgbClr')) if solidFill is not None else None
            if srgbClr is not None:
                alpha = etree.SubElement(srgbClr, qn('a:alpha'))
                alpha.set('val', str(int((1 - transparency / 100) * 100000)))
    else:
        shape.fill.background()
    if line_hex:
        shape.line.color.rgb = hex_color(line_hex)
        shape.line.width = Pt(line_width_pt)
    else:
        shape.line.fill.background()
    return shape
```

---

## Common Pitfalls

⚠️ These issues cause file corruption, visual bugs, or broken output.

1. **NEVER use "#" with hex colors** — RGBColor takes individual int channels, not hex strings directly.
   ```python
   RGBColor(0xFF, 0x00, 0x00)      # ✅ CORRECT
   RGBColor("#FF0000")              # ❌ WRONG — will error or corrupt
   # Use the hex_color() helper above for string-based hex
   ```

2. **NEVER pass transparency as part of the color value** — use the alpha XML approach shown in Shapes.

3. **Use XML buChar for bullets** — never prepend "•" to text manually (creates double bullets when bullet property is also set).

4. **Multi-line text**: Add a new paragraph with `tf.add_paragraph()` for each line rather than embedding `\n` in run text (unreliable).

5. **Avoid lineSpacing with bullets** — use `paraSpaceAfter` (via XML `a:spcAft`) instead:
   ```python
   from pptx.oxml.ns import qn
   from lxml import etree
   pPr = p._p.get_or_add_pPr()
   spcAft = etree.SubElement(pPr, qn('a:spcAft'))
   spcPts = etree.SubElement(spcAft, qn('a:spcPts'))
   spcPts.set('val', '200')  # 2pt spacing after paragraph (in hundredths of pt)
   ```

6. **Create a fresh Presentation() for each file** — do not reuse `prs` objects across multiple output files.

7. **Do NOT reuse style dicts across shapes** — python-pptx can mutate objects internally. Define values inline or use factory functions (same principle as the PptxGenJS `makeShadow()` pattern).
   ```python
   # ❌ Risky: shared dict mutated by first call may affect second
   style = {"fill_hex": "FFFFFF"}
   add_rect(slide, 1, 1, 3, 2, **style)
   add_rect(slide, 5, 1, 3, 2, **style)

   # ✅ Safe: inline values
   add_rect(slide, 1, 1, 3, 2, fill_hex="FFFFFF")
   add_rect(slide, 5, 1, 3, 2, fill_hex="FFFFFF")
   ```

8. **ROUNDED_RECTANGLE + accent borders**: Rectangular overlay bars won't cover rounded corners.
   ```python
   # ❌ WRONG: accent bar doesn't cover rounded corners
   slide.shapes.add_shape(5, ...)  # ROUNDED_RECTANGLE
   slide.shapes.add_shape(1, ...)  # RECTANGLE accent — corners still visible

   # ✅ CORRECT: Use RECTANGLE (shape type 1) for clean alignment
   slide.shapes.add_shape(1, ...)  # RECTANGLE card
   slide.shapes.add_shape(1, ...)  # RECTANGLE accent bar
   ```

---

## Quick Reference

### Shape Type Integers

| Shape             | Int |
|-------------------|-----|
| RECTANGLE         | 1   |
| ROUNDED_RECTANGLE | 5   |
| OVAL              | 9   |
| RIGHT_TRIANGLE    | 8   |

For lines/connectors: `slide.shapes.add_connector(MSO_CONNECTOR_TYPE.STRAIGHT, x1, y1, x2, y2)`

### Chart Types (`XL_CHART_TYPE`)

| Type                | Enum                          |
|---------------------|-------------------------------|
| Column (vertical)   | `COLUMN_CLUSTERED`            |
| Bar (horizontal)    | `BAR_CLUSTERED`               |
| Line                | `LINE` / `LINE_MARKERS`       |
| Pie                 | `PIE`                         |
| Doughnut            | `DOUGHNUT`                    |
| Scatter             | `XY_SCATTER`                  |
| Radar               | `RADAR`                       |

### Alignment Constants

```python
from pptx.enum.text import PP_ALIGN
PP_ALIGN.LEFT / PP_ALIGN.CENTER / PP_ALIGN.RIGHT / PP_ALIGN.JUSTIFY

from pptx.enum.text import MSO_ANCHOR
MSO_ANCHOR.TOP / MSO_ANCHOR.MIDDLE / MSO_ANCHOR.BOTTOM
```

### Unit Conversions

```python
from pptx.util import Inches, Pt, Emu
Inches(1)    # 914400 EMU
Pt(1)        # 12700 EMU
Emu(914400)  # 1 inch
```

### Required Imports (full reference)

```python
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_CONNECTOR_TYPE
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.chart.data import ChartData
from pptx.oxml.ns import qn
from lxml import etree
```

### Dependencies

```bash
pip install python-pptx    # Core library
pip install lxml            # XML manipulation (usually installed with python-pptx)
pip install cairosvg        # SVG → PNG for icons
pip install Pillow          # Image processing
pip install requests        # Download remote SVGs/images
```
