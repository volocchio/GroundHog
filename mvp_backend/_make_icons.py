"""Generate PWA icons for GroundHog. Run once; commit the resulting PNGs."""
import os
from PIL import Image, ImageDraw

OUT = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(OUT, exist_ok=True)


def make_icon(sz: int, name: str) -> None:
    im = Image.new("RGBA", (sz, sz), (15, 23, 42, 255))  # slate-900 background
    d = ImageDraw.Draw(im)
    pad = sz // 10
    d.rounded_rectangle((pad, pad, sz - pad, sz - pad),
                        radius=sz // 8, fill=(5, 150, 105, 255))  # emerald
    cx, cy = sz // 2, sz // 2
    s = sz // 3
    # Garmin-style ownship triangle in yellow
    tri = [(cx, cy - s), (cx + s * 3 // 4, cy + s),
           (cx, cy + s // 2), (cx - s * 3 // 4, cy + s)]
    d.polygon(tri, fill=(245, 211, 0, 255), outline=(17, 17, 17, 255))
    im.save(os.path.join(OUT, name), "PNG", optimize=True)
    print("wrote", name)


if __name__ == "__main__":
    make_icon(192, "icon-192.png")
    make_icon(512, "icon-512.png")
    make_icon(180, "apple-touch-icon.png")
    # Maskable variant: extra safe area so Android can crop circle/squircle
    sz = 512
    im = Image.new("RGBA", (sz, sz), (5, 150, 105, 255))
    d = ImageDraw.Draw(im)
    cx, cy = sz // 2, sz // 2
    s = sz // 4
    tri = [(cx, cy - s), (cx + s * 3 // 4, cy + s),
           (cx, cy + s // 2), (cx - s * 3 // 4, cy + s)]
    d.polygon(tri, fill=(245, 211, 0, 255), outline=(17, 17, 17, 255))
    im.save(os.path.join(OUT, "icon-maskable-512.png"), "PNG", optimize=True)
    print("wrote icon-maskable-512.png")
