from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Tuple


def gpx_from_path(name: str, path: Iterable[Tuple[float, float]]) -> str:
    pts = list(path)
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<gpx version="1.1" creator="GroundHog" xmlns="http://www.topografix.com/GPX/1/1">',
        f'  <metadata><name>{_xml(name)}</name><time>{ts}</time></metadata>',
        f'  <trk><name>{_xml(name)}</name><trkseg>',
    ]
    for lat, lon in pts:
        lines.append(f'    <trkpt lat="{lat:.6f}" lon="{lon:.6f}"></trkpt>')
    lines += ['  </trkseg></trk>', '</gpx>']
    return "\n".join(lines) + "\n"


def _xml(s: str) -> str:
    return (
        s.replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&apos;')
    )
