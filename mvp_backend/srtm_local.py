from __future__ import annotations

import gzip
import math
import os
import struct
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import requests


def _tile_name(lat: float, lon: float) -> tuple[str, int, int]:
    """Return (name, ilat, ilon) for 1x1 degree tile containing lat/lon.

    Tile convention uses SW corner integer degrees.
    """
    ilat = math.floor(lat)
    ilon = math.floor(lon)
    ns = "N" if ilat >= 0 else "S"
    ew = "E" if ilon >= 0 else "W"
    name = f"{ns}{abs(ilat):02d}{ew}{abs(ilon):03d}.hgt"
    return name, ilat, ilon


def _tile_url(ilat: int, ilon: int) -> str:
    """Mapzen / AWS elevation tiles (SRTM-derived) in .hgt.gz.

    These are widely used 'skadi' tiles. Directory uses N/S for latitude band.
    Example: .../skadi/N37/N37W122.hgt.gz
    """
    ns = "N" if ilat >= 0 else "S"
    band = f"{ns}{abs(ilat):02d}"
    ew = "E" if ilon >= 0 else "W"
    fname = f"{ns}{abs(ilat):02d}{ew}{abs(ilon):03d}.hgt.gz"
    return f"https://s3.amazonaws.com/elevation-tiles-prod/skadi/{band}/{fname}"


@dataclass
class SRTMTile:
    ilat: int
    ilon: int
    n: int
    data: memoryview  # big-endian int16

    def elev_m(self, lat: float, lon: float) -> float:
        """Nearest-neighbor sample. Returns meters."""
        # Fraction within tile from NW corner.
        # HGT is arranged from north to south, west to east.
        # lat in [ilat, ilat+1), lon in [ilon, ilon+1)
        frac_y = (self.ilat + 1.0 - lat)  # 0 at north edge
        frac_x = (lon - self.ilon)
        i = int(round(frac_y * (self.n - 1)))
        j = int(round(frac_x * (self.n - 1)))
        i = max(0, min(self.n - 1, i))
        j = max(0, min(self.n - 1, j))
        idx = (i * self.n + j) * 2
        val = struct.unpack(">h", self.data[idx:idx+2])[0]
        # voids are often -32768
        if val <= -32000:
            return float("nan")
        return float(val)


@dataclass
class SRTMProvider:
    cache_dir: str
    # SRTM1 tiles are ~25 MB in memory; 16 tiles ≈ 400 MB and covers a
    # continental-scale multi-leg route with room to spare. Overridable
    # via env var for tighter/looser deployments.
    max_tiles: int = int(os.environ.get("GROUNDHOG_SRTM_MAX_TILES", "16"))

    def __post_init__(self):
        os.makedirs(self.cache_dir, exist_ok=True)
        self._tiles: "OrderedDict[tuple[int, int], SRTMTile]" = OrderedDict()

    def _load_tile(self, ilat: int, ilon: int) -> SRTMTile:
        key = (ilat, ilon)
        cached = self._tiles.get(key)
        if cached is not None:
            self._tiles.move_to_end(key)
            return cached

        ns = "N" if ilat >= 0 else "S"
        ew = "E" if ilon >= 0 else "W"
        base = f"{ns}{abs(ilat):02d}{ew}{abs(ilon):03d}.hgt"
        path = os.path.join(self.cache_dir, base)

        def download_tile() -> None:
            url = _tile_url(ilat, ilon)
            gz_path = path + ".gz"
            tmp_gz = gz_path + ".tmp"
            if not os.path.exists(gz_path):
                r = requests.get(url, timeout=90)
                r.raise_for_status()
                with open(tmp_gz, "wb") as f:
                    f.write(r.content)
                os.replace(tmp_gz, gz_path)  # atomic rename
            tmp_hgt = path + ".tmp"
            with gzip.open(gz_path, "rb") as gz, open(tmp_hgt, "wb") as out:
                out.write(gz.read())
            os.replace(tmp_hgt, path)  # atomic rename

        if not os.path.exists(path):
            download_tile()

        size = os.path.getsize(path)
        # each sample is 2 bytes
        n = int(round(math.sqrt(size / 2)))
        if n * n * 2 != size:
            # Cached tile is corrupt/truncated. Delete both expanded and
            # compressed copies and try once before failing the route.
            for bad_path in (path, path + ".gz"):
                try:
                    if os.path.exists(bad_path):
                        os.remove(bad_path)
                except OSError:
                    pass
            download_tile()
            size = os.path.getsize(path)
            n = int(round(math.sqrt(size / 2)))
            if n * n * 2 != size:
                raise RuntimeError(f"Unexpected HGT size for {path}: {size}")

        with open(path, "rb") as f:
            data = memoryview(f.read())

        tile = SRTMTile(ilat=ilat, ilon=ilon, n=n, data=data)
        self._tiles[key] = tile
        while len(self._tiles) > self.max_tiles:
            self._tiles.popitem(last=False)
        return tile

    def get_many_m(self, points: Iterable[tuple[float, float]]) -> List[float]:
        pts = list(points)
        out: List[float] = []
        # Fast path: adjacent grid rows almost always fall in the same tile,
        # so cache the last-used tile and skip the tile-name / dict lookup.
        last_ilat: int | None = None
        last_ilon: int | None = None
        last_tile: SRTMTile | None = None
        floor = math.floor
        for lat, lon in pts:
            ilat = floor(lat)
            ilon = floor(lon)
            if ilat == last_ilat and ilon == last_ilon and last_tile is not None:
                tile = last_tile
            else:
                try:
                    tile = self._load_tile(ilat, ilon)
                except Exception:
                    last_ilat, last_ilon, last_tile = None, None, None
                    out.append(float("nan"))
                    continue
                last_ilat, last_ilon, last_tile = ilat, ilon, tile
            try:
                out.append(tile.elev_m(lat, lon))
            except Exception:
                out.append(float("nan"))
        return out
