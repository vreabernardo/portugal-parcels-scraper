# Portugal Cadastral Parcels Scraper

Downloads all cadastral parcels from Portugal (4.9M+ parcels) and outputs a single normalized GeoJSON file.

## Data Sources

1. **INSPIRE Cadastral Parcels** - WFS from Direccao-Geral do Territorio (DGT)
   - URL: `https://snicws.dgterritorio.gov.pt/geoserver/wfs`
   - Coverage: All of Portugal
   - ~1.8M parcels

2. **RGG Registered Parcels** - ArcGIS MapServer from BUPi
   - URL: `https://geo.bupi.gov.pt/gisbupi/rest/services/opendata/RGG_DadosGovPT/MapServer`
   - Coverage: Northern Portugal (registered properties)
   - ~3.1M parcels

## How It Works

1. Downloads INSPIRE parcels in batches of 50,000 via WFS
2. Downloads RGG parcels in batches of 2,000 via ArcGIS REST API
3. Reprojects INSPIRE from EPSG:3763 to EPSG:4326
4. Normalizes both datasets to common schema
5. Merges into single GeoJSON file
6. Cleans up temp files

## Output Schema

| Field | Description |
|-------|-------------|
| id | Unique identifier |
| reference | National cadastral reference (INSPIRE only) |
| area_m2 | Area in square meters |
| source | `inspire` or `rgg` |
| geometry | Polygon in EPSG:4326 |

## Usage

```bash
uv venv
uv pip install aiohttp geopandas
uv run scraper.py
```

Output: `portugal_parcels.geojson` (~5 GB)

## Requirements

- Python 3.10+
- aiohttp
- geopandas
- pandas

