import asyncio
import aiohttp
import json
import xml.etree.ElementTree as ET
import shutil
from pathlib import Path
import geopandas as gpd
import pandas as pd

TEMP_DIR = Path(".temp_parcels")
OUTPUT_FILE = "portugal_parcels.geojson"
MAX_CONCURRENT = 5


async def download_inspire_parcels(session):
    """Download INSPIRE cadastral parcels from DGT WFS."""
    print("\n=== Downloading INSPIRE Cadastral Parcels ===", flush=True)
    
    wfs_url = "https://snicws.dgterritorio.gov.pt/geoserver/wfs"
    output_dir = TEMP_DIR / "inspire"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get total count
    async with session.get(wfs_url, params={
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": "inspire:cadastralparcel", "resultType": "hits"
    }) as resp:
        root = ET.fromstring(await resp.text())
        total = int(root.attrib.get('numberMatched', 0))
    
    print(f"Total INSPIRE parcels: {total:,}", flush=True)
    
    batch_size = 50000
    num_batches = (total + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async def download_batch(batch_num):
        async with semaphore:
            params = {
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeName": "inspire:cadastralparcel", "outputFormat": "application/json",
                "startIndex": (batch_num - 1) * batch_size, "count": batch_size
            }
            try:
                async with session.get(wfs_url, params=params, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        with open(output_dir / f"{batch_num:04d}.geojson", 'w') as f:
                            json.dump(data, f)
                        print(f"INSPIRE {batch_num}/{num_batches}", flush=True)
                        return True
            except Exception as e:
                print(f"INSPIRE {batch_num}: failed - {e}", flush=True)
            return False
    
    await asyncio.gather(*[download_batch(i+1) for i in range(num_batches)])


async def download_rgg_parcels(session):
    """Download RGG parcels from BUPi MapServer."""
    print("\n=== Downloading RGG Parcels ===", flush=True)
    
    url = "https://geo.bupi.gov.pt/gisbupi/rest/services/opendata/RGG_DadosGovPT/MapServer/0/query"
    output_dir = TEMP_DIR / "rgg"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get total count
    async with session.get(url, params={"where": "1=1", "returnCountOnly": "true", "f": "json"}) as resp:
        total = (await resp.json()).get('count', 0)
    
    print(f"Total RGG parcels: {total:,}", flush=True)
    
    batch_size = 2000
    num_batches = (total + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async def download_batch(batch_num):
        async with semaphore:
            params = {
                "where": "1=1", "outFields": "*", "returnGeometry": "true",
                "outSR": "4326", "resultOffset": (batch_num - 1) * batch_size,
                "resultRecordCount": batch_size, "f": "geojson"
            }
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        with open(output_dir / f"{batch_num:04d}.geojson", 'w') as f:
                            json.dump(data, f)
                        if batch_num % 100 == 0:
                            print(f"RGG {batch_num}/{num_batches}", flush=True)
                        return True
            except Exception as e:
                print(f"RGG {batch_num}: failed - {e}", flush=True)
            return False
    
    await asyncio.gather(*[download_batch(i+1) for i in range(num_batches)])


def normalize_and_merge():
    """Load, normalize, and merge all parcels into single GeoJSON."""
    print("\n=== Normalizing and Merging ===", flush=True)
    
    # Load INSPIRE (EPSG:3763 -> EPSG:4326)
    inspire = gpd.GeoDataFrame(pd.concat([gpd.read_file(f) for f in sorted((TEMP_DIR / "inspire").glob("*.geojson"))], ignore_index=True), crs="EPSG:3763").to_crs("EPSG:4326")
    inspire_norm = gpd.GeoDataFrame({'id': inspire.get('inspireid', inspire.index.astype(str)), 'reference': inspire.get('nationalcadastralreference', ''), 'area_m2': inspire.get('areavalue', None), 'source': 'inspire', 'geometry': inspire.geometry}, crs="EPSG:4326")
    
    # Load RGG (already EPSG:4326)
    rgg = gpd.GeoDataFrame(pd.concat([gpd.read_file(f) for f in sorted((TEMP_DIR / "rgg").glob("*.geojson"))], ignore_index=True), crs="EPSG:4326")
    rgg_norm = gpd.GeoDataFrame({'id': rgg.get('objectid', rgg.index.astype(str)).astype(str), 'reference': '', 'area_m2': rgg.get('st_area(shape)', None), 'source': 'rgg', 'geometry': rgg.geometry}, crs="EPSG:4326")
    
    # Merge and save
    combined = gpd.GeoDataFrame(pd.concat([inspire_norm, rgg_norm], ignore_index=True), crs="EPSG:4326")
    print(f"Total: {len(combined):,} features. Saving to {OUTPUT_FILE}...", flush=True)
    combined.to_file(OUTPUT_FILE, driver="GeoJSON")


async def main():
    TEMP_DIR.mkdir(exist_ok=True)
    
    connector = aiohttp.TCPConnector(ssl=False, limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        await download_inspire_parcels(session)
        await download_rgg_parcels(session)
    
    normalize_and_merge()
    
    print("\nCleaning up temp files...", flush=True)
    shutil.rmtree(TEMP_DIR)
    print(f"Done. Output: {OUTPUT_FILE}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
