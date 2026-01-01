import asyncio
import aiohttp
import ssl
import json
import xml.etree.ElementTree as ET
import shutil
from pathlib import Path

import geopandas as gpd
import pandas as pd

TEMP_DIR = Path("output/.temp_parcels")
OUTPUT_FILE = "output/portugal_parcels.geojson"
MAX_CONCURRENT = 5


def log(msg):
    print(msg, flush=True)


def get_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def download_inspire_parcels(session):
    """Download INSPIRE cadastral parcels from DGT WFS."""
    log("\n=== Downloading INSPIRE Cadastral Parcels ===")
    
    wfs_url = "https://snicws.dgterritorio.gov.pt/geoserver/wfs"
    output_subdir = TEMP_DIR / "inspire"
    output_subdir.mkdir(parents=True, exist_ok=True)
    
    count_params = {"service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeName": "inspire:cadastralparcel", "resultType": "hits"}
    async with session.get(wfs_url, params=count_params) as resp:
        root = ET.fromstring(await resp.text())
        total = int(root.attrib.get('numberMatched', 0))
    
    log(f"Total INSPIRE parcels: {total:,}")
    
    batch_size = 50000
    num_batches = (total + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async def download_batch(batch_num):
        async with semaphore:
            output_file = output_subdir / f"{batch_num:04d}.geojson"
            if output_file.exists():
                return True
            
            params = {
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeName": "inspire:cadastralparcel", "outputFormat": "application/json",
                "startIndex": (batch_num - 1) * batch_size, "count": batch_size
            }
            try:
                async with session.get(wfs_url, params=params, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        with open(output_file, 'w') as f:
                            json.dump(data, f)
                        log(f"INSPIRE {batch_num}/{num_batches}: {len(data.get('features', [])):,} features")
                        return True
            except Exception as e:
                log(f"INSPIRE {batch_num}: failed - {e}")
            return False
    
    await asyncio.gather(*[download_batch(i+1) for i in range(num_batches)])


async def download_rgg_parcels(session):
    """Download RGG parcels from BUPi MapServer."""
    log("\n=== Downloading RGG Parcels ===")
    
    url = "https://geo.bupi.gov.pt/gisbupi/rest/services/opendata/RGG_DadosGovPT/MapServer/0/query"
    output_subdir = TEMP_DIR / "rgg"
    output_subdir.mkdir(parents=True, exist_ok=True)
    
    async with session.get(url, params={"where": "1=1", "returnCountOnly": "true", "f": "json"}) as resp:
        total = (await resp.json()).get('count', 0)
    
    log(f"Total RGG parcels: {total:,}")
    
    batch_size = 2000
    num_batches = (total + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    
    async def download_batch(batch_num):
        async with semaphore:
            output_file = output_subdir / f"{batch_num:04d}.geojson"
            if output_file.exists():
                return True
            
            params = {
                "where": "1=1", "outFields": "*", "returnGeometry": "true",
                "outSR": "4326", "resultOffset": (batch_num - 1) * batch_size,
                "resultRecordCount": batch_size, "f": "geojson"
            }
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        with open(output_file, 'w') as f:
                            json.dump(data, f)
                        if batch_num % 50 == 0:
                            log(f"RGG {batch_num}/{num_batches}: {len(data.get('features', [])):,} features")
                        return True
            except Exception as e:
                log(f"RGG {batch_num}: failed - {e}")
            return False
    
    await asyncio.gather(*[download_batch(i+1) for i in range(num_batches)])
    log(f"RGG download complete")


def normalize_and_merge():
    """Load, normalize, and merge all parcels into single GeoJSON."""
    log("\n=== Normalizing and Merging ===")
    
    # Load INSPIRE (EPSG:3763)
    log("Loading INSPIRE parcels...")
    inspire_files = sorted((TEMP_DIR / "inspire").glob("*.geojson"))
    inspire_gdfs = [gpd.read_file(f) for f in inspire_files]
    inspire = gpd.GeoDataFrame(pd.concat(inspire_gdfs, ignore_index=True), crs="EPSG:3763")
    log(f"INSPIRE: {len(inspire):,} features")
    
    log("Reprojecting INSPIRE to EPSG:4326...")
    inspire = inspire.to_crs("EPSG:4326")
    
    inspire_norm = gpd.GeoDataFrame({
        'id': inspire.get('inspireid', inspire.index.astype(str)),
        'reference': inspire.get('nationalcadastralreference', ''),
        'area_m2': inspire.get('areavalue', None),
        'source': 'inspire',
        'geometry': inspire.geometry
    }, crs="EPSG:4326")
    
    # Load RGG (already EPSG:4326)
    log("Loading RGG parcels...")
    rgg_files = sorted((TEMP_DIR / "rgg").glob("*.geojson"))
    rgg_gdfs = [gpd.read_file(f) for f in rgg_files]
    rgg = gpd.GeoDataFrame(pd.concat(rgg_gdfs, ignore_index=True), crs="EPSG:4326")
    log(f"RGG: {len(rgg):,} features")
    
    rgg_norm = gpd.GeoDataFrame({
        'id': rgg.get('objectid', rgg.index.astype(str)).astype(str),
        'reference': '',
        'area_m2': rgg.get('st_area(shape)', None),
        'source': 'rgg',
        'geometry': rgg.geometry
    }, crs="EPSG:4326")
    
    # Merge
    log("Merging...")
    combined = gpd.GeoDataFrame(
        pd.concat([inspire_norm, rgg_norm], ignore_index=True),
        crs="EPSG:4326"
    )
    log(f"Total: {len(combined):,} features")
    
    # Save
    log(f"Saving to {OUTPUT_FILE}...")
    combined.to_file(OUTPUT_FILE, driver="GeoJSON")
    log("Done.")


async def main():
    TEMP_DIR.mkdir(exist_ok=True)
    
    connector = aiohttp.TCPConnector(ssl=get_ssl_context(), limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=connector) as session:
        await download_inspire_parcels(session)
        await download_rgg_parcels(session)
    
    normalize_and_merge()
    
    log("\nCleaning up temp files...")
    shutil.rmtree(TEMP_DIR)
    
    log(f"\nOutput: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
