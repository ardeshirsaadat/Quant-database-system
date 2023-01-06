import aiohttp
import asyncio
import urllib
import urllib.parse
import db_logs

async def async_aiohttp_get_all(urls, fmt="json"):
    async with aiohttp.ClientSession() as session:
        async def fetch(url, tries=0):
            async with session.get(url) as response:
                db_logs.DBLogs().debug("try {} {}".format(url, tries))
                try:
                    if fmt == "json":
                        return await response.json()
                except Exception:
                    if tries <= 5:
                        return await fetch(url, tries+1)
                    return None
        return await asyncio.gather(*[
            fetch(url) for url in urls
        ])