import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
import logging
import json
from urllib.parse import urljoin, urlparse
import time
from dataclasses import dataclass
import pandas as pd

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ScraperConfig:
    base_url: str
    max_concurrent: int = 10
    request_delay: float = 1.0
    timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    output_format: str = "json"  # json, csv, excel

class AsyncWebScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.visited_urls = set()
        self.session = None
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.results = []
        self.domain = urlparse(config.base_url).netloc

    async def _get_session(self) -> aiohttp.ClientSession:
        """Создание aiohttp сессии с настройками"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            connector = aiohttp.TCPConnector(limit=self.config.max_concurrent)
            headers = {"User-Agent": self.config.user_agent}
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=headers
            )
        return self.session

    async def fetch_page(self, url: str) -> Optional[str]:
        """Асинхронная загрузка страницы с ограничением скорости"""
        if url in self.visited_urls:
            return None
            
        async with self.semaphore:
            try:
                session = await self._get_session()
                async with session.get(url) as response:
                    if response.status == 200:
                        self.visited_urls.add(url)
                        await asyncio.sleep(self.config.request_delay)
                        return await response.text()
                    else:
                        logger.warning(f"Status {response.status} for {url}")
                        return None
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                return None

    def parse_page(self, html: str, url: str) -> List[Dict]:
        """Парсинг HTML с помощью BeautifulSoup"""
        soup = BeautifulSoup(html, 'html.parser')
        data = []
        
        # Пример: извлечение всех статей с страницы
        for article in soup.find_all('article'):
            try:
                item = {
                    'title': article.find('h2').get_text(strip=True),
                    'url': urljoin(url, article.find('a')['href']),
                    'summary': article.find('p').get_text(strip=True),
                    'source_url': url,
                    'timestamp': time.time()
                }
                data.append(item)
            except Exception as e:
                logger.error(f"Error parsing article: {e}")
                continue
                
        return data

    async def scrape_site(self, start_url: str = None, max_pages: int = 50):
        """Основной метод скрапинга с рекурсивным обходом"""
        url = start_url or self.config.base_url
        html = await self.fetch_page(url)
        
        if not html or len(self.results) >= max_pages:
            return
            
        page_data = self.parse_page(html, url)
        self.results.extend(page_data)
        logger.info(f"Scraped {url}, found {len(page_data)} items")
        
        # Рекурсивный обход ссылок (пример)
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a', href=True):next_url = urljoin(url, link['href'])
            if (urlparse(next_url).netloc == self.domain and 
                next_url not in self.visited_urls):
                await self.scrape_site(next_url, max_pages)

    async def close(self):
        """Корректное закрытие сессии"""
        if self.session and not self.session.closed:
            await self.session.close()

    def save_results(self, filename: str = "scraped_data"):
        """Сохранение результатов в выбранном формате"""
        if not self.results:
            logger.warning("No results to save")
            return
            
        if self.config.output_format == "json":
            with open(f"{filename}.json", 'w', encoding='utf-8') as f:
                json.dump(self.results, f, ensure_ascii=False, indent=2)
        elif self.config.output_format == "csv":
            pd.DataFrame(self.results).to_csv(f"{filename}.csv", index=False)
        elif self.config.output_format == "excel":
            pd.DataFrame(self.results).to_excel(f"{filename}.xlsx", index=False)
            
        logger.info(f"Saved {len(self.results)} items to {filename}.{self.config.output_format}")

# Пример использования
async def main():
    config = ScraperConfig(
        base_url="https://example.com/news",
        max_concurrent=5,
        request_delay=1.5,
        output_format="json"
    )
    
    scraper = AsyncWebScraper(config)
    try:
        await scraper.scrape_site(max_pages=100)
    finally:
        await scraper.close()
    
    scraper.save_results("news_data")

if name == "__main__":
    asyncio.run(main())
