import asyncio
import aiohttp
import aioboto3
import os
import time
import logging
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

#logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@dataclass
class FilingMetadata:
    """Metadata for a SEC filing"""
    ticker: str
    cik: str
    company_name: str
    accession_number: str
    filing_date: str
    primary_document: str
    doc_url: str


class RateLimiter:
    """Token bucket rate limiter for SEC API (10 requests/second)"""

    def __init__(self, rate):
        # requests / second
        self.rate = rate
        self.tokens = float(rate)
        self.max_tokens = float(rate)
        self.lock = asyncio.Lock()
        self.last_refill = time.time()

    async def acquire(self):
        """Token management for making requests"""
        # check for lock
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill

            # Refill tokens based on elapsed time
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
            self.last_refill = now

            # If no tokens available, wait
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 1.0
                self.last_refill = time.time()

            # Consume one token
            self.tokens -= 1.0


class Downloader:
    """Async SEC filing downloader with direct S3 upload"""

    def __init__(self):
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.region = os.getenv("AWS_REGION")

        if not self.bucket_name:
            raise ValueError("AWS_BUCKET_NAME not found in environment variables")

        # Rate limiter (RATE =  requests/second)
        self.rate_limiter = RateLimiter(rate=10)

        # SEC API headers
        self.headers = {
            'User-Agent': 'UnCovered Research Tool research@example.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }

        # Companies to process
        self.companies = {
            'AAPL': '0000320193',  # Apple
            'MSFT': '0000789019',  # Microsoft
            'NVDA': '0001045810',  # Nvidia
            'GOOGL': '0001652044', # Alphabet/Google
            'AMZN': '0001018724',  # Amazon
            'META': '0001326801',  # Meta/Facebook
            'BRK-A': '0001067983', # Berkshire Hathaway
            'TSLA': '0001318605',  # Tesla
            'LLY': '0000059478',   # Eli Lilly
            'AVGO': '0001730168',  # Broadcom
            'WMT': '0000104169',   # Walmart
            'JPM': '0000019617',   # JPMorgan Chase
            'V': '0001403161',     # Visa
            'UNH': '0000731766',   # UnitedHealth
            'XOM': '0000034088',   # ExxonMobil
            'JNJ': '0000200406',   # Johnson & Johnson
            'MA': '0001141391',    # Mastercard
            'PG': '0000080424',    # Procter & Gamble
            'HD': '0000354950',    # Home Depot
            'COST': '0000909832',  # Costco
        }

        self.uploaded = 0
        self.failed = 0
        self.start_time = None

    @retry(
        # stop after 3 attempts
        stop=stop_after_attempt(3),
        # time to wait
        wait=wait_exponential(multiplier=2, min=2, max=10),
        # retry condition
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def fetch_metadata(self, session: aiohttp.ClientSession, ticker: str, cik: str) -> Optional[FilingMetadata]:
        """Fetch company metadata and find latest 10-K filing"""
        try:
            # add rate limiter
            await self.rate_limiter.acquire()

            # Fetch company submissions
            index_url = f"https://data.sec.gov/submissions/CIK{cik}.json"

            async with session.get(index_url) as response:
                response.raise_for_status()
                data = await response.json()

            company_name = data['name']
            filings = data['filings']['recent']

            # Find most recent 10-K filing
            filing_index = None
            for i, form_type in enumerate(filings['form']):
                if form_type == '10-K':
                    filing_index = i
                    break

            if filing_index is None:
                logger.warning(f"[{ticker}] No 10-K filing found")
                return None

            # Extract filing metadata
            accession_number = filings['accessionNumber'][filing_index]
            filing_date = filings['filingDate'][filing_index]
            primary_document = filings['primaryDocument'][filing_index]

            # find document URL
            accession_clean = accession_number.replace('-', '')
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_clean}/{primary_document}"


            logger.info(f"[{ticker}] Found 10-K: {company_name} ({filing_date})")

            # store filing metadata
            return FilingMetadata(
                ticker=ticker,
                cik=cik,
                company_name=company_name,
                accession_number=accession_number,
                filing_date=filing_date,
                primary_document=primary_document,
                doc_url=doc_url
            )

        except Exception as e:
            logger.error(f"[{ticker}] Metadata fetch failed: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def download_document(self, session: aiohttp.ClientSession, metadata: FilingMetadata) -> bytes:
        """Download 10-K document from SEC"""
        try:
            # Rate limit the request
            await self.rate_limiter.acquire()

            # get document content
            async with session.get(metadata.doc_url) as response:
                response.raise_for_status()
                content = await response.read()

            logger.info(f"[{metadata.ticker}] Downloaded document ({len(content)} bytes)")
            return content

        except Exception as e:
            logger.error(f"[{metadata.ticker}] Document download failed: {e}")
            raise

    async def upload_to_s3(self, metadata: FilingMetadata, content: bytes):
        """Upload document to S3"""
        try:
            year = metadata.filing_date.split('-')[0]
            filename = f"{metadata.ticker}-10K-{year}.html"
            s3_key = f"SEC/{filename}"

            # Use aioboto3 for async S3 upload
            session = aioboto3.Session()
            async with session.client('s3', region_name=self.region) as s3_client:
                await s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=content,
                    ContentType='text/html'
                )

            logger.info(f"[{metadata.ticker}] ✓ Uploaded to S3: {s3_key}")

        except Exception as e:
            logger.error(f"[{metadata.ticker}] S3 upload failed: {e}")
            raise

    async def process_company(self, session: aiohttp.ClientSession, ticker: str, cik: str):
        """Process a single company: fetch metadata -> download -> upload"""
        try:
            # Fetch metadata
            metadata = await self.fetch_metadata(session, ticker, cik)
            if not metadata:
                self.failed += 1
                return

            # Download document
            content = await self.download_document(session, metadata)

            # Upload to S3
            await self.upload_to_s3(metadata, content)

            self.uploaded += 1

        except Exception as e:
            logger.error(f"[{ticker}] ✗ Failed: {e}")
            self.failed += 1

    async def run(self):
        """Start Processing"""
        self.start_time = time.time()

        print("=" * 60)
        print("SEC EDGAR 10-K Filings Async Uploader to S3")
        print("Top 20 Most Valuable Companies")
        print("=" * 60)
        print(f"Bucket: {self.bucket_name}")
        print(f"Region: {self.region}")
        print(f"Processing {len(self.companies)} companies concurrently...")
        print()

        # Create aiohttp session
        async with aiohttp.ClientSession(headers=self.headers) as session:
            # Create tasks for all companies
            tasks = [
                self.process_company(session, ticker, cik)
                for ticker, cik in self.companies.items()
            ]

            # Run all tasks concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

        elapsed = time.time() - self.start_time

        print()
        print("=" * 60)
        print(f"Upload Complete!")
        print(f"Success: {self.uploaded} | Failed: {self.failed}")
        print(f"Time elapsed: {elapsed:.2f} seconds")
        print(f"Files uploaded to S3 bucket: {self.bucket_name}/SEC/")
        print("=" * 60)


if __name__ == "__main__":
    try:
        downloader = Downloader()
        asyncio.run(downloader.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
