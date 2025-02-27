import asyncio
import logging
import os
from dotenv import load_dotenv
import yaml
from scraper.github_scraper import GithubReposScrapper
from scraper.clickhouse import ClickHouseClient
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config():
    try:
        with open("config.yaml", "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Ошибка загрузки конфигурации: {e}")
        raise


async def main():

    clickhouse_client = ClickHouseClient(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        database=os.getenv("CLICKHOUSE_DATABASE", "test"),
        user=os.getenv("CLICKHOUSE_USER", "test_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "test_password")
    )

    scraper = GithubReposScrapper(
        access_token=os.getenv("GITHUB_ACCESS_TOKEN"),
        clickhouse_client=clickhouse_client
    )
    try:
        repositories = await scraper.get_repositories()
        logger.info(f"Получено {len(repositories)} репозиториев")
    except Exception as e:
        logger.error(f"Ошибка при получении данных: {e}")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
