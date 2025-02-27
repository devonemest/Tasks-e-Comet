
import asyncio
from datetime import datetime, timedelta, timezone
from aiohttp import ClientSession, ClientError
from aiolimiter import AsyncLimiter
from dataclasses import dataclass
from typing import Final, Any
from scraper.clickhouse import ClickHouseClient
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"
MCR: Final[int] = 30
RPS: Final[int] = 50


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class GithubReposScrapper:
    def __init__(self, access_token: str, clickhouse_client: ClickHouseClient):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        self._limiter = AsyncLimiter(RPS)
        self._semaphore = asyncio.Semaphore(MCR)
        self._clickhouse_client = clickhouse_client

    async def _make_request(self, endpoint: str, method: str = "GET",
                            params: dict[str, Any] | None = None) -> Any:
        try:
            async with self._limiter:
                async with self._session.request(
                        method,
                        f"{GITHUB_API_BASE_URL}/{endpoint}",
                        params=params
                ) as response:
                    response.raise_for_status()
                    return await response.json()
        except ClientError as e:
            logger.error(f"Ошибка запроса: {e}")
            return {}

    async def _get_top_repositories(
            self, limit: int = 100) -> list[dict[str, Any]]:
        data = await self._make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": limit},
        )
        return data.get("items", [])

    async def _get_repository_commits(
            self, owner: str, repo: str) -> list[dict[str, Any]]:
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        async with self._semaphore:
            commits = await self._make_request(
                endpoint=f"repos/{owner}/{repo}/commits",
                params={"since": since},
            )
        return commits if isinstance(commits, list) else []

    async def get_repositories(self) -> list[Repository]:
        try:
            top_repos = await self._get_top_repositories()
            tasks = [self._process_repository(repo) for repo in top_repos]
            return await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Ошибка при получении репозиториев: {e}")
            return []

    async def _process_repository(self, repo: dict[str, Any]) -> Repository:
        try:
            owner, name = repo["owner"]["login"], repo["name"]
            commits = await self._get_repository_commits(owner, name)

            authors = {}
            for commit in commits:
                author = commit.get(
                    "commit",
                    {}).get(
                    "author",
                    {}).get(
                    "name",
                    "Unknown")
                authors[author] = authors.get(author, 0) + 1
            authors_commits_num_today = [
                RepositoryAuthorCommitsNum(
                    author,
                    num) for author,
                num in authors.items()]

            repository_data = {
                "name": name or 'Unknown',
                "owner": owner or 'Unknown',
                "stars": repo.get("stargazers_count", 0) or 0,
                "watchers": repo.get("watchers_count", 0) or 0,
                "forks": repo.get("forks_count", 0) or 0,
                "language": repo.get("language", "Unknown") or 'Unknown',
                "updated": datetime.now(timezone.utc),
            }

            self._clickhouse_client.insert_repository(repository_data)

            commit_data = [
                {
                    "date": datetime.now().date(),
                    "repo": name,
                    "author": author_commits.author,
                    "commits_num": author_commits.commits_num,
                }
                for author_commits in authors_commits_num_today
            ]
            self._clickhouse_client.insert_repository_author_commits(
                commit_data)

            position_data = {
                "date": datetime.now().date(),
                "repo": name,
                "position": repo.get("id", 0),
            }
            self._clickhouse_client.insert_repository_position(position_data)

            return Repository(
                name=name or 'Unknown',
                owner=owner or 'Unknown',
                position=repo.get("id", 0) or 0,
                stars=repo.get("stargazers_count", 0) or 0,
                watchers=repo.get("watchers_count", 0) or 0,
                forks=repo.get("forks_count", 0) or 0,
                language=repo.get("language", "Unknown") or 'Unknown',
                authors_commits_num_today=authors_commits_num_today,
            )
        except KeyError as e:
            logger.error(f"Ошибка: отсутствует ключ{e}")
            return Repository(name="Unknown", owner="Unknown", position=0,
                              stars=0, watchers=0, forks=0, language="Unknown",
                              authors_commits_num_today=[])

    async def close(self):
        await self._session.close()
