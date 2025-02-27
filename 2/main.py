import asyncio
from datetime import datetime, timedelta, timezone
from aiohttp import ClientSession, ClientError
from aiolimiter import AsyncLimiter
from dataclasses import dataclass
from typing import Final, Any

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"
MCR: Final[int] = 20
RPS: Final[int] = 10


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
    def __init__(self, access_token: str):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )

        self._limiter = AsyncLimiter(RPS)
        self._semaphore = asyncio.Semaphore(MCR)

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
            print(f"Ошибка запроса: {e}")
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
            print(f"Ошибка при получении репозиториев: {e}")
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

            return Repository(
                name=name,
                owner=owner,
                position=repo.get("id", 0),
                stars=repo.get("stargazers_count", 0),
                watchers=repo.get("watchers_count", 0),
                forks=repo.get("forks_count", 0),
                language=repo.get("language", "Unknown"),
                authors_commits_num_today=authors_commits_num_today,
            )
        except KeyError as e:
            print(f"Ошибка: отсутствует ключ {e}")
            return Repository(name="Unknown", owner="Unknown", position=0,
                              stars=0, watchers=0, forks=0, language="Unknown",
                              authors_commits_num_today=[])

    async def close(self):
        await self._session.close()
