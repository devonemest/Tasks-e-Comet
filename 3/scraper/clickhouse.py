from clickhouse_driver import Client
from typing import Any, List, Dict


class ClickHouseClient:
    def __init__(self, host: str, database: str, user: str, password: str):
        self.client = Client(
            host=host,
            database=database,
            user=user,
            password=password)

    def execute(self, query: str, params: Any = None) -> None:
        try:
            self.client.execute(query, params)
        except Exception as e:
            print(f"Ошибка выполнения запроса: {e}")
            raise

    def insert_repository(self, repository_data: Dict[str, Any]) -> None:
        query = """
        INSERT INTO test.repositories (
        name, owner, stars, watchers, forks, language, updated
        )
        VALUES
        """
        values = [
            (repository_data["name"], repository_data["owner"],
             repository_data["stars"], repository_data["watchers"],
             repository_data["forks"], repository_data["language"],
             repository_data["updated"])
        ]
        self.execute(query, values)

    def insert_repository_author_commits(
            self, commit_data: List[Dict[str, Any]]) -> None:
        query = """
        INSERT INTO test.repositories_authors_commits (
        date, repo, author, commits_num
        )
        VALUES
        """
        values = [
            (data["date"], data["repo"],
             data["author"], data["commits_num"]) for data in commit_data
        ]
        self.execute(query, values)

    def insert_repository_position(
            self, position_data: Dict[str, Any]) -> None:
        query = """
        INSERT INTO test.repositories_positions (date, repo, position)
        VALUES
        """
        values = [
            (position_data["date"],
             position_data["repo"],
             position_data["position"])
        ]
        self.execute(query, values)
