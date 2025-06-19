import unittest
import os
import json
import csv
import tempfile
import shutil

from dotenv import load_dotenv

from mcp_clickhouse import create_clickhouse_client, list_databases, list_tables, run_select_query, save_query_results

load_dotenv()


class TestClickhouseTools(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the environment before tests."""
        cls.client = create_clickhouse_client()

        # Prepare test database and table
        cls.test_db = "test_tool_db"
        cls.test_table = "test_table"
        cls.client.command(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")

        # Drop table if exists to ensure clean state
        cls.client.command(f"DROP TABLE IF EXISTS {cls.test_db}.{cls.test_table}")

        # Create table with comments
        cls.client.command(f"""
            CREATE TABLE {cls.test_db}.{cls.test_table} (
                id UInt32 COMMENT 'Primary identifier',
                name String COMMENT 'User name field'
            ) ENGINE = MergeTree()
            ORDER BY id
            COMMENT 'Test table for unit testing'
        """)
        cls.client.command(f"""
            INSERT INTO {cls.test_db}.{cls.test_table} (id, name) VALUES (1, 'Alice'), (2, 'Bob')
        """)

    @classmethod
    def tearDownClass(cls):
        """Clean up the environment after tests."""
        cls.client.command(f"DROP DATABASE IF EXISTS {cls.test_db}")

    def test_list_databases(self):
        """Test listing databases."""
        result = list_databases()
        self.assertIn(self.test_db, result)

    def test_list_tables_without_like(self):
        """Test listing tables without a 'LIKE' filter."""
        result = list_tables(self.test_db)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], self.test_table)

    def test_list_tables_with_like(self):
        """Test listing tables with a 'LIKE' filter."""
        result = list_tables(self.test_db, like=f"{self.test_table}%")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], self.test_table)

    def test_run_select_query_success(self):
        """Test running a SELECT query successfully."""
        query = f"SELECT * FROM {self.test_db}.{self.test_table}"
        result = run_select_query(query)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)
        self.assertEqual(result["rows"][0][0], 1)
        self.assertEqual(result["rows"][0][1], "Alice")

    def test_run_select_query_failure(self):
        """Test running a SELECT query with an error."""
        query = f"SELECT * FROM {self.test_db}.non_existent_table"
        result = run_select_query(query)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["status"], "error")
        self.assertIn("Query failed", result["message"])

    def test_table_and_column_comments(self):
        """Test that table and column comments are correctly retrieved."""
        result = list_tables(self.test_db)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)

        table_info = result[0]
        # Verify table comment
        self.assertEqual(table_info["comment"], "Test table for unit testing")

        # Get columns by name for easier testing
        columns = {col["name"]: col for col in table_info["columns"]}

        # Verify column comments
        self.assertEqual(columns["id"]["comment"], "Primary identifier")
        self.assertEqual(columns["name"]["comment"], "User name field")

    def test_save_query_results_csv(self):
        """Test saving query results to CSV file."""
        test_dir = tempfile.mkdtemp()

        try:
            query = f"SELECT * FROM {self.test_db}.{self.test_table} ORDER BY id"
            filepath = os.path.join(test_dir, "test_results.csv")

            result = save_query_results(query, filepath, "csv")

            self.assertEqual(result["status"], "success")
            self.assertEqual(result["format"], "csv")
            self.assertEqual(result["rows_written"], 2)
            self.assertEqual(result["columns"], 2)

            self.assertTrue(os.path.exists(filepath))

            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)

            self.assertEqual(len(rows), 3)  # header + 2 data rows
            self.assertEqual(rows[0], ['id', 'name'])
            self.assertEqual(rows[1], ['1', 'Alice'])
            self.assertEqual(rows[2], ['2', 'Bob'])

        finally:
            shutil.rmtree(test_dir)

    def test_save_query_results_json(self):
        """Test saving query results to JSON file."""
        test_dir = tempfile.mkdtemp()

        try:
            query = f"SELECT * FROM {self.test_db}.{self.test_table} ORDER BY id"
            filepath = os.path.join(test_dir, "test_results.json")

            result = save_query_results(query, filepath, "json")

            self.assertEqual(result["status"], "success")
            self.assertEqual(result["format"], "json")
            self.assertEqual(result["rows_written"], 2)

            self.assertTrue(os.path.exists(filepath))

            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.assertEqual(len(data), 2)
            self.assertEqual(data[0], {'id': 1, 'name': 'Alice'})
            self.assertEqual(data[1], {'id': 2, 'name': 'Bob'})

        finally:
            shutil.rmtree(test_dir)


if __name__ == "__main__":
    unittest.main()
