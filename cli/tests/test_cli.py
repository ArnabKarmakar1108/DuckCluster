from __future__ import annotations

import unittest

from duckcluster.format import format_query_result
from duckcluster.validate import ValidationError, extract_table_names, validate_tables_and_shards


class ValidateTest(unittest.TestCase):
    def test_extract_table_names(self) -> None:
        sql = "SELECT * FROM events e JOIN categories c ON e.id = c.id"
        self.assertEqual(extract_table_names(sql), ["events", "categories"])

    def test_unknown_table_raises(self) -> None:
        shards = {
            "tables": [
                {
                    "tableName": "events",
                    "shards": [{"shardId": 0, "owners": ["worker-1"]}],
                }
            ]
        }
        with self.assertRaisesRegex(ValidationError, "unknown table"):
            validate_tables_and_shards("SELECT * FROM lineitem", shards)

    def test_missing_shard_owners_raises(self) -> None:
        shards = {
            "tables": [
                {
                    "tableName": "events",
                    "shards": [
                        {"shardId": 0, "owners": ["worker-1"]},
                        {"shardId": 1, "owners": []},
                    ],
                }
            ]
        }
        with self.assertRaisesRegex(ValidationError, "without owners"):
            validate_tables_and_shards("SELECT * FROM events", shards)


class FormatTest(unittest.TestCase):
    def test_table_output_includes_rows(self) -> None:
        rendered = format_query_result(
            {
                "columns": ["category", "cnt"],
                "rows": [["A", 3], ["B", 2]],
                "stats": {"durationMs": 42},
            },
            "table",
        )
        self.assertIn("category", rendered)
        self.assertIn("(2 rows in 42 ms)", rendered)


class ReadSqlTest(unittest.TestCase):
    def test_missing_sql_file(self) -> None:
        from argparse import Namespace

        from duckcluster.__main__ import _read_sql

        args = Namespace(file="missing-report.sql", sql=None)
        with self.assertRaisesRegex(OSError, "SQL file not found"):
            _read_sql(args)


class BannerTest(unittest.TestCase):
    def test_embedded_duck_banner(self) -> None:
        from duckcluster.banner import _DUCK_ASCII

        self.assertIn("#", _DUCK_ASCII)
        self.assertGreater(_DUCK_ASCII.count("#"), 20)


class HistoryTest(unittest.TestCase):
    def test_read_command_undoes_readline_auto_add(self) -> None:
        import unittest.mock

        import duckcluster.input as input_mod

        mock_readline = unittest.mock.Mock()
        mock_readline.get_current_history_length.return_value = 1
        mock_readline.get_history_item.return_value = "SELECT 1;"
        with unittest.mock.patch.object(input_mod, "readline", mock_readline):
            with unittest.mock.patch("builtins.input", return_value="SELECT 1;"):
                line = input_mod.read_command("duckcluster> ")
        self.assertEqual(line, "SELECT 1;")
        mock_readline.remove_history_item.assert_called_once_with(1)

    def test_remember_command_stores_entry_with_semicolon(self) -> None:
        import unittest.mock

        import duckcluster.input as input_mod

        mock_readline = unittest.mock.Mock()
        mock_readline.get_current_history_length.return_value = 0
        with unittest.mock.patch.object(input_mod, "readline", mock_readline):
            input_mod.remember_command("SELECT 1;")
        mock_readline.add_history.assert_called_once_with("SELECT 1;")

    def test_remember_command_skips_consecutive_duplicate(self) -> None:
        import unittest.mock

        import duckcluster.input as input_mod

        mock_readline = unittest.mock.Mock()
        mock_readline.get_current_history_length.return_value = 1
        mock_readline.get_history_item.return_value = "SELECT 1"
        with unittest.mock.patch.object(input_mod, "readline", mock_readline):
            input_mod.remember_command("SELECT 1")
        mock_readline.add_history.assert_not_called()


if __name__ == "__main__":
    unittest.main()
