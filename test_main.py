import subprocess
import os
import tempfile
import unittest


class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compile_cmd = [
            "gcc",
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pedantic",
            "main.c",
            "-o",
            "db",
        ]
        result = subprocess.run(compile_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                "Failed to compile db:\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )

    def make_db_path(self):
        fd, path = tempfile.mkstemp(prefix="sqlyt_", suffix=".db")
        os.close(fd)
        return path

    def run_script(self, commands, db_filename):
        input_text = "\n".join(commands) + "\n"
        proc = subprocess.run(
            ["./db", db_filename],
            input=input_text,
            capture_output=True,
            text=True,
            errors="strict",
            check=False,
        )
        return proc.stdout.split("\n")

    def test_inserts_and_retrieves_a_row(self):
        db_filename = self.make_db_path()
        try:
            result = self.run_script([
                "insert 1 user1 person1@example.com",
                "select",
                ".exit",
            ], db_filename)
            self.assertCountEqual(
                result,
                [
                    "db > Executed.",
                    "db > (1, user1, person1@example.com)",
                    "Executed.",
                    "db > ",
                ],
            )
        finally:
            os.remove(db_filename)

    def test_prints_error_message_when_table_is_full(self):
        db_filename = self.make_db_path()
        script = [
            f"insert {i} user{i} person{i}@example.com"
            for i in range(1, 1402)
        ]
        script.append(".exit")
        try:
            result = self.run_script(script, db_filename)
            self.assertEqual(result[-2], "db > Error: Table full.")
        finally:
            os.remove(db_filename)

    def test_allows_inserting_strings_that_are_the_maximum_length(self):
        db_filename = self.make_db_path()
        long_username = "a" * 32
        long_email = "a" * 255
        try:
            result = self.run_script([
                f"insert 1 {long_username} {long_email}",
                "select",
                ".exit",
            ], db_filename)
            self.assertCountEqual(
                result,
                [
                    "db > Executed.",
                    f"db > (1, {long_username}, {long_email})",
                    "Executed.",
                    "db > ",
                ],
            )
        finally:
            os.remove(db_filename)

    def test_prints_error_message_if_strings_are_too_long(self):
        db_filename = self.make_db_path()
        long_username = "a" * 33
        long_email = "a" * 256
        try:
            result = self.run_script([
                f"insert 1 {long_username} {long_email}",
                "select",
                ".exit",
            ], db_filename)
            self.assertCountEqual(
                result,
                [
                    "db > String is too long.",
                    "db > Executed.",
                    "db > ",
                ],
            )
        finally:
            os.remove(db_filename)

    def test_prints_an_error_message_if_id_is_negative(self):
        db_filename = self.make_db_path()
        try:
            result = self.run_script([
                "insert -1 cstack foo@bar.com",
                "select",
                ".exit",
            ], db_filename)
            self.assertCountEqual(
                result,
                [
                    "db > ID must be positive.",
                    "db > Executed.",
                    "db > ",
                ],
            )
        finally:
            os.remove(db_filename)

    def test_keeps_data_after_closing_connection(self):
        db_filename = self.make_db_path()
        try:
            self.run_script([
                "insert 1 user1 person1@example.com",
                ".exit",
            ], db_filename)

            result = self.run_script([
                "select",
                ".exit",
            ], db_filename)

            self.assertCountEqual(
                result,
                [
                    "db > (1, user1, person1@example.com)",
                    "Executed.",
                    "db > ",
                ],
            )
        finally:
            os.remove(db_filename)


if __name__ == "__main__":
    unittest.main(verbosity=2)
