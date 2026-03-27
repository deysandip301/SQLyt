import subprocess
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

    def run_script(self, commands):
        input_text = "\n".join(commands) + "\n"
        proc = subprocess.run(
            ["./db"],
            input=input_text,
            capture_output=True,
            text=True,
            errors="strict",
            check=False,
        )
        return proc.stdout.split("\n")

    def test_inserts_and_retrieves_a_row(self):
        result = self.run_script([
            "insert 1 user1 person1@example.com",
            "select",
            ".exit",
        ])
        self.assertCountEqual(
            result,
            [
                "db > Executed.",
                "db > (1, user1, person1@example.com)",
                "Executed.",
                "db > ",
            ],
        )

    def test_prints_error_message_when_table_is_full(self):
        script = [
            f"insert {i} user{i} person{i}@example.com"
            for i in range(1, 1402)
        ]
        script.append(".exit")
        result = self.run_script(script)
        self.assertEqual(result[-2], "db > Error: Table full.")

    def test_allows_inserting_strings_that_are_the_maximum_length(self):
        long_username = "a" * 32
        long_email = "a" * 255
        result = self.run_script([
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit",
        ])
        self.assertCountEqual(
            result,
            [
                "db > Executed.",
                f"db > (1, {long_username}, {long_email})",
                "Executed.",
                "db > ",
            ],
        )

    def test_prints_error_message_if_strings_are_too_long(self):
        long_username = "a" * 33
        long_email = "a" * 256
        result = self.run_script([
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit",
        ])
        self.assertCountEqual(
            result,
            [
                "db > String is too long.",
                "db > Executed.",
                "db > ",
            ],
        )

    def test_prints_an_error_message_if_id_is_negative(self):
        result = self.run_script([
            "insert -1 cstack foo@bar.com",
            "select",
            ".exit",
        ])
        self.assertCountEqual(
            result,
            [
                "db > ID must be positive.",
                "db > Executed.",
                "db > ",
            ],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
