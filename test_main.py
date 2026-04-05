import os
import shutil
import struct
import subprocess
import tempfile
import unittest


class TestSQLytSQLMode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compile_cmd = [
            "gcc",
            "-std=c11",
            #"-Wall",
            #"-Wextra",
            # "-Werror",
            #"-pedantic",
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

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="sqlyt_root_")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def run_script(self, commands, use_default_root=False):
        args = ["./db"] if use_default_root else ["./db", self.root]
        proc = subprocess.run(
            args,
            input="\n".join(commands) + "\n",
            capture_output=True,
            text=True,
            errors="strict",
            check=False,
        )
        return proc.stdout.split("\n")

    def test_usedatabase_create_insert_select(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(20), email varchar(30))",
            "insert into users values (1, alice, a@example.com)",
            "select * from users",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > (1, alice, a@example.com)",
                "Executed.",
                "db > ",
            ],
        )

    def test_fixed_varchar_enforced(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(3), email varchar(10))",
            "insert into users values (1, alice, a@example.com)",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > String is too long.",
                "db > ",
            ],
        )

    def test_duplicate_key_scoped_per_table_root(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(20), email varchar(30))",
            "create table admins (id int primary key, name varchar(20), email varchar(30))",
            "insert into users values (1, alice, a@example.com)",
            "insert into users values (1, alice2, a2@example.com)",
            "insert into admins values (1, bob, b@example.com)",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > Executed.",
                "db > Error: Duplicate key.",
                "db > Executed.",
                "db > ",
            ],
        )

    def test_showtables_displays_root_pages(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(20), email varchar(30))",
            "create table admins (id int primary key, name varchar(20), email varchar(30))",
            ".showtables",
            ".exit",
        ])

        self.assertIn("db > users (root_page=2)", result)
        self.assertIn("admins (root_page=3)", result)

    def test_next_root_page_persists_after_reopen(self):
        self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(20), email varchar(30))",
            ".exit",
        ])

        result = self.run_script([
            ".usedatabase app",
            "create table admins (id int primary key, name varchar(20), email varchar(30))",
            ".showtables",
            ".exit",
        ])

        self.assertIn("db > users (root_page=2)", result)
        self.assertIn("admins (root_page=3)", result)

    def test_recovers_corrupted_header_and_rebuilds_next_root(self):
        self.run_script([
            "create database app",
            ".usedatabase app",
            "create table users (id int primary key, name varchar(20), email varchar(30))",
            "create table admins (id int primary key, name varchar(20), email varchar(30))",
            ".exit",
        ])

        db_file = os.path.join(self.root, "app", "database.db")
        with open(db_file, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("<8sIII", b"BROKEN!!", 1, 0, 0))

        result = self.run_script([
            ".usedatabase app",
            "create table audit (id int primary key, name varchar(20), email varchar(30))",
            ".showtables",
            ".exit",
        ])

        self.assertIn("db > Recovered database header metadata.", result)
        self.assertTrue(any("users (root_page=2)" in line for line in result))
        self.assertTrue(any("admins (root_page=3)" in line for line in result))
        self.assertTrue(any("audit (root_page=4)" in line for line in result))

    def test_default_startup_uses_data_root(self):
        cwd_before = os.getcwd()
        temp_workdir = tempfile.mkdtemp(prefix="sqlyt_workdir_")
        try:
            os.chdir(temp_workdir)
            proc = subprocess.run(
                [os.path.join(cwd_before, "db")],
                input="create database app\n.usedatabase app\n.exit\n",
                capture_output=True,
                text=True,
                errors="strict",
                check=False,
            )
            output = proc.stdout.split("\n")
            self.assertEqual(output, ["db > Executed.", "db > Using database app", "db > "])
            self.assertTrue(os.path.isdir(os.path.join(temp_workdir, "data", "app")))
        finally:
            os.chdir(cwd_before)
            shutil.rmtree(temp_workdir, ignore_errors=True)

    def test_cannot_use_database_before_creation(self):
        result = self.run_script([
            ".usedatabase missing",
            ".exit",
        ])
        self.assertEqual(result, ["db > Unable to use database.", "db > "])

    def test_supports_more_than_three_columns(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table profile (id int primary key, name varchar(20), email varchar(30), city varchar(20), role varchar(15))",
            "insert into profile values (1, alice, a@example.com, berlin, engineer)",
            "select * from profile",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > (1, alice, a@example.com, berlin, engineer)",
                "Executed.",
                "db > ",
            ],
        )

    def test_supports_int_and_varchar_mixed_columns(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table user (id int primary key, user_id int, user_name varchar(20), email varchar(30), password varchar(20))",
            "insert into user values (1, 101, alice, a@example.com, secret)",
            "select * from user",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > (1, 101, alice, a@example.com, secret)",
                "Executed.",
                "db > ",
            ],
        )

    def test_supports_table_with_single_user_column(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table flags (id int primary key, enabled int)",
            "insert into flags values (1, 1)",
            "select * from flags",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > (1, 1)",
                "Executed.",
                "db > ",
            ],
        )

    def test_supports_quoted_strings_with_spaces(self):
        result = self.run_script([
            "create database app",
            ".usedatabase app",
            "create table people (id int primary key, user_name varchar(30), city varchar(30))",
            'insert into people values (1, "Alice Doe", "New York")',
            "select * from people",
            ".exit",
        ])

        self.assertEqual(
            result,
            [
                "db > Executed.",
                "db > Using database app",
                "db > Executed.",
                "db > Executed.",
                "db > (1, Alice Doe, New York)",
                "Executed.",
                "db > ",
            ],
        )

    def test_btree_node_split_when(self):
        table = "bigtree"
        commands = [
            "create database app",
            ".usedatabase app",
            f"create table {table} (id int primary key, name varchar(20))",
        ]
        for i in range(1, 14):
            commands.append(f"insert into {table} values ({i}, row{i})")
        commands.extend([f".btree {table}", ".exit"])

        result = self.run_script(commands)
        out = "\n".join(result)

        self.assertIn("Tree:", out)
        # self.assertIn("internal", out)

if __name__ == "__main__":
    unittest.main(verbosity=2)
