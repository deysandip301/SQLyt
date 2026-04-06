#include "sqlyt.h"

static void normalize_input_line(InputBuffer* input_buffer) {
  char* s;
  size_t len;
  size_t start = 0;
  size_t end;

  if (input_buffer == NULL || input_buffer->buffer == NULL) {
    return;
  }

  s = input_buffer->buffer;
  len = strlen(s);

  while (start < len && (s[start] == ' ' || s[start] == '\t' ||
                         s[start] == '\n' || s[start] == '\r')) {
    start++;
  }

  end = len;
  while (end > start &&
         (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\n' ||
          s[end - 1] == '\r')) {
    end--;
  }

  if (start > 0 && end >= start) {
    memmove(s, s + start, end - start);
  }
  s[end - start] = '\0';
  input_buffer->input_length = (ssize_t)(end - start);
  input_buffer->buffer_length = (size_t)input_buffer->input_length + 1;
}

int main(int argc, char* argv[]) {
  Session session;
  InputBuffer* input_buffer;
  SqlStatement* stmt;
  const char* root_path;

  memset(&session, 0, sizeof(session));

  root_path = (argc >= 2) ? argv[1] : DEFAULT_ROOT_PATH;
  strncpy(session.root_path, root_path, sizeof(session.root_path) - 1);
  session.root_path[sizeof(session.root_path) - 1] = '\0';
  if (!ensure_directory(session.root_path)) {
    printf("Unable to create/open root directory.\n");
    exit(EXIT_FAILURE);
  }

  input_buffer = new_input_buffer();
  stmt = calloc(1, sizeof(SqlStatement));
  if (stmt == NULL) {
    printf("Unable to allocate statement buffer.\n");
    close_input_buffer(input_buffer);
    exit(EXIT_FAILURE);
  }

  while (true) {
    if (!isatty(STDIN_FILENO)) {
      print_prompt();
    }
    read_input(input_buffer);
    normalize_input_line(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      MetaCommandResult meta = do_meta_command(&session, input_buffer);
      if (meta == META_COMMAND_EXIT) {
        free(stmt);
        close_input_buffer(input_buffer);
        if (session.database != NULL) {
          db_close(session.database);
        }
        exit(EXIT_SUCCESS);
      }
      if (meta == META_COMMAND_UNRECOGNIZED_COMMAND) {
        printf("Unrecognized command '%s'\n", input_buffer->buffer);
      }
      continue;
    }

    memset(stmt, 0, sizeof(*stmt));
    if (!parse_sql_statement(input_buffer->buffer, stmt)) {
      printf("Syntax error. Could not parse SQL statement.\n");
      continue;
    }

    if (stmt->type != SQL_STMT_CREATE_DATABASE &&
        (!session.has_active_database || session.database == NULL)) {
      printf("No active database. Use .usedatabase <name>.\n");
      continue;
    }

    execute_sql(&session, stmt);
  }
}