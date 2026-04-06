#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <dlfcn.h>

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

typedef char* (*ReadlineGeneratorFn)(const char*, int);
typedef char** (*ReadlineCompletionMatchesFn)(const char*, ReadlineGeneratorFn);
typedef char** (*ReadlineCompletionFn)(const char*, int, int);

typedef struct {
  bool initialized;
  bool enabled;
  void* handle;
  char* (*readline_fn)(const char*);
  void (*add_history_fn)(const char*);
  void (*using_history_fn)(void);
  void (*stifle_history_fn)(int);
  ReadlineCompletionMatchesFn completion_matches_fn;
  ReadlineCompletionFn* attempted_completion_slot;
} ReadlineApi;

static ReadlineApi g_readline_api = {0};

static const char* g_meta_completion_words[] = {
    ".exit", ".usedatabase", ".showdatabases", ".showtables", ".btree",
    ".constants", NULL};

static const char* g_sql_completion_words[] = {
    "create", "database", "table", "insert", "into", "values", "select",
    "from", "delete", "where", "update", "set", "drop", "primary", "key",
    "int", "text", "null", NULL};

static char* sqlyt_completion_generator(const char* text, int state) {
  static const char** words;
  static size_t index;
  size_t text_len = strlen(text);

  if (state == 0) {
    words = (text_len > 0 && text[0] == '.') ? g_meta_completion_words
                                             : g_sql_completion_words;
    index = 0;
  }

  while (words[index] != NULL) {
    const char* candidate = words[index++];
    if (strncasecmp(candidate, text, text_len) == 0) {
      return strdup(candidate);
    }
  }
  return NULL;
}

static char** sqlyt_completion(const char* text, int start, int end) {
  (void)start;
  (void)end;
  if (g_readline_api.completion_matches_fn == NULL) {
    return NULL;
  }
  return g_readline_api.completion_matches_fn(text, sqlyt_completion_generator);
}

static bool init_readline_if_needed(void) {
  if (g_readline_api.initialized) {
    return g_readline_api.enabled;
  }

  g_readline_api.initialized = true;
  g_readline_api.handle = dlopen("libreadline.so.8", RTLD_LAZY);
  if (g_readline_api.handle == NULL) {
    g_readline_api.handle = dlopen("libreadline.so", RTLD_LAZY);
  }
  if (g_readline_api.handle == NULL) {
    return false;
  }

  g_readline_api.readline_fn = dlsym(g_readline_api.handle, "readline");
  g_readline_api.add_history_fn = dlsym(g_readline_api.handle, "add_history");
  g_readline_api.using_history_fn = dlsym(g_readline_api.handle, "using_history");
  g_readline_api.stifle_history_fn = dlsym(g_readline_api.handle, "stifle_history");
  g_readline_api.completion_matches_fn =
      dlsym(g_readline_api.handle, "rl_completion_matches");
  g_readline_api.attempted_completion_slot =
      dlsym(g_readline_api.handle, "rl_attempted_completion_function");

  if (g_readline_api.readline_fn == NULL || g_readline_api.add_history_fn == NULL ||
      g_readline_api.using_history_fn == NULL ||
      g_readline_api.stifle_history_fn == NULL ||
      g_readline_api.completion_matches_fn == NULL ||
      g_readline_api.attempted_completion_slot == NULL) {
    dlclose(g_readline_api.handle);
    memset(&g_readline_api, 0, sizeof(g_readline_api));
    g_readline_api.initialized = true;
    return false;
  }

  g_readline_api.using_history_fn();
  g_readline_api.stifle_history_fn(1000);
  *g_readline_api.attempted_completion_slot = sqlyt_completion;
  g_readline_api.enabled = true;
  return true;
}

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_DUPLICATE_KEY,
} ExecuteResult;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND,
  META_COMMAND_EXIT
} MetaCommandResult;

#define DEFAULT_ROOT_PATH "./data"
#define DATABASE_FILE_NAME "database.db"
#define MAX_TABLE_NAME_LENGTH 28
#define MAX_SCHEMA_COLUMNS 10
#define MAX_COLUMN_NAME_LENGTH 24
#define MAX_CELL_TEXT 64
#define COLUMN_TYPE_INT 1
#define COLUMN_TYPE_TEXT 2
#define DB_HEADER_MAGIC "SQLYTDB1"
#define DB_FORMAT_VERSION 2
#define DB_HEADER_MASTER_ROOT_PAGE 1
#define DB_HEADER_FIRST_USER_ROOT_PAGE 2

/* Catalog (sqlite_master) row: btree key = root_page; value layout is fixed. */
#define CATALOG_TABLE_NAME_SIZE 32
#define CATALOG_SCHEMA_STORAGE 512
#define CATALOG_VALUE_SIZE \
  (sizeof(uint32_t) + CATALOG_TABLE_NAME_SIZE + CATALOG_SCHEMA_STORAGE)
#define SCHEMA_TEXT_MAX CATALOG_SCHEMA_STORAGE
#define SQL_LINE_BUF 1024
#define MAX_INSERT_ROWS 32

typedef struct {
  uint32_t column_count;
  char column_names[MAX_SCHEMA_COLUMNS][MAX_COLUMN_NAME_LENGTH + 1];
  uint32_t column_type[MAX_SCHEMA_COLUMNS];
  uint32_t column_max[MAX_SCHEMA_COLUMNS];
  char create_sql[SCHEMA_TEXT_MAX + 1];
} SqlSchema;

typedef struct {
  uint32_t value_size;
  uint32_t col_offset[MAX_SCHEMA_COLUMNS];
  uint32_t col_size[MAX_SCHEMA_COLUMNS];
} RowLayout;

typedef enum {
  SQL_STMT_CREATE_DATABASE,
  SQL_STMT_CREATE_TABLE,
  SQL_STMT_INSERT,
  SQL_STMT_SELECT,
  SQL_STMT_DELETE,
  SQL_STMT_UPDATE,
  SQL_STMT_DROP_TABLE
} SqlStatementType;

typedef struct Table Table;

typedef struct {
  SqlStatementType type;
  char table_name[MAX_TABLE_NAME_LENGTH + 1];
  uint32_t id;
  char update_column[MAX_COLUMN_NAME_LENGTH + 1];
  char update_value[256];
  uint32_t row_count;
  uint32_t row_value_count[MAX_INSERT_ROWS];
  uint32_t row_ids[MAX_INSERT_ROWS];
  bool row_auto_id[MAX_INSERT_ROWS];
  char values[MAX_INSERT_ROWS][MAX_SCHEMA_COLUMNS][256];
  SqlSchema schema;
} SqlStatement;

typedef enum {
  SQL_EXEC_OK,
  SQL_EXEC_CREATE_DB_FAILED,
  SQL_EXEC_RESERVED_TABLE_NAME,
  SQL_EXEC_TABLE_EXISTS,
  SQL_EXEC_ROW_LAYOUT_TOO_LARGE,
  SQL_EXEC_TABLE_NAME_TOO_LONG,
  SQL_EXEC_SCHEMA_METADATA_TOO_LONG,
  SQL_EXEC_DUPLICATE_KEY,
  SQL_EXEC_TABLE_NOT_FOUND,
  SQL_EXEC_WRONG_VALUE_COUNT,
  SQL_EXEC_SCHEMA_INVALID_PK,
  SQL_EXEC_INSERT_VALIDATION_FAILED,
  SQL_EXEC_ROW_PACK_FAILED,
  SQL_EXEC_SELECT_LAYOUT_INVALID,
  SQL_EXEC_DELETE_RECORD_NOT_FOUND,
  SQL_EXEC_UPDATE_COLUMN_NOT_FOUND,
  SQL_EXEC_UPDATE_PK_NOT_ALLOWED,
  SQL_EXEC_UPDATE_RECORD_NOT_FOUND,
  SQL_EXEC_RESERVED_DROP_TABLE,
} SqlExecuteResult;

typedef struct {
  char root_path[512];
  bool has_active_database;
  char active_database[MAX_TABLE_NAME_LENGTH + 1];
  Table* database;
} Session;

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 400

#define INVALID_PAGE_NUM UINT32_MAX

#include <pthread.h>
#define WAL_CHECKPOINT_THRESHOLD 100

typedef struct {
  uint32_t page_num;
  uint32_t is_commit;
} WalFrameHeader;

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  uint32_t master_root_page;
  uint32_t next_root_page;
  void* pages[TABLE_MAX_PAGES];
  int wal_file_descriptor;
  uint32_t page_to_wal_frame[TABLE_MAX_PAGES];
  uint32_t wal_frame_count;
  uint8_t page_dirty[TABLE_MAX_PAGES];
  bool checkpoint_in_progress;
  pthread_mutex_t wal_mutex;
} Pager;

typedef struct {
  char magic[8];
  uint32_t version;
  uint32_t master_root_page;
  uint32_t next_root_page;
} DbFileHeader;


uint32_t get_unused_page_num(Pager* pager);

#include "btree.c"
#include "pager.c"
#include "parser.c"
#include "cli.c"
