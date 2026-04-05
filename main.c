#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

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
#define COLUMN_TYPE_VARCHAR 2
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
  SQL_STMT_SELECT
} SqlStatementType;

typedef struct Table Table;

typedef struct {
  SqlStatementType type;
  char table_name[MAX_TABLE_NAME_LENGTH + 1];
  uint32_t id;
  uint32_t value_count;
  char values[MAX_SCHEMA_COLUMNS][MAX_CELL_TEXT + 1];
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

void mark_page_dirty(Pager* pager, uint32_t page_num) {
  if (page_num < TABLE_MAX_PAGES) {
    pager->page_dirty[page_num] = 1;
  }
}

void pager_commit_transaction_sync(Pager* pager);

struct Table {
  Pager* pager;
  uint32_t root_page_num;
  bool is_catalog;
  uint32_t value_size;
  uint32_t cell_size;
  uint32_t max_cells;
  SqlSchema schema;
  RowLayout row_layout;
};

typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

/*
 * Common Node Header Layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
/* Keep this small for testing */
const uint32_t INTERNAL_NODE_MAX_KEYS = 3;

/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET =
    LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                       LEAF_NODE_NUM_CELLS_SIZE +
                                       LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)

NodeType get_node_type(void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root(void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;
}

void set_node_root(void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(void* node) { return node + PARENT_POINTER_OFFSET; }

uint32_t* internal_node_num_keys(void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    uint32_t* right_child = internal_node_right_child(node);
    if (*right_child == INVALID_PAGE_NUM) {
      printf("Tried to access right child of node, but was invalid page\n");
      exit(EXIT_FAILURE);
    }
    return right_child;
  } else {
    uint32_t* child = internal_node_cell(node, child_num);
    if (*child == INVALID_PAGE_NUM) {
      printf("Tried to access child %d of node, but was invalid page\n", child_num);
      exit(EXIT_FAILURE);
    }
    return child;
  }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

void table_init_catalog(Table* t, Pager* pager) {
  t->pager = pager;
  t->root_page_num = pager->master_root_page;
  t->is_catalog = true;
  memset(&t->schema, 0, sizeof(t->schema));
  memset(&t->row_layout, 0, sizeof(t->row_layout));
  t->value_size = CATALOG_VALUE_SIZE;
  t->cell_size = LEAF_NODE_KEY_SIZE + t->value_size;
  t->max_cells = LEAF_NODE_SPACE_FOR_CELLS / t->cell_size;
}

bool compute_row_layout(const SqlSchema* schema, RowLayout* L) {
  L->value_size = 0;
  for (uint32_t i = 0; i < schema->column_count; i++) {
    L->col_offset[i] = L->value_size;
    if (schema->column_type[i] == COLUMN_TYPE_INT) {
      L->col_size[i] = sizeof(uint32_t);
    } else {
      L->col_size[i] = schema->column_max[i];
    }
    L->value_size += L->col_size[i];
  }
  uint32_t cell = LEAF_NODE_KEY_SIZE + L->value_size;
  if (L->value_size == 0 || cell > LEAF_NODE_SPACE_FOR_CELLS) {
    return false;
  }
  return true;
}

bool table_init_user(Table* t, Pager* pager, uint32_t root_page,
                     const SqlSchema* schema) {
  t->pager = pager;
  t->root_page_num = root_page;
  t->is_catalog = false;
  memcpy(&t->schema, schema, sizeof(SqlSchema));
  if (!compute_row_layout(schema, &t->row_layout)) {
    return false;
  }
  t->value_size = t->row_layout.value_size;
  t->cell_size = LEAF_NODE_KEY_SIZE + t->value_size;
  t->max_cells = LEAF_NODE_SPACE_FOR_CELLS / t->cell_size;
  return t->max_cells > 0;
}

static inline void* tbl_leaf_cell(Table* t, void* node, uint32_t cell_num) {
  return (uint8_t*)node + LEAF_NODE_HEADER_SIZE + cell_num * t->cell_size;
}

static inline uint32_t* tbl_leaf_key(Table* t, void* node, uint32_t cell_num) {
  return (uint32_t*)tbl_leaf_cell(t, node, cell_num);
}

static inline void* tbl_leaf_value(Table* t, void* node, uint32_t cell_num) {
  return (uint8_t*)tbl_leaf_cell(t, node, cell_num) + LEAF_NODE_KEY_SIZE;
}

static inline uint32_t* leaf_key_sized(void* node, uint32_t cell_num,
                                       uint32_t cell_size) {
  return (uint32_t*)((uint8_t*)node + LEAF_NODE_HEADER_SIZE +
                     cell_num * cell_size);
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d >= %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss. Allocate memory and load from file.
    void* page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // We might save a partial page at the end of the file
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    // Hold wal_mutex while checking the WAL mapping and reading from WAL to
    // prevent a concurrent checkpoint from truncating the WAL mid-read.
    pthread_mutex_lock(&pager->wal_mutex);
    if (pager->page_to_wal_frame[page_num] > 0) {
      uint32_t frame_index = pager->page_to_wal_frame[page_num] - 1;
      off_t offset = frame_index * (sizeof(WalFrameHeader) + PAGE_SIZE) + sizeof(WalFrameHeader);
      lseek(pager->wal_file_descriptor, offset, SEEK_SET);
      ssize_t bytes_read = read(pager->wal_file_descriptor, page, PAGE_SIZE);
      pthread_mutex_unlock(&pager->wal_mutex);
      if (bytes_read == -1) {
        printf("Error reading WAL file\n");
        free(page);
        exit(EXIT_FAILURE);
      }
    } else {
      pthread_mutex_unlock(&pager->wal_mutex);
      if (page_num < num_pages) {
        lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
        ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
        if (bytes_read == -1) {
          printf("Error reading file: %d\n", errno);
          free(page);
          exit(EXIT_FAILURE);
        } else if ((uint32_t)bytes_read < PAGE_SIZE) {
          memset((char*)page + bytes_read, 0, PAGE_SIZE - (uint32_t)bytes_read);
        }
      } else {
        memset(page, 0, PAGE_SIZE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

uint32_t get_node_max_key(Table* table, void* node) {
  if (get_node_type(node) == NODE_LEAF) {
    uint32_t n = *leaf_node_num_cells(node);
    return *tbl_leaf_key(table, node, n - 1);
  }
  void* right_child = get_page(table->pager, *internal_node_right_child(node));
  return get_node_max_key(table, right_child);
}

void print_constants() {
  Table sample;
  memset(&sample, 0, sizeof(sample));
  sample.value_size = CATALOG_VALUE_SIZE;
  sample.cell_size = LEAF_NODE_KEY_SIZE + CATALOG_VALUE_SIZE;
  sample.max_cells = LEAF_NODE_SPACE_FOR_CELLS / sample.cell_size;
  printf("CATALOG_VALUE_SIZE: %zu\n", (size_t)CATALOG_VALUE_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("catalog cell_size: %u\n", sample.cell_size);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("catalog max_cells: %u\n", sample.max_cells);
}

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level,
                uint32_t leaf_cell_size) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_key_sized(node, i, leaf_cell_size));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
        for (uint32_t i = 0; i < num_keys; i++) {
          child = *internal_node_child(node, i);
          print_tree(pager, child, indentation_level + 1, leaf_cell_size);

          indent(indentation_level + 1);
          printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1, leaf_cell_size);
      }
      break;
  }
}

void catalog_write_value(uint32_t root_page_key, const char* table_name,
                         const char* schema_text, void* dest_value) {
  uint8_t* p = dest_value;
  memcpy(p, &root_page_key, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memset(p, 0, CATALOG_TABLE_NAME_SIZE);
  strncpy((char*)p, table_name, CATALOG_TABLE_NAME_SIZE - 1);
  p += CATALOG_TABLE_NAME_SIZE;
  memset(p, 0, CATALOG_SCHEMA_STORAGE);
  strncpy((char*)p, schema_text, CATALOG_SCHEMA_STORAGE - 1);
}

void catalog_read_value(const void* value, uint32_t* root_page_key,
                        char* table_name_out, char* schema_out,
                        size_t schema_cap) {
  const uint8_t* p = value;
  memcpy(root_page_key, p, sizeof(uint32_t));
  p += sizeof(uint32_t);
  memset(table_name_out, 0, MAX_TABLE_NAME_LENGTH + 1);
  strncpy(table_name_out, (const char*)p, MAX_TABLE_NAME_LENGTH);
  table_name_out[MAX_TABLE_NAME_LENGTH] = '\0';
  p += CATALOG_TABLE_NAME_SIZE;
  if (schema_out != NULL && schema_cap > 0) {
    memset(schema_out, 0, schema_cap);
    strncpy(schema_out, (const char*)p, schema_cap - 1);
  }
}

void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
  /*
  Necessary because the root page number is 0; by not initializing an internal 
  node's right child to an invalid page number when initializing the node, we may
  end up with 0 as the node's right child, which makes the node a parent of the root
  */
  *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;
  cursor->end_of_table = false;

  // Binary search
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    uint32_t key_at_index = *tbl_leaf_key(table, node, index);
    if (key == key_at_index) {
      cursor->cell_num = index;
      return cursor;
    }
    if (key < key_at_index) {
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  cursor->cell_num = min_index;
  return cursor;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
  /*
  Return the index of the child which should contain
  the given key.
  */

  uint32_t num_keys = *internal_node_num_keys(node);

  /* Binary search */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys; /* there is one more child than key */

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    uint32_t key_to_right = *internal_node_key(node, index);
    if (key_to_right >= key) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);

  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch (get_node_type(child)) {
    case NODE_LEAF:
      return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
      return internal_node_find(table, child_num, key);
    default:
      printf("Corrupt node type.\n");
      exit(EXIT_FAILURE);
  }
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);

  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

Cursor* table_start(Table* table) {
  Cursor* cursor = table_find(table, 0);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);

void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return tbl_leaf_value(cursor->table, page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);

  cursor->cell_num += 1;
  if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
    /* Advance to next leaf node */
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      /* This was rightmost leaf */
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

Pager* pager_open(const char* filename) {
  int fd = open(filename,
                O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // User write permission
                    S_IRUSR   // User read permission
                );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  char wal_filename[512];
  snprintf(wal_filename, sizeof(wal_filename), "%s-wal", filename);
  int wal_fd = open(wal_filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (wal_fd == -1) {
    printf("Unable to open WAL file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->wal_file_descriptor = wal_fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);
  pager->wal_frame_count = 0;
  pager->checkpoint_in_progress = false;
  pthread_mutex_init(&pager->wal_mutex, NULL);
  
  if (file_length > 0 && file_length % PAGE_SIZE != 0) {
    if (file_length == sizeof(DbFileHeader)) {
      file_length = PAGE_SIZE;
      ftruncate(fd, PAGE_SIZE);
      pager->file_length = file_length;
      pager->num_pages = 1;
    } else {
      printf("Db file is not a whole number of pages. Corrupt file.\n");
      exit(EXIT_FAILURE);
    }
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
    pager->page_to_wal_frame[i] = 0;
    pager->page_dirty[i] = 0;
  }

  off_t wal_length = lseek(wal_fd, 0, SEEK_END);
  uint32_t frame_size = sizeof(WalFrameHeader) + PAGE_SIZE;
  uint32_t complete_frames = wal_length / frame_size;
  
  lseek(wal_fd, 0, SEEK_SET);
  uint32_t last_commit_frame = 0;
  for (uint32_t i = 0; i < complete_frames; i++) {
    WalFrameHeader header;
    read(wal_fd, &header, sizeof(WalFrameHeader));
    if (header.page_num < TABLE_MAX_PAGES) {
      pager->page_to_wal_frame[header.page_num] = i + 1;
    }
    lseek(wal_fd, PAGE_SIZE, SEEK_CUR); // Skip page data
    if (header.is_commit) {
      last_commit_frame = i + 1;
    }
  }
  
  if (complete_frames > last_commit_frame) {
    ftruncate(wal_fd, last_commit_frame * frame_size);
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) pager->page_to_wal_frame[i] = 0;
    lseek(wal_fd, 0, SEEK_SET);
    for (uint32_t i = 0; i < last_commit_frame; i++) {
      WalFrameHeader header;
      read(wal_fd, &header, sizeof(WalFrameHeader));
      if (header.page_num < TABLE_MAX_PAGES) {
        pager->page_to_wal_frame[header.page_num] = i + 1;
      }
      lseek(wal_fd, PAGE_SIZE, SEEK_CUR);
    }
  }
  pager->wal_frame_count = last_commit_frame;

  return pager;
}

bool header_is_valid(const DbFileHeader* header) {
  if (memcmp(header->magic, DB_HEADER_MAGIC, sizeof(header->magic)) != 0) {
    return false;
  }
  if (header->version != DB_FORMAT_VERSION) {
    return false;
  }
  if (header->master_root_page == 0 ||
      header->master_root_page >= TABLE_MAX_PAGES ||
      header->next_root_page <= header->master_root_page ||
      header->next_root_page >= TABLE_MAX_PAGES) {
    return false;
  }
  return true;
}

uint32_t recover_next_root_page_from_master(Pager* pager, uint32_t master_root_page) {
  Table master_table;
  Cursor* cursor;
  uint32_t max_root = master_root_page;

  table_init_catalog(&master_table, pager);
  master_table.root_page_num = master_root_page;

  cursor = table_start(&master_table);
  while (!cursor->end_of_table) {
    void* node = get_page(pager, cursor->page_num);
    uint32_t k = *tbl_leaf_key(&master_table, node, cursor->cell_num);
    if (k > max_root) {
      max_root = k;
    }
    cursor_advance(cursor);
  }
  free(cursor);

  if (max_root + 1 >= TABLE_MAX_PAGES) {
    printf("Corrupted catalog: no free root pages left.\n");
    exit(EXIT_FAILURE);
  }
  return max_root + 1;
}

// persist_header_now is removed because it circumvented WAL.

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  bool is_new_file = (pager->num_pages == 0);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;

  void* header_page = get_page(pager, 0);
  DbFileHeader* header = (DbFileHeader*)header_page;

  if (is_new_file) {
    memset(header, 0, PAGE_SIZE);
    memcpy(header->magic, DB_HEADER_MAGIC, sizeof(header->magic));
    header->version = DB_FORMAT_VERSION;
    header->master_root_page = DB_HEADER_MASTER_ROOT_PAGE;
    header->next_root_page = DB_HEADER_FIRST_USER_ROOT_PAGE;

    void* root_node = get_page(pager, header->master_root_page);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
    mark_page_dirty(pager, 0);
    mark_page_dirty(pager, header->master_root_page);
    pager_commit_transaction_sync(pager);
  } else {
    if (!header_is_valid(header)) {
      uint32_t repaired_master_root = DB_HEADER_MASTER_ROOT_PAGE;
      uint32_t repaired_next_root;
      void* repaired_root = get_page(pager, repaired_master_root);

      if (get_node_type(repaired_root) != NODE_LEAF &&
          get_node_type(repaired_root) != NODE_INTERNAL) {
        initialize_leaf_node(repaired_root);
        set_node_root(repaired_root, true);
      }

      repaired_next_root = recover_next_root_page_from_master(pager, repaired_master_root);

      memset(header, 0, PAGE_SIZE);
      memcpy(header->magic, DB_HEADER_MAGIC, sizeof(header->magic));
      header->version = DB_FORMAT_VERSION;
      header->master_root_page = repaired_master_root;
      header->next_root_page = repaired_next_root;
      mark_page_dirty(pager, 0);
      pager_commit_transaction_sync(pager);

      printf("Recovered database header metadata.\n");
    }
  }

  pager->master_root_page = header->master_root_page;
  pager->next_root_page = header->next_root_page;
  table_init_catalog(table, pager);

  return table;
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // Ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t is_commit) {
  pthread_mutex_lock(&pager->wal_mutex);
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  uint32_t frame_index = pager->wal_frame_count;
  off_t offset = frame_index * (sizeof(WalFrameHeader) + PAGE_SIZE);
  lseek(pager->wal_file_descriptor, offset, SEEK_SET);

  WalFrameHeader header;
  header.page_num = page_num;
  header.is_commit = is_commit;
  
  if (write(pager->wal_file_descriptor, &header, sizeof(WalFrameHeader)) != sizeof(WalFrameHeader)) {
      printf("Error writing WAL header: %d\n", errno);
      exit(EXIT_FAILURE);
  }
  if (write(pager->wal_file_descriptor, pager->pages[page_num], PAGE_SIZE) != PAGE_SIZE) {
      printf("Error writing WAL page: %d\n", errno);
      exit(EXIT_FAILURE);
  }

  pager->page_to_wal_frame[page_num] = frame_index + 1;
  pager->wal_frame_count += 1;
  pthread_mutex_unlock(&pager->wal_mutex);
}

void* background_checkpoint_task(void* arg) {
  Pager* pager = (Pager*)arg;
  bool checkpoint_failed = false;
  pthread_mutex_lock(&pager->wal_mutex);

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->page_to_wal_frame[i] > 0) {
      uint32_t frame_index = pager->page_to_wal_frame[i] - 1;
      off_t wal_offset = frame_index * (sizeof(WalFrameHeader) + PAGE_SIZE) + sizeof(WalFrameHeader);

      void* page = malloc(PAGE_SIZE);
      if (page == NULL) {
        printf("Error allocating checkpoint page buffer\n");
        checkpoint_failed = true;
        break;
      }

      if (lseek(pager->wal_file_descriptor, wal_offset, SEEK_SET) == (off_t)-1) {
        printf("Error seeking WAL during checkpoint: %d\n", errno);
        free(page);
        checkpoint_failed = true;
        break;
      }

      ssize_t bytes_read = read(pager->wal_file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1 || (uint32_t)bytes_read != PAGE_SIZE) {
        printf("Error reading WAL page during checkpoint: %d\n", errno);
        free(page);
        checkpoint_failed = true;
        break;
      }

      if (lseek(pager->file_descriptor, i * PAGE_SIZE, SEEK_SET) == (off_t)-1) {
        printf("Error seeking database file during checkpoint: %d\n", errno);
        free(page);
        checkpoint_failed = true;
        break;
      }

      ssize_t bytes_written = write(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_written == -1 || (uint32_t)bytes_written != PAGE_SIZE) {
        printf("Error writing database page during checkpoint: %d\n", errno);
        free(page);
        checkpoint_failed = true;
        break;
      }

      free(page);
    }
  }

  if (!checkpoint_failed) {
    if (ftruncate(pager->wal_file_descriptor, 0) == -1) {
      printf("Error truncating WAL after checkpoint: %d\n", errno);
      checkpoint_failed = true;
    } else {
      pager->wal_frame_count = 0;
      for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->page_to_wal_frame[i] = 0;
      }
    }
  }

  pager->checkpoint_in_progress = false;
  pthread_mutex_unlock(&pager->wal_mutex);

  if (checkpoint_failed) {
    exit(EXIT_FAILURE);
  }
  return NULL;
}

void pager_checkpoint(Pager* pager) {
  background_checkpoint_task(pager);
}

void pager_commit_transaction_sync(Pager* pager) {
  uint32_t dirty_count = 0;
  uint32_t last_dirty_page = 0;
  
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->page_dirty[i]) {
      dirty_count++;
      last_dirty_page = i;
    }
  }
  
  if (dirty_count == 0) return;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    if (pager->page_dirty[i]) {
      pager_flush(pager, i, (i == last_dirty_page) ? 1 : 0);
      pager->page_dirty[i] = 0;
    }
  }

  pthread_mutex_lock(&pager->wal_mutex);
  bool should_checkpoint = (pager->wal_frame_count >= WAL_CHECKPOINT_THRESHOLD &&
                             !pager->checkpoint_in_progress);
  if (should_checkpoint) {
    pager->checkpoint_in_progress = true;
  }
  pthread_mutex_unlock(&pager->wal_mutex);

  if (should_checkpoint) {
    pthread_t checkpoint_thread;
    pthread_create(&checkpoint_thread, NULL, background_checkpoint_task, pager);
    pthread_detach(checkpoint_thread);
  }
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  // Mark all in-memory pages dirty so they are committed as a single WAL
  // transaction (only the last frame has is_commit=1), preventing partial
  // commit visibility on crash during close.
  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] != NULL) {
      pager->page_dirty[i] = 1;
    }
  }
  pager_commit_transaction_sync(pager);

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) continue;
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  pager_checkpoint(pager); // Sync WAL to DB before exiting

  int result = close(pager->file_descriptor);
  close(pager->wal_file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) {
  mark_page_dirty(pager, pager->num_pages);
  return pager->num_pages;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
  /*
  Handle splitting the root.
  Old root copied to new page, becomes left child.
  Address of right child passed in.
  Re-initialize root page to contain the new root node.
  New root node points to two children.
  */

  void* root = get_page(table->pager, table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);

  if (get_node_type(root) == NODE_INTERNAL) {
    initialize_internal_node(right_child);
    initialize_internal_node(left_child);
  }

  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  if (get_node_type(left_child) == NODE_INTERNAL) {
    void* child;
    for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
      child = get_page(table->pager, *internal_node_child(left_child,i));
      *node_parent(child) = left_child_page_num;
    }
    child = get_page(table->pager, *internal_node_right_child(left_child));
    *node_parent(child) = left_child_page_num;
  }

  /* Root node is a new internal node with one key and two children */
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = get_node_max_key(table, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  mark_page_dirty(table->pager, parent_page_num);
  /*
  Add a new child/key pair to parent that corresponds to child
  */

  void* parent = get_page(table->pager, parent_page_num);
  void* child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(table, child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);

  if (original_num_keys >= INTERNAL_NODE_MAX_KEYS) {
    internal_node_split_and_insert(table, parent_page_num, child_page_num);
    return;
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);
  /*
  An internal node with a right child of INVALID_PAGE_NUM is empty
  */
  if (right_child_page_num == INVALID_PAGE_NUM) {
    *internal_node_right_child(parent) = child_page_num;
    return;
  }

  void* right_child = get_page(table->pager, right_child_page_num);
  /*
  If we are already at the max number of cells for a node, we cannot increment
  before splitting. Incrementing without inserting a new key/child pair
  and immediately calling internal_node_split_and_insert has the effect
  of creating a new key at (max_cells + 1) with an uninitialized value
  */
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (child_max_key > get_node_max_key(table, right_child)) {
    /* Replace right child */
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table, right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    /* Make room for the new cell */
    for (uint32_t i = original_num_keys; i > index; i--) {
      void* destination = internal_node_cell(parent, i);
      void* source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  *internal_node_key(node, old_child_index) = new_key;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  mark_page_dirty(table->pager, parent_page_num);
  uint32_t old_page_num = parent_page_num;
  void* old_node = get_page(table->pager,parent_page_num);
  uint32_t old_max = get_node_max_key(table, old_node);

  void* child = get_page(table->pager, child_page_num); 
  uint32_t child_max = get_node_max_key(table, child);

  uint32_t new_page_num = get_unused_page_num(table->pager);

  /*
  Declaring a flag before updating pointers which
  records whether this operation involves splitting the root -
  if it does, we will insert our newly created node during
  the step where the table's new root is created. If it does
  not, we have to insert the newly created node into its parent
  after the old node's keys have been transferred over. We are not
  able to do this if the newly created node's parent is not a newly
  initialized root node, because in that case its parent may have existing
  keys aside from our old node which we are splitting. If that is true, we
  need to find a place for our newly created node in its parent, and we
  cannot insert it at the correct index if it does not yet have any keys
  */
  uint32_t splitting_root = is_node_root(old_node);

  void* parent;
  void* new_node;
  if (splitting_root) {
    create_new_root(table, new_page_num);
    parent = get_page(table->pager,table->root_page_num);
    /*
    If we are splitting the root, we need to update old_node to point
    to the new root's left child, new_page_num will already point to
    the new root's right child
    */
    old_page_num = *internal_node_child(parent,0);
    old_node = get_page(table->pager, old_page_num);
  } else {
    parent = get_page(table->pager,*node_parent(old_node));
    new_node = get_page(table->pager, new_page_num);
    initialize_internal_node(new_node);
  }
  
  uint32_t* old_num_keys = internal_node_num_keys(old_node);

  uint32_t cur_page_num = *internal_node_right_child(old_node);
  void* cur = get_page(table->pager, cur_page_num);

  /*
  First put right child into new node and set right child of old node to invalid page number
  */
  internal_node_insert(table, new_page_num, cur_page_num);
  *node_parent(cur) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;
  /*
  For each key until you get to the middle key, move the key and the child to the new node
  */
  for (int i = INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS / 2; i--) {
    cur_page_num = *internal_node_child(old_node, i);
    cur = get_page(table->pager, cur_page_num);

    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;

    (*old_num_keys)--;
  }

  /*
  Set child before middle key, which is now the highest key, to be node's right child,
  and decrement number of keys
  */
  *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
  (*old_num_keys)--;

  /*
  Determine which of the two nodes after the split should contain the child to be inserted,
  and insert the child
  */
  uint32_t max_after_split = get_node_max_key(table, old_node);

  uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(parent, old_max, get_node_max_key(table, old_node));

  if (!splitting_root) {
    internal_node_insert(table,*node_parent(old_node),new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key,
                                const void* value) {
  mark_page_dirty(cursor->table->pager, cursor->page_num);
  Table* t = cursor->table;
  uint32_t max_c = t->max_cells;
  uint32_t right_split = (max_c + 1) / 2;
  uint32_t left_split = (max_c + 1) - right_split;

  void* old_node = get_page(t->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(t, old_node);
  uint32_t new_page_num = get_unused_page_num(t->pager);
  void* new_node = get_page(t->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  for (int32_t i = (int32_t)max_c; i >= 0; i--) {
    void* destination_node;
    if ((uint32_t)i >= left_split) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = (uint32_t)i % left_split;
    void* destination = tbl_leaf_cell(t, destination_node, index_within_node);

    if ((uint32_t)i == cursor->cell_num) {
      memcpy(tbl_leaf_value(t, destination_node, index_within_node), value,
             t->value_size);
      *tbl_leaf_key(t, destination_node, index_within_node) = key;
    } else if ((uint32_t)i > cursor->cell_num) {
      memcpy(destination, tbl_leaf_cell(t, old_node, (uint32_t)i - 1),
             t->cell_size);
    } else {
      memcpy(destination, tbl_leaf_cell(t, old_node, (uint32_t)i),
             t->cell_size);
    }
  }

  *(leaf_node_num_cells(old_node)) = left_split;
  *(leaf_node_num_cells(new_node)) = right_split;

  if (is_node_root(old_node)) {
    create_new_root(t, new_page_num);
    return;
  }
  uint32_t parent_page_num = *node_parent(old_node);
  uint32_t new_max = get_node_max_key(t, old_node);
  void* parent = get_page(t->pager, parent_page_num);

  update_internal_node_key(parent, old_max, new_max);
  internal_node_insert(t, parent_page_num, new_page_num);
}

void leaf_node_insert(Cursor* cursor, uint32_t key, const void* value) {
  mark_page_dirty(cursor->table->pager, cursor->page_num);
  Table* t = cursor->table;
  void* node = get_page(t->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= t->max_cells) {
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor->cell_num < num_cells) {
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(tbl_leaf_cell(t, node, i), tbl_leaf_cell(t, node, i - 1),
             t->cell_size);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *tbl_leaf_key(t, node, cursor->cell_num) = key;
  memcpy(tbl_leaf_value(t, node, cursor->cell_num), value, t->value_size);
}

ExecuteResult insert_row_into_table(Table* table, uint32_t key,
                                    const void* value) {
  Cursor* cursor = table_find(table, key);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *tbl_leaf_key(table, node, cursor->cell_num);
    if (key_at_index == key) {
      free(cursor);
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, key, value);
  free(cursor);
  return EXECUTE_SUCCESS;
}

bool ensure_directory(const char* path) {
  struct stat st;
  if (stat(path, &st) == 0) {
    return S_ISDIR(st.st_mode);
  }
  return mkdir(path, 0700) == 0;
}

bool build_path(char* out, size_t out_size, const char* left, const char* right) {
  int written = snprintf(out, out_size, "%s/%s", left, right);
  return written >= 0 && (size_t)written < out_size;
}

bool is_valid_identifier(const char* name, size_t max_len) {
  if (name == NULL || name[0] == '\0' || strlen(name) > max_len) {
    return false;
  }
  for (size_t i = 0; name[i] != '\0'; i++) {
    char c = name[i];
    bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_';
    if (!ok) {
      return false;
    }
  }
  return true;
}

bool parse_u32(const char* text, uint32_t* value) {
  char* endptr = NULL;
  unsigned long parsed = strtoul(text, &endptr, 10);
  if (text == endptr || *endptr != '\0' || parsed > UINT32_MAX) {
    return false;
  }
  *value = (uint32_t)parsed;
  return true;
}

bool split_schema_payload(const char* payload, SqlSchema* schema) {
  char temp[SCHEMA_TEXT_MAX + 1];
  char* parts[1 + (MAX_SCHEMA_COLUMNS * 3)];
  int count = 0;
  char* saveptr = NULL;
  char* token;

  strncpy(temp, payload, sizeof(temp) - 1);
  temp[sizeof(temp) - 1] = '\0';

  token = strtok_r(temp, "|", &saveptr);
  while (token != NULL && count < (int)(1 + (MAX_SCHEMA_COLUMNS * 3))) {
    parts[count++] = token;
    token = strtok_r(NULL, "|", &saveptr);
  }

  if (count < 1) {
    return false;
  }
  if (!parse_u32(parts[0], &schema->column_count)) {
    return false;
  }
  if (schema->column_count > MAX_SCHEMA_COLUMNS) {
    return false;
  }

  if (count != 1 + (int)(schema->column_count * 3)) {
    return false;
  }
  for (uint32_t i = 0; i < schema->column_count; i++) {
    uint32_t parsed_type;
    uint32_t parsed_max;
    strncpy(schema->column_names[i], parts[1 + (i * 3)],
            sizeof(schema->column_names[i]) - 1);
    schema->column_names[i][sizeof(schema->column_names[i]) - 1] = '\0';
    if (!parse_u32(parts[2 + (i * 3)], &parsed_type) ||
        !parse_u32(parts[3 + (i * 3)], &parsed_max)) {
      return false;
    }
    if (parsed_type != COLUMN_TYPE_INT && parsed_type != COLUMN_TYPE_VARCHAR) {
      return false;
    }
    if (parsed_type == COLUMN_TYPE_INT) {
      parsed_max = 0;
    }
    if (parsed_type == COLUMN_TYPE_VARCHAR &&
        (parsed_max == 0 || parsed_max > MAX_CELL_TEXT)) {
      return false;
    }
    schema->column_type[i] = parsed_type;
    schema->column_max[i] = parsed_max;
  }

  strncpy(schema->create_sql, payload, sizeof(schema->create_sql) - 1);
  schema->create_sql[sizeof(schema->create_sql) - 1] = '\0';
  return true;
}

bool make_schema_payload(const SqlSchema* schema, char* out, size_t out_size) {
  int written = snprintf(out, out_size, "%u", schema->column_count);
  if (written < 0 || (size_t)written >= out_size) {
    return false;
  }

  for (uint32_t i = 0; i < schema->column_count; i++) {
    written += snprintf(out + written, out_size - (size_t)written, "|%s|%u|%u",
                        schema->column_names[i],
                        schema->column_type[i],
                        schema->column_max[i]);
    if (written < 0 || (size_t)written >= out_size) {
      return false;
    }
  }

  return true;
}

bool pack_insert_row(const SqlStatement* stmt, const SqlSchema* schema,
                     const RowLayout* L, uint8_t* out) {
  memset(out, 0, L->value_size);
  for (uint32_t i = 0; i < schema->column_count; i++) {
    uint32_t off = L->col_offset[i];
    uint32_t sz = L->col_size[i];
    if (schema->column_type[i] == COLUMN_TYPE_INT) {
      uint32_t v;
      if (!parse_u32(stmt->values[i], &v)) {
        return false;
      }

      memcpy(out + off, &v, sizeof(uint32_t));
    } else {
      const char* s = stmt->values[i];
      size_t len = strlen(s);
      if (len > sz) {
        return false;
      }
      memcpy(out + off, s, len);
    }
  }
  return true;
}

bool parse_u32_tokens_for_insert(const SqlStatement* stmt,
                                 const SqlSchema* schema) {
  for (uint32_t i = 0; i < schema->column_count; i++) {
    if (schema->column_type[i] == COLUMN_TYPE_INT) {
      uint32_t v;
      if (!parse_u32(stmt->values[i], &v)) {
        printf("Type error: expected int for column %s.\n",
               schema->column_names[i]);
        return false;
      }
      if (i == 0 && v != stmt->id) {
        return false;
      }
    } else if (schema->column_type[i] == COLUMN_TYPE_VARCHAR) {
      if (strlen(stmt->values[i]) > schema->column_max[i]) {
        printf("String is too long.\n");
        return false;
      }
    }
  }
  return true;
}

void print_user_row(const void* value, const SqlSchema* s, const RowLayout* L) {
  const uint8_t* d = value;
  printf("(");
  for (uint32_t i = 0; i < s->column_count; i++) {
    if (i > 0) {
      printf(", ");
    }
    if (s->column_type[i] == COLUMN_TYPE_INT) {
      uint32_t v;
      memcpy(&v, d + L->col_offset[i], sizeof(uint32_t));
      printf("%u", v);
    } else {
      printf("%.*s", (int)L->col_size[i], (const char*)(d + L->col_offset[i]));
    }
  }
  printf(")\n");
}

bool master_find_table(Table* db, const char* table_name, SqlSchema* schema,
                       uint32_t* root_page) {
  Cursor* cursor;
  Table master_table;
  char name_buf[MAX_TABLE_NAME_LENGTH + 1];
  char schema_buf[SCHEMA_TEXT_MAX + 1];

  if (!is_valid_identifier(table_name, MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  table_init_catalog(&master_table, db->pager);
  master_table.root_page_num = db->pager->master_root_page;
  cursor = table_start(&master_table);
  while (!cursor->end_of_table) {
    void* node = get_page(db->pager, cursor->page_num);
    uint32_t key = *tbl_leaf_key(&master_table, node, cursor->cell_num);
    uint32_t dummy_root;
    catalog_read_value(tbl_leaf_value(&master_table, node, cursor->cell_num),
                       &dummy_root, name_buf, schema_buf, sizeof(schema_buf));
    if (strcmp(name_buf, table_name) == 0) {
      bool ok = split_schema_payload(schema_buf, schema);
      if (ok && root_page != NULL) {
        *root_page = key;
      }
      free(cursor);
      return ok;
    }
    cursor_advance(cursor);
  }

  free(cursor);
  return false;
}

uint32_t allocate_next_root_page(Pager* pager) {
  uint32_t root_page = pager->next_root_page;
  void* header_page = get_page(pager, 0);
  DbFileHeader* header = (DbFileHeader*)header_page;

  if (root_page >= TABLE_MAX_PAGES) {
    printf("Error: Table root page limit reached.\n");
    exit(EXIT_FAILURE);
  }

  pager->next_root_page += 1;
  header->next_root_page = pager->next_root_page;
  mark_page_dirty(pager, 0);
  return root_page;
}

bool create_table_root(Table* db, uint32_t root_page) {
  void* root_node = get_page(db->pager, root_page);
  initialize_leaf_node(root_node);
  set_node_root(root_node, true);
  mark_page_dirty(db->pager, root_page);
  return true;
}

int split_tokens_sql(char* input, char* tokens[], int max_tokens) {
  int count = 0;
  char* p = input;

  while (*p != '\0' && count < max_tokens) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' ||
           *p == '(' || *p == ')' || *p == ',' || *p == ';') {
      p++;
    }

    if (*p == '\0') {
      break;
    }

    if (*p == '"') {
      p++;
      tokens[count++] = p;
      while (*p != '\0' && *p != '"') {
        p++;
      }
      if (*p == '"') {
        *p = '\0';
        p++;
      }
    } else {
      tokens[count++] = p;
      while (*p != '\0' && *p != ' ' && *p != '\t' && *p != '\n' &&
             *p != '\r' && *p != '(' && *p != ')' && *p != ',' && *p != ';') {
        p++;
      }
      if (*p != '\0') {
        *p = '\0';
        p++;
      }
    }
  }

  return count;
}

bool parse_create_table(char* line, SqlStatement* out) {
  char raw[SQL_LINE_BUF];
  char copy[SQL_LINE_BUF];
  char* tokens[64];
  int count;
  int index;

  strncpy(raw, line, sizeof(raw) - 1);
  raw[sizeof(raw) - 1] = '\0';
  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';

  count = split_tokens_sql(copy, tokens, 64);
  if (count < 8 || strcmp(tokens[0], "create") != 0 || strcmp(tokens[1], "table") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  if (count < 8 || strcmp(tokens[3], "id") != 0 || strcmp(tokens[4], "int") != 0) {
    return false;
  }

  index = 5;
  if (index + 1 < count && strcmp(tokens[index], "primary") == 0 &&
      strcmp(tokens[index + 1], "key") == 0) {
    index += 2;
  }

  strncpy(out->table_name, tokens[2], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';

  out->schema.column_count = 0;
  strncpy(out->schema.column_names[0], "id", sizeof(out->schema.column_names[0]) - 1);
  out->schema.column_names[0][sizeof(out->schema.column_names[0]) - 1] = '\0';
  out->schema.column_type[0] = COLUMN_TYPE_INT;
  out->schema.column_max[0] = 0;
  out->schema.column_count = 1;

  while (index + 1 < count) {
    const char* col_name;
    int advance;
    uint32_t max_len;

    if (out->schema.column_count >= MAX_SCHEMA_COLUMNS) {
      return false;
    }
    if (!is_valid_identifier(tokens[index], MAX_COLUMN_NAME_LENGTH)) {
      return false;
    }
    col_name = tokens[index];

    if (strcmp(tokens[index + 1], "int") == 0) {
      max_len = 0;
      out->schema.column_type[out->schema.column_count] = COLUMN_TYPE_INT;
      advance = 2;
    } else if (strcmp(tokens[index + 1], "varchar") == 0) {
      if (index + 2 >= count) {
        return false;
      }
      if (!parse_u32(tokens[index + 2], &max_len) || max_len == 0 || max_len > MAX_CELL_TEXT) {
        return false;
      }
      out->schema.column_type[out->schema.column_count] = COLUMN_TYPE_VARCHAR;
      advance = 3;
    } else {
      return false;
    }

    strncpy(out->schema.column_names[out->schema.column_count], col_name,
            sizeof(out->schema.column_names[out->schema.column_count]) - 1);
    out->schema.column_names[out->schema.column_count]
        [sizeof(out->schema.column_names[out->schema.column_count]) - 1] = '\0';
    out->schema.column_max[out->schema.column_count] = max_len;
    out->schema.column_count += 1;
    index += advance;
  }

  if (out->schema.column_count == 0) {
    return false;
  }

  strncpy(out->schema.create_sql, raw, sizeof(out->schema.create_sql) - 1);
  out->schema.create_sql[sizeof(out->schema.create_sql) - 1] = '\0';
  out->type = SQL_STMT_CREATE_TABLE;
  return true;
}

bool parse_insert_into(char* line, SqlStatement* out) {
  char copy[SQL_LINE_BUF];
  char* tokens[64];
  int count;
  int values_idx = -1;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';
  count = split_tokens_sql(copy, tokens, 64);

  if (count < 6 || strcmp(tokens[0], "insert") != 0 || strcmp(tokens[1], "into") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }
  for (int i = 3; i < count; i++) {
    if (strcmp(tokens[i], "values") == 0) {
      values_idx = i;
      break;
    }
  }
  if (values_idx == -1 || values_idx + 1 >= count) {
    return false;
  }

  strncpy(out->table_name, tokens[2], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';

  if (!parse_u32(tokens[values_idx + 1], &out->id)) {
    return false;
  }

  strncpy(out->values[0], tokens[values_idx + 1], MAX_CELL_TEXT);
  out->values[0][MAX_CELL_TEXT] = '\0';
  out->value_count = 1;
  for (int i = values_idx + 2; i < count; i++) {
    if (out->value_count >= MAX_SCHEMA_COLUMNS) {
      return false;
    }
    strncpy(out->values[out->value_count], tokens[i], MAX_CELL_TEXT);
    out->values[out->value_count][MAX_CELL_TEXT] = '\0';
    out->value_count += 1;
  }

  out->type = SQL_STMT_INSERT;
  return true;
}

bool parse_create_database(char* line, SqlStatement* out) {
  char copy[128];
  char* tokens[4];
  int count;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';
  count = split_tokens_sql(copy, tokens, 4);
  if (count != 3 || strcmp(tokens[0], "create") != 0 ||
      strcmp(tokens[1], "database") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  out->type = SQL_STMT_CREATE_DATABASE;
  strncpy(out->table_name, tokens[2], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';
  return true;
}

bool parse_select_from(char* line, SqlStatement* out) {
  char copy[SQL_LINE_BUF];
  char* tokens[16];
  int count;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';
  count = split_tokens_sql(copy, tokens, 16);

  if (count != 4 || strcmp(tokens[0], "select") != 0 || strcmp(tokens[1], "*") != 0 ||
      strcmp(tokens[2], "from") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[3], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  strncpy(out->table_name, tokens[3], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';
  out->type = SQL_STMT_SELECT;
  return true;
}

bool parse_sql_statement(char* line, SqlStatement* out) {
  if (strncmp(line, "create database", 15) == 0) {
    return parse_create_database(line, out);
  }
  if (strncmp(line, "create table", 12) == 0) {
    return parse_create_table(line, out);
  }
  if (strncmp(line, "insert into", 11) == 0) {
    return parse_insert_into(line, out);
  }
  if (strncmp(line, "select * from", 13) == 0) {
    return parse_select_from(line, out);
  }
  return false;
}

bool switch_database(Session* session, const char* db_name) {
  char db_dir[1024];
  char db_file[1024];

  if (!is_valid_identifier(db_name, MAX_TABLE_NAME_LENGTH)) {
    return false;
  }
  if (!build_path(db_dir, sizeof(db_dir), session->root_path, db_name)) {
    return false;
  }
  struct stat st;
  if (stat(db_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
    return false;
  }
  if (!build_path(db_file, sizeof(db_file), db_dir, DATABASE_FILE_NAME)) {
    return false;
  }

  if (session->database != NULL) {
    db_close(session->database);
    session->database = NULL;
  }
  session->database = db_open(db_file);
  if (session->database == NULL) {
    return false;
  }

  strcpy(session->active_database, db_name);
  session->has_active_database = true;
  return true;
}

void list_databases(const char* root_path) {
  DIR* dir = opendir(root_path);
  struct dirent* entry;
  if (dir == NULL) {
    printf("Unable to open root path.\n");
    return;
  }

  while ((entry = readdir(dir)) != NULL) {
    char path[1024];
    struct stat st;
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (!build_path(path, sizeof(path), root_path, entry->d_name)) {
      continue;
    }
    if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
      printf("%s\n", entry->d_name);
    }
  }
  closedir(dir);
}

void show_tables(Table* db) {
  Cursor* cursor;
  Table master;
  char name_buf[MAX_TABLE_NAME_LENGTH + 1];
  char schema_buf[SCHEMA_TEXT_MAX + 1];

  table_init_catalog(&master, db->pager);
  master.root_page_num = db->pager->master_root_page;

  cursor = table_start(&master);
  while (!cursor->end_of_table) {
    void* node = get_page(db->pager, cursor->page_num);
    uint32_t rp = *tbl_leaf_key(&master, node, cursor->cell_num);
    uint32_t dummy;
    catalog_read_value(tbl_leaf_value(&master, node, cursor->cell_num), &dummy,
                       name_buf, schema_buf, sizeof(schema_buf));
    printf("%s (root_page=%u)\n", name_buf, rp);
    cursor_advance(cursor);
  }
  free(cursor);
}

bool create_database(Session* session, const char* db_name) {
  char db_dir[1024];
  if (!is_valid_identifier(db_name, MAX_TABLE_NAME_LENGTH)) {
    return false;
  }
  if (!build_path(db_dir, sizeof(db_dir), session->root_path, db_name)) {
    return false;
  }
  return ensure_directory(db_dir);
}

SqlExecuteResult execute_create_database(Session* session,
                                         const SqlStatement* stmt) {
  if (!create_database(session, stmt->table_name)) {
    return SQL_EXEC_CREATE_DB_FAILED;
  }
  return SQL_EXEC_OK;
}

SqlExecuteResult execute_create_table(Table* db, const SqlStatement* stmt) {
  SqlSchema existing;
  uint32_t root_page;
  char schema_payload[SCHEMA_TEXT_MAX + 1];
  uint8_t catalog_value[CATALOG_VALUE_SIZE];
  ExecuteResult btree_result;
  RowLayout layout_check;

  if (strcmp(stmt->table_name, "sqlite_master") == 0) {
    return SQL_EXEC_RESERVED_TABLE_NAME;
  }
  if (master_find_table(db, stmt->table_name, &existing, NULL)) {
    return SQL_EXEC_TABLE_EXISTS;
  }

  if (!compute_row_layout(&stmt->schema, &layout_check)) {
    return SQL_EXEC_ROW_LAYOUT_TOO_LARGE;
  }

  root_page = allocate_next_root_page(db->pager);
  create_table_root(db, root_page);

  if (strlen(stmt->table_name) >= CATALOG_TABLE_NAME_SIZE) {
    return SQL_EXEC_TABLE_NAME_TOO_LONG;
  }
  if (!make_schema_payload(&stmt->schema, schema_payload,
                           sizeof(schema_payload))) {
    return SQL_EXEC_SCHEMA_METADATA_TOO_LONG;
  }

  catalog_write_value(root_page, stmt->table_name, schema_payload,
                      catalog_value);

  btree_result = insert_row_into_table(db, root_page, catalog_value);
  if (btree_result == EXECUTE_DUPLICATE_KEY) {
    return SQL_EXEC_DUPLICATE_KEY;
  }
  return SQL_EXEC_OK;
}

SqlExecuteResult execute_insert(Table* db, const SqlStatement* stmt) {
  SqlSchema schema;
  uint32_t root_page;
  ExecuteResult btree_result;
  Table user_table;
  uint8_t row_buf[LEAF_NODE_SPACE_FOR_CELLS];

  if (!master_find_table(db, stmt->table_name, &schema, &root_page)) {
    return SQL_EXEC_TABLE_NOT_FOUND;
  }
  if (stmt->value_count != schema.column_count) {
    printf("Error: Expected %u values.\n", schema.column_count);
    return SQL_EXEC_WRONG_VALUE_COUNT;
  }

  if (schema.column_count == 0 || schema.column_type[0] != COLUMN_TYPE_INT) {
    return SQL_EXEC_SCHEMA_INVALID_PK;
  }
  if (!parse_u32_tokens_for_insert(stmt, &schema)) {
    return SQL_EXEC_INSERT_VALIDATION_FAILED;
  }

  if (!table_init_user(&user_table, db->pager, root_page, &schema)) {
    return SQL_EXEC_ROW_LAYOUT_TOO_LARGE;
  }
  if (!pack_insert_row(stmt, &schema, &user_table.row_layout, row_buf)) {
    return SQL_EXEC_ROW_PACK_FAILED;
  }

  btree_result = insert_row_into_table(&user_table, stmt->id, row_buf);
  if (btree_result == EXECUTE_DUPLICATE_KEY) {
    return SQL_EXEC_DUPLICATE_KEY;
  }
  return SQL_EXEC_OK;
}

SqlExecuteResult execute_select(Table* db, const SqlStatement* stmt) {
  SqlSchema schema;
  uint32_t root_page;
  Table target;
  Cursor* cursor;

  if (!master_find_table(db, stmt->table_name, &schema, &root_page)) {
    return SQL_EXEC_TABLE_NOT_FOUND;
  }

  if (!table_init_user(&target, db->pager, root_page, &schema)) {
    return SQL_EXEC_SELECT_LAYOUT_INVALID;
  }
  cursor = table_start(&target);
  while (!cursor->end_of_table) {
    print_user_row(cursor_value(cursor), &schema, &target.row_layout);
    cursor_advance(cursor);
  }
  free(cursor);
  return SQL_EXEC_OK;
}

SqlExecuteResult execute_statement(Session* session, const SqlStatement* stmt) {
  Table* db = session->database;

  switch (stmt->type) {
    case SQL_STMT_CREATE_DATABASE:
      return execute_create_database(session, stmt);
    case SQL_STMT_CREATE_TABLE:
      return execute_create_table(db, stmt);
    case SQL_STMT_INSERT:
      return execute_insert(db, stmt);
    case SQL_STMT_SELECT:
      return execute_select(db, stmt);
  }
  return SQL_EXEC_OK;
}

static void print_sql_execute_result(SqlExecuteResult r) {
  switch (r) {
    case SQL_EXEC_OK:
      printf("Executed.\n");
      break;
    case SQL_EXEC_CREATE_DB_FAILED:
      printf("Error: Could not create database.\n");
      break;
    case SQL_EXEC_RESERVED_TABLE_NAME:
      printf("Error: Reserved table name.\n");
      break;
    case SQL_EXEC_TABLE_EXISTS:
      printf("Error: Table already exists.\n");
      break;
    case SQL_EXEC_ROW_LAYOUT_TOO_LARGE:
      printf("Error: Row layout too large for one page.\n");
      break;
    case SQL_EXEC_TABLE_NAME_TOO_LONG:
      printf("Error: Table name too long for catalog.\n");
      break;
    case SQL_EXEC_SCHEMA_METADATA_TOO_LONG:
      printf("Error: Schema metadata too long.\n");
      break;
    case SQL_EXEC_DUPLICATE_KEY:
      printf("Error: Duplicate key.\n");
      break;
    case SQL_EXEC_TABLE_NOT_FOUND:
      printf("Error: Table not found.\n");
      break;
    case SQL_EXEC_WRONG_VALUE_COUNT:
      break;
    case SQL_EXEC_SCHEMA_INVALID_PK:
      printf("Error: First column must be int primary key.\n");
      break;
    case SQL_EXEC_INSERT_VALIDATION_FAILED:
      break;
    case SQL_EXEC_ROW_PACK_FAILED:
      printf("Error: Row pack failed.\n");
      break;
    case SQL_EXEC_SELECT_LAYOUT_INVALID:
      printf("Error: Invalid table layout.\n");
      break;
  }
}

void execute_sql(Session* session, const SqlStatement* stmt) {
  SqlExecuteResult r = execute_statement(session, stmt);
  print_sql_execute_result(r);
  
  if (r == SQL_EXEC_OK && (stmt->type == SQL_STMT_INSERT || stmt->type == SQL_STMT_CREATE_TABLE || stmt->type == SQL_STMT_CREATE_DATABASE)) {
    if (session->database && session->database->pager) {
      pager_commit_transaction_sync(session->database->pager);
    }
  }
}

MetaCommandResult do_meta_command(Session* session, InputBuffer* input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    return META_COMMAND_EXIT;
  }

  if (strncmp(input_buffer->buffer, ".usedatabase ", 13) == 0) {
    const char* db_name = input_buffer->buffer + 13;
    if (!switch_database(session, db_name)) {
      printf("Unable to use database.\n");
    } else {
      printf("Using database %s\n", db_name);
    }
    return META_COMMAND_SUCCESS;
  }

  if (strcmp(input_buffer->buffer, ".showdatabases") == 0) {
    list_databases(session->root_path);
    return META_COMMAND_SUCCESS;
  }

  if (strcmp(input_buffer->buffer, ".showtables") == 0) {
    if (!session->has_active_database || session->database == NULL) {
      printf("No active database. Use .usedatabase <name>.\n");
      return META_COMMAND_SUCCESS;
    }
    show_tables(session->database);
    return META_COMMAND_SUCCESS;
  }

  if (strncmp(input_buffer->buffer, ".btree ", 7) == 0) {
    const char* table_name = input_buffer->buffer + 7;
    SqlSchema schema;
    uint32_t root_page;
    if (!session->has_active_database || session->database == NULL) {
      printf("No active database. Use .usedatabase <name>.\n");
      return META_COMMAND_SUCCESS;
    }
    if (!master_find_table(session->database, table_name, &schema, &root_page)) {
      printf("Error: Table not found.\n");
      return META_COMMAND_SUCCESS;
    }
    Table layout_table;
    if (!table_init_user(&layout_table, session->database->pager, root_page,
                         &schema)) {
      printf("Error: Invalid table layout.\n");
      return META_COMMAND_SUCCESS;
    }
    printf("Tree:\n");
    print_tree(session->database->pager, root_page, 0, layout_table.cell_size);
    return META_COMMAND_SUCCESS;
  }

  if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  }

  return META_COMMAND_UNRECOGNIZED_COMMAND;
}

int main(int argc, char* argv[]) {
  Session session;
  InputBuffer* input_buffer;
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
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      MetaCommandResult meta = do_meta_command(&session, input_buffer);
      if (meta == META_COMMAND_EXIT) {
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

    SqlStatement stmt;
    if (!parse_sql_statement(input_buffer->buffer, &stmt)) {
      printf("Syntax error. Could not parse SQL statement.\n");
      continue;
    }

    if (stmt.type != SQL_STMT_CREATE_DATABASE &&
        (!session.has_active_database || session.database == NULL)) {
      printf("No active database. Use .usedatabase <name>.\n");
      continue;
    }

    execute_sql(&session, &stmt);
  }
}
