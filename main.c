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
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define DEFAULT_ROOT_PATH "./data"
#define DATABASE_FILE_NAME "database.db"
#define MAX_TABLE_NAME_LENGTH 28
#define MASTER_TAG_PREFIX "m"
#define MAX_SCHEMA_COLUMNS 10
#define MAX_COLUMN_NAME_LENGTH 24
#define MAX_CELL_TEXT 64
#define COLUMN_TYPE_INT 1
#define COLUMN_TYPE_VARCHAR 2
#define DB_HEADER_MAGIC "SQLYTDB1"
#define DB_FORMAT_VERSION 1
#define DB_HEADER_MASTER_ROOT_PAGE 1
#define DB_HEADER_FIRST_USER_ROOT_PAGE 2
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
} Statement;

typedef struct {
  uint32_t column_count;
  char column_names[MAX_SCHEMA_COLUMNS][MAX_COLUMN_NAME_LENGTH + 1];
  uint32_t column_type[MAX_SCHEMA_COLUMNS];
  uint32_t column_max[MAX_SCHEMA_COLUMNS];
  char create_sql[COLUMN_EMAIL_SIZE + 1];
} SqlSchema;

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

typedef struct {
  char root_path[512];
  bool has_active_database;
  char active_database[MAX_TABLE_NAME_LENGTH + 1];
  Table* database;
} Session;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 400

#define INVALID_PAGE_NUM UINT32_MAX

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  uint32_t master_root_page;
  uint32_t next_root_page;
  void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  char magic[8];
  uint32_t version;
  uint32_t master_root_page;
  uint32_t next_root_page;
} DbFileHeader;

struct Table {
  Pager* pager;
  uint32_t root_page_num;
};

typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

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
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

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

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
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

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;

    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

uint32_t get_node_max_key(Pager* pager, void* node) {
  if (get_node_type(node) == NODE_LEAF) {
    return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
  void* right_child = get_page(pager,*internal_node_right_child(node));
  return get_node_max_key(pager, right_child);
}

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  void* node = get_page(pager, page_num);
  uint32_t num_keys, child;

  switch (get_node_type(node)) {
    case (NODE_LEAF):
      num_keys = *leaf_node_num_cells(node);
      indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        indent(indentation_level + 1);
        printf("- %d\n", *leaf_node_key(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *internal_node_num_keys(node);
      indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
        for (uint32_t i = 0; i < num_keys; i++) {
          child = *internal_node_child(node, i);
          print_tree(pager, child, indentation_level + 1);

          indent(indentation_level + 1);
          printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
      }
      break;
  }
}

void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
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
    uint32_t key_at_index = *leaf_node_key(node, index);
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

void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
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

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

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
  Row row;
  uint32_t max_root = master_root_page;

  master_table.pager = pager;
  master_table.root_page_num = master_root_page;

  cursor = table_start(&master_table);
  while (!cursor->end_of_table) {
    deserialize_row(cursor_value(cursor), &row);
    if (strncmp(row.username, MASTER_TAG_PREFIX ":", 2) == 0 && row.id > max_root) {
      max_root = row.id;
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

void persist_header_now(Pager* pager, DbFileHeader* header) {
  off_t offset = lseek(pager->file_descriptor, 0, SEEK_SET);
  if (offset == -1) {
    printf("Error seeking db header: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t written = write(pager->file_descriptor, header, sizeof(DbFileHeader));
  if (written != (ssize_t)sizeof(DbFileHeader)) {
    printf("Error writing db header: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  bool is_new_file = (pager->num_pages == 0);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = DB_HEADER_MASTER_ROOT_PAGE;

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
    persist_header_now(pager, header);
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
      persist_header_now(pager, header);

      printf("Recovered database header metadata.\n");
    }
  }

  pager->master_root_page = header->master_root_page;
  pager->next_root_page = header->next_root_page;
  table->root_page_num = pager->master_root_page;

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

void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  int result = close(pager->file_descriptor);
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

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    db_close(table);
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
    printf("Tree:\n");
    print_tree(table->pager, 0, 0);
    return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
  statement->type = STATEMENT_INSERT;

  char* keyword = strtok(input_buffer->buffer, " ");
  char* id_string = strtok(NULL, " ");
  char* username = strtok(NULL, " ");
  char* email = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }

  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }

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
  uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
  *internal_node_key(root, 0) = left_child_max_key;
  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

void internal_node_insert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num) {
  /*
  Add a new child/key pair to parent that corresponds to child
  */

  void* parent = get_page(table->pager, parent_page_num);
  void* child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(table->pager, child);
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

  if (child_max_key > get_node_max_key(table->pager, right_child)) {
    /* Replace right child */
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) =
        get_node_max_key(table->pager, right_child);
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
  uint32_t old_page_num = parent_page_num;
  void* old_node = get_page(table->pager,parent_page_num);
  uint32_t old_max = get_node_max_key(table->pager, old_node);

  void* child = get_page(table->pager, child_page_num); 
  uint32_t child_max = get_node_max_key(table->pager, child);

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
  uint32_t max_after_split = get_node_max_key(table->pager, old_node);

  uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

  if (!splitting_root) {
    internal_node_insert(table,*node_parent(old_node),new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
  /*
  Create a new node and move half the cells over.
  Insert the new value in one of the two nodes.
  Update parent or create a new parent.
  */

  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

  /*
  All existing keys plus new key should should be divided
  evenly between old (left) and new (right) nodes.
  Starting from the right, move each key to correct position.
  */
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void* destination_node;
    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
    } else {
      destination_node = old_node;
    }
    uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
    void* destination = leaf_node_cell(destination_node, index_within_node);

    if (i == cursor->cell_num) {
      serialize_row(value,
                    leaf_node_value(destination_node, index_within_node));
      *leaf_node_key(destination_node, index_within_node) = key;
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }

  /* Update cell count on both leaf nodes */
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node);
    uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
    void* parent = get_page(cursor->table->pager, parent_page_num);

    update_internal_node_key(parent, old_max, new_max);
    internal_node_insert(cursor->table, parent_page_num, new_page_num);
    return;
  }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);

  uint32_t num_cells = *leaf_node_num_cells(node);
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    // Node full
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

  if (cursor->cell_num < num_cells) {
    // Make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
  Row* row_to_insert = &(statement->row_to_insert);
  uint32_t key_to_insert = row_to_insert->id;
  Cursor* cursor = table_find(table, key_to_insert);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATE_KEY;
    }
  }

  leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
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
  char temp[COLUMN_EMAIL_SIZE + 1];
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

  if (count == 1 + (int)(schema->column_count * 3)) {
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
  } else if (count == 1 + (int)(schema->column_count * 2)) {
    /* Backward compatibility: old payload format stored name|max only. */
    for (uint32_t i = 0; i < schema->column_count; i++) {
      uint32_t parsed_max;
      strncpy(schema->column_names[i], parts[1 + (i * 2)],
              sizeof(schema->column_names[i]) - 1);
      schema->column_names[i][sizeof(schema->column_names[i]) - 1] = '\0';
      if (!parse_u32(parts[2 + (i * 2)], &parsed_max) ||
          parsed_max == 0 || parsed_max > MAX_CELL_TEXT) {
        return false;
      }
      schema->column_type[i] = COLUMN_TYPE_VARCHAR;
      schema->column_max[i] = parsed_max;
    }
  } else {
    return false;
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

bool pack_values_payload(uint32_t value_count,
                         char values[MAX_SCHEMA_COLUMNS][MAX_CELL_TEXT + 1],
                         Row* row) {
  char payload[USERNAME_SIZE + EMAIL_SIZE + 1];
  size_t used = 0;

  payload[0] = '\0';
  for (uint32_t i = 0; i < value_count; i++) {
    size_t need = strlen(values[i]) + (i == 0 ? 0 : 1);
    if (used + need >= sizeof(payload)) {
      return false;
    }
    if (i > 0) {
      payload[used++] = '|';
      payload[used] = '\0';
    }
    strcat(payload, values[i]);
    used += strlen(values[i]);
  }

  memset(row->username, 0, sizeof(row->username));
  memset(row->email, 0, sizeof(row->email));
  strncpy(row->username, payload, COLUMN_USERNAME_SIZE);
  if (used > COLUMN_USERNAME_SIZE) {
    strncpy(row->email, payload + COLUMN_USERNAME_SIZE, COLUMN_EMAIL_SIZE);
  }
  return true;
}

bool unpack_values_payload(const Row* row,
                           uint32_t expected_count,
                           char values[MAX_SCHEMA_COLUMNS][MAX_CELL_TEXT + 1]) {
  char payload[USERNAME_SIZE + EMAIL_SIZE + 1];
  char* token;
  char* saveptr = NULL;
  uint32_t count = 0;

  memset(payload, 0, sizeof(payload));
  strncpy(payload, row->username, COLUMN_USERNAME_SIZE);
  strncpy(payload + COLUMN_USERNAME_SIZE, row->email, COLUMN_EMAIL_SIZE);

  token = strtok_r(payload, "|", &saveptr);
  while (token != NULL && count < MAX_SCHEMA_COLUMNS) {
    strncpy(values[count], token, MAX_CELL_TEXT);
    values[count][MAX_CELL_TEXT] = '\0';
    count++;
    token = strtok_r(NULL, "|", &saveptr);
  }

  return count == expected_count;
}

bool make_master_tag(const char* table_name, char* out, size_t out_size) {
  int written = snprintf(out, out_size, "%s:%s", MASTER_TAG_PREFIX, table_name);
  return written >= 0 && (size_t)written < out_size;
}

Table as_table_root(Pager* pager, uint32_t root_page_num) {
  Table table;
  table.pager = pager;
  table.root_page_num = root_page_num;
  return table;
}

bool master_find_table(Table* db, const char* table_name, SqlSchema* schema, uint32_t* root_page) {
  Cursor* cursor;
  Row row;
  char tag[COLUMN_USERNAME_SIZE + 1];
  Table master_table;

  if (!make_master_tag(table_name, tag, sizeof(tag))) {
    return false;
  }

  master_table = as_table_root(db->pager, db->pager->master_root_page);
  cursor = table_start(&master_table);
  while (!cursor->end_of_table) {
    deserialize_row(cursor_value(cursor), &row);
    if (strcmp(row.username, tag) == 0) {
      bool ok = split_schema_payload(row.email, schema);
      if (ok && root_page != NULL) {
        *root_page = row.id;
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
  return root_page;
}

bool create_table_root(Table* db, uint32_t root_page) {
  void* root_node = get_page(db->pager, root_page);
  initialize_leaf_node(root_node);
  set_node_root(root_node, true);
  return true;
}

ExecuteResult insert_row_into_root(Pager* pager, uint32_t root_page, Row* row) {
  Table table = as_table_root(pager, root_page);
  Statement statement;
  statement.type = STATEMENT_INSERT;
  statement.row_to_insert = *row;
  return execute_insert(&statement, &table);
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
  char raw[COLUMN_EMAIL_SIZE + 1];
  char copy[COLUMN_EMAIL_SIZE + 1];
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
  char copy[COLUMN_EMAIL_SIZE + 1];
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

  out->value_count = 0;
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
  char copy[COLUMN_EMAIL_SIZE + 1];
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
  Row row;
  Table master = as_table_root(db->pager, db->pager->master_root_page);

  cursor = table_start(&master);
  while (!cursor->end_of_table) {
    deserialize_row(cursor_value(cursor), &row);
    if (strncmp(row.username, MASTER_TAG_PREFIX ":", 2) == 0) {
      printf("%s (root_page=%u)\n", row.username + 2, row.id);
    }
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

void execute_sql(Session* session, const SqlStatement* stmt) {
  Table* db = session->database;

  if (stmt->type == SQL_STMT_CREATE_DATABASE) {
    if (!create_database(session, stmt->table_name)) {
      printf("Error: Could not create database.\n");
      return;
    }
    printf("Executed.\n");
    return;
  }

  if (stmt->type == SQL_STMT_CREATE_TABLE) {
    SqlSchema existing;
    uint32_t root_page;
    Row master_row;
    char master_tag[COLUMN_USERNAME_SIZE + 1];
    char schema_payload[COLUMN_EMAIL_SIZE + 1];
    ExecuteResult result;

    if (strcmp(stmt->table_name, "sqlite_master") == 0) {
      printf("Error: Reserved table name.\n");
      return;
    }
    if (master_find_table(db, stmt->table_name, &existing, NULL)) {
      printf("Error: Table already exists.\n");
      return;
    }

    root_page = allocate_next_root_page(db->pager);
    create_table_root(db, root_page);

    if (!make_master_tag(stmt->table_name, master_tag, sizeof(master_tag)) ||
        !make_schema_payload(&stmt->schema, schema_payload, sizeof(schema_payload))) {
      printf("Error: Schema metadata too long.\n");
      return;
    }

    memset(&master_row, 0, sizeof(master_row));
    master_row.id = root_page;
    strcpy(master_row.username, master_tag);
    strcpy(master_row.email, schema_payload);

    result = insert_row_into_root(db->pager, db->pager->master_root_page, &master_row);
    if (result == EXECUTE_DUPLICATE_KEY) {
      printf("Error: Duplicate key.\n");
      return;
    }
    printf("Executed.\n");
    return;
  }

  if (stmt->type == SQL_STMT_INSERT) {
    SqlSchema schema;
    uint32_t root_page;
    ExecuteResult result;
    Row data_row;

    if (!master_find_table(db, stmt->table_name, &schema, &root_page)) {
      printf("Error: Table not found.\n");
      return;
    }
    if (stmt->value_count != schema.column_count) {
      printf("Error: Expected %u values.\n", schema.column_count);
      return;
    }

    for (uint32_t i = 0; i < schema.column_count; i++) {
      if (schema.column_type[i] == COLUMN_TYPE_INT) {
        uint32_t dummy;
        if (!parse_u32(stmt->values[i], &dummy)) {
          printf("Type error: expected int for column %s.\n", schema.column_names[i]);
          return;
        }
      } else if (schema.column_type[i] == COLUMN_TYPE_VARCHAR) {
        if (strlen(stmt->values[i]) > schema.column_max[i]) {
          printf("String is too long.\n");
          return;
        }
      }
    }

    memset(&data_row, 0, sizeof(data_row));
    data_row.id = stmt->id;
    if (!pack_values_payload(schema.column_count,
                             (char (*)[MAX_CELL_TEXT + 1])stmt->values,
                             &data_row)) {
      printf("Error: Row payload too long.\n");
      return;
    }

    result = insert_row_into_root(db->pager, root_page, &data_row);
    if (result == EXECUTE_DUPLICATE_KEY) {
      printf("Error: Duplicate key.\n");
      return;
    }
    printf("Executed.\n");
    return;
  }

  if (stmt->type == SQL_STMT_SELECT) {
    SqlSchema schema;
    uint32_t root_page;
    Table target;
    Cursor* cursor;
    Row row;
    char values[MAX_SCHEMA_COLUMNS][MAX_CELL_TEXT + 1];

    if (!master_find_table(db, stmt->table_name, &schema, &root_page)) {
      printf("Error: Table not found.\n");
      return;
    }

    target = as_table_root(db->pager, root_page);
    cursor = table_start(&target);
    while (!cursor->end_of_table) {
      deserialize_row(cursor_value(cursor), &row);
      if (unpack_values_payload(&row, schema.column_count, values)) {
        printf("(%u", row.id);
        for (uint32_t i = 0; i < schema.column_count; i++) {
          printf(", %s", values[i]);
        }
        printf(")\n");
      }
      cursor_advance(cursor);
    }
    free(cursor);
    printf("Executed.\n");
    return;
  }
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
      if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        if (session.database != NULL) {
          db_close(session.database);
        }
        exit(EXIT_SUCCESS);
      }

      if (strncmp(input_buffer->buffer, ".usedatabase ", 13) == 0) {
        const char* db_name = input_buffer->buffer + 13;
        if (!switch_database(&session, db_name)) {
          printf("Unable to use database.\n");
        } else {
          printf("Using database %s\n", db_name);
        }
        continue;
      }

      if (strcmp(input_buffer->buffer, ".showdatabases") == 0) {
        list_databases(session.root_path);
        continue;
      }

      if (strcmp(input_buffer->buffer, ".showtables") == 0) {
        if (!session.has_active_database || session.database == NULL) {
          printf("No active database. Use .usedatabase <name>.\n");
          continue;
        }
        show_tables(session.database);
        continue;
      }

      if (strncmp(input_buffer->buffer, ".btree ", 7) == 0) {
        const char* table_name = input_buffer->buffer + 7;
        SqlSchema schema;
        uint32_t root_page;
        if (!session.has_active_database || session.database == NULL) {
          printf("No active database. Use .usedatabase <name>.\n");
          continue;
        }
        if (!master_find_table(session.database, table_name, &schema, &root_page)) {
          printf("Error: Table not found.\n");
          continue;
        }
        printf("Tree:\n");
        print_tree(session.database->pager, root_page, 0);
        continue;
      }

      if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        continue;
      }

      printf("Unrecognized command '%s'\n", input_buffer->buffer);
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
