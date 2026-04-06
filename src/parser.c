#include "sqlyt.h"

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

static void skip_spaces(const char** p) {
  while (**p == ' ' || **p == '\t' || **p == '\n' || **p == '\r') {
    (*p)++;
  }
}

static bool parse_identifier_token_ci(const char** p, char* out,
                                      size_t out_size) {
  size_t len = 0;
  const char* s;
  skip_spaces(p);
  s = *p;
  if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || *s == '_')) {
    return false;
  }
  while (((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
          (*s >= '0' && *s <= '9') || *s == '_') &&
         len + 1 < out_size) {
    out[len++] = *s;
    s++;
  }
  out[len] = '\0';
  if (!is_valid_identifier(out, out_size - 1)) {
    return false;
  }
  *p = s;
  return true;
}

static bool consume_keyword_ci(const char** p, const char* keyword) {
  size_t klen = strlen(keyword);
  const char* s;
  skip_spaces(p);
  s = *p;
  if (strncasecmp(s, keyword, klen) != 0) {
    return false;
  }
  if ((s[klen] >= 'a' && s[klen] <= 'z') ||
      (s[klen] >= 'A' && s[klen] <= 'Z') ||
      (s[klen] >= '0' && s[klen] <= '9') || s[klen] == '_') {
    return false;
  }
  *p = s + klen;
  return true;
}

static bool parse_sql_value_token(const char** p, char* out, size_t out_size) {
  size_t len = 0;
  const char* s;
  skip_spaces(p);
  s = *p;
  if (*s == '"') {
    s++;
    while (*s != '\0' && *s != '"') {
      if (len + 1 >= out_size) {
        return false;
      }
      out[len++] = *s;
      s++;
    }
    if (*s != '"') {
      return false;
    }
    s++;
  } else {
    while (*s != '\0' && *s != ',' && *s != ')' && *s != ';' && *s != ' ' &&
           *s != '\t' && *s != '\n' && *s != '\r') {
      if (len + 1 >= out_size) {
        return false;
      }
      out[len++] = *s;
      s++;
    }
  }
  out[len] = '\0';
  if (len == 0) {
    return false;
  }
  *p = s;
  return true;
}

static bool parse_insert_values_clause(const char* values_start,
                                       SqlStatement* out) {
  const char* p = values_start;
  uint32_t row = 0;
  skip_spaces(&p);
  while (*p != '\0') {
    uint32_t col = 0;
    if (row >= MAX_INSERT_ROWS) {
      return false;
    }
    if (*p != '(') {
      return false;
    }
    p++;
    skip_spaces(&p);
    while (true) {
      if (col >= MAX_SCHEMA_COLUMNS) {
        return false;
      }
      if (!parse_sql_value_token(&p, out->values[row][col],
                                 sizeof(out->values[row][col]))) {
        return false;
      }
      col++;
      skip_spaces(&p);
      if (*p == ',') {
        p++;
        skip_spaces(&p);
        continue;
      }
      if (*p == ')') {
        p++;
        break;
      }
      return false;
    }

    if (col == 0) {
      return false;
    }
    out->row_value_count[row] = col;
    if (strcasecmp(out->values[row][0], "null") == 0) {
      out->row_auto_id[row] = true;
      out->row_ids[row] = 0;
    } else {
      out->row_auto_id[row] = false;
      if (!parse_u32(out->values[row][0], &out->row_ids[row])) {
        return false;
      }
    }

    row++;
    skip_spaces(&p);
    if (*p == ',') {
      p++;
      skip_spaces(&p);
      continue;
    }
    break;
  }

  skip_spaces(&p);
  if (*p == ';') {
    p++;
    skip_spaces(&p);
  }
  if (*p != '\0' || row == 0) {
    return false;
  }
  out->row_count = row;
  return true;
}

size_t bounded_text_len(const char* text, size_t max_len) {
  size_t n = 0;
  while (n < max_len && text[n] != '\0') {
    n++;
  }
  return n;
}

void print_table_border(const size_t* widths, uint32_t count) {
  printf("+");
  for (uint32_t i = 0; i < count; i++) {
    for (size_t j = 0; j < widths[i] + 2; j++) {
      printf("-");
    }
    printf("+");
  }
  printf("\n");
}

void print_table_cell_text(const char* text, size_t width, bool right_align) {
  size_t len = strlen(text);
  size_t pad = (width > len) ? (width - len) : 0;

  printf(" ");
  if (right_align) {
    for (size_t i = 0; i < pad; i++) {
      printf(" ");
    }
    printf("%s", text);
  } else {
    printf("%s", text);
    for (size_t i = 0; i < pad; i++) {
      printf(" ");
    }
  }
  printf(" ");
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
    if (parsed_type != COLUMN_TYPE_INT && parsed_type != COLUMN_TYPE_TEXT) {
      return false;
    }
    if (parsed_type == COLUMN_TYPE_INT) {
      parsed_max = 0;
    }
    if (parsed_type == COLUMN_TYPE_TEXT &&
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

bool pack_insert_row_values(const char values[MAX_SCHEMA_COLUMNS][256],
                            const SqlSchema* schema, const RowLayout* L,
                            uint8_t* out) {
  memset(out, 0, L->value_size);
  for (uint32_t i = 0; i < schema->column_count; i++) {
    uint32_t off = L->col_offset[i];
    uint32_t sz = L->col_size[i];
    if (schema->column_type[i] == COLUMN_TYPE_INT) {
      uint32_t v;
      if (!parse_u32(values[i], &v)) {
        return false;
      }

      memcpy(out + off, &v, sizeof(uint32_t));
    } else {
      const char* s = values[i];
      size_t len = strlen(s);
      if (len > sz) {
        return false;
      }
      memcpy(out + off, s, len);
    }
  }
  return true;
}

bool parse_u32_tokens_for_insert_values(
    const char values[MAX_SCHEMA_COLUMNS][256], const SqlSchema* schema,
    uint32_t key) {
  for (uint32_t i = 0; i < schema->column_count; i++) {
    if (schema->column_type[i] == COLUMN_TYPE_INT) {
      uint32_t v;
      if (!parse_u32(values[i], &v)) {
        printf("Type error: expected int for column %s.\n",
               schema->column_names[i]);
        return false;
      }
      if (i == 0 && v != key) {
        return false;
      }
    } else if (schema->column_type[i] == COLUMN_TYPE_TEXT) {
      if (strlen(values[i]) > schema->column_max[i]) {
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
    if (strcasecmp(name_buf, table_name) == 0) {
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

uint32_t find_table_max_key(Table* table, bool* has_rows) {
  Cursor* cursor = table_start(table);
  uint32_t max_key = 0;

  *has_rows = false;
  while (!cursor->end_of_table) {
    void* node = get_page(table->pager, cursor->page_num);
    max_key = *tbl_leaf_key(table, node, cursor->cell_num);
    *has_rows = true;
    cursor_advance(cursor);
  }

  free(cursor);
  return max_key;
}

bool table_has_key(Table* table, uint32_t key) {
  Cursor* cursor = table_find(table, key);
  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  bool found = false;

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *tbl_leaf_key(table, node, cursor->cell_num);
    found = (key_at_index == key);
  }

  free(cursor);
  return found;
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
  if (count < 8 || strcasecmp(tokens[0], "create") != 0 ||
      strcasecmp(tokens[1], "table") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  if (count < 8 || strcasecmp(tokens[3], "id") != 0 ||
      strcasecmp(tokens[4], "int") != 0) {
    return false;
  }

  index = 5;
  if (index + 1 < count && strcasecmp(tokens[index], "primary") == 0 &&
      strcasecmp(tokens[index + 1], "key") == 0) {
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

    if (strcasecmp(tokens[index + 1], "int") == 0) {
      max_len = 0;
      out->schema.column_type[out->schema.column_count] = COLUMN_TYPE_INT;
      advance = 2;
    } else if (strcasecmp(tokens[index + 1], "text") == 0) {
      max_len = MAX_CELL_TEXT;
      out->schema.column_type[out->schema.column_count] = COLUMN_TYPE_TEXT;
      advance = 2;
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
  const char* p = line;
  const char* values_start;

  memset(out->row_value_count, 0, sizeof(out->row_value_count));
  memset(out->row_ids, 0, sizeof(out->row_ids));
  memset(out->row_auto_id, 0, sizeof(out->row_auto_id));
  out->row_count = 0;

  if (!consume_keyword_ci(&p, "insert")) {
    return false;
  }
  if (!consume_keyword_ci(&p, "into")) {
    return false;
  }
  if (!parse_identifier_token_ci(&p, out->table_name, sizeof(out->table_name))) {
    return false;
  }
  if (!consume_keyword_ci(&p, "values")) {
    return false;
  }
  values_start = p;

  if (!parse_insert_values_clause(values_start, out)) {
    return false;
  }

  out->id = out->row_count > 0 ? out->row_ids[0] : 0;
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
  if (count != 3 || strcasecmp(tokens[0], "create") != 0 ||
      strcasecmp(tokens[1], "database") != 0) {
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

  if (count != 4 || strcasecmp(tokens[0], "select") != 0 ||
      strcmp(tokens[1], "*") != 0 || strcasecmp(tokens[2], "from") != 0) {
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

bool parse_delete_from(char* line, SqlStatement* out) {
  char copy[SQL_LINE_BUF];
  char* tokens[16];
  int count;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';
  
  for (int i = 0; copy[i] != '\0'; i++) {
    if (copy[i] == '=') {
      copy[i] = ' ';
    }
  }
  
  count = split_tokens_sql(copy, tokens, 16);

  // e.g. "delete from user where id = 1" becomes tokens ["delete", "from", "user", "where", "id", "1"]
  if (count != 6 || strcasecmp(tokens[0], "delete") != 0 ||
      strcasecmp(tokens[1], "from") != 0 || strcasecmp(tokens[3], "where") != 0 ||
      strcasecmp(tokens[4], "id") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  strncpy(out->table_name, tokens[2], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';
  
  if (!parse_u32(tokens[5], &out->id)) {
    return false;
  }

  out->type = SQL_STMT_DELETE;
  return true;
}

bool parse_update_table(char* line, SqlStatement* out) {
  char copy[SQL_LINE_BUF];
  char* tokens[32];
  int count;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';

  for (int i = 0; copy[i] != '\0'; i++) {
    if (copy[i] == '=') {
      copy[i] = ' ';
    }
  }
  count = split_tokens_sql(copy, tokens, 32);

  if (count != 8 || strcasecmp(tokens[0], "update") != 0 ||
      strcasecmp(tokens[2], "set") != 0 || strcasecmp(tokens[5], "where") != 0 ||
      strcasecmp(tokens[6], "id") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[1], MAX_TABLE_NAME_LENGTH) ||
      !is_valid_identifier(tokens[3], MAX_COLUMN_NAME_LENGTH)) {
    return false;
  }
  if (!parse_u32(tokens[7], &out->id)) {
    return false;
  }

  strncpy(out->table_name, tokens[1], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';
  strncpy(out->update_column, tokens[3], sizeof(out->update_column) - 1);
  out->update_column[sizeof(out->update_column) - 1] = '\0';
  strncpy(out->update_value, tokens[4], sizeof(out->update_value) - 1);
  out->update_value[sizeof(out->update_value) - 1] = '\0';
  out->type = SQL_STMT_UPDATE;
  return true;
}

bool parse_drop_table(char* line, SqlStatement* out) {
  char copy[SQL_LINE_BUF];
  char* tokens[8];
  int count;

  strncpy(copy, line, sizeof(copy) - 1);
  copy[sizeof(copy) - 1] = '\0';
  count = split_tokens_sql(copy, tokens, 8);
  if (count != 3 || strcasecmp(tokens[0], "drop") != 0 ||
      strcasecmp(tokens[1], "table") != 0) {
    return false;
  }
  if (!is_valid_identifier(tokens[2], MAX_TABLE_NAME_LENGTH)) {
    return false;
  }

  strncpy(out->table_name, tokens[2], sizeof(out->table_name) - 1);
  out->table_name[sizeof(out->table_name) - 1] = '\0';
  out->type = SQL_STMT_DROP_TABLE;
  return true;
}

bool parse_sql_statement(char* line, SqlStatement* out) {
  return parse_create_database(line, out) || parse_create_table(line, out) ||
         parse_insert_into(line, out) || parse_select_from(line, out) ||
         parse_delete_from(line, out) || parse_update_table(line, out) ||
         parse_drop_table(line, out);
}

