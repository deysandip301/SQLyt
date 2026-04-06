#include "sqlyt.h"

ReadlineApi g_readline_api = {0};

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

bool init_readline_if_needed(void) {
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
  g_readline_api.using_history_fn =
      dlsym(g_readline_api.handle, "using_history");
  g_readline_api.stifle_history_fn =
      dlsym(g_readline_api.handle, "stifle_history");
  g_readline_api.completion_matches_fn =
      dlsym(g_readline_api.handle, "rl_completion_matches");
  g_readline_api.attempted_completion_slot =
      dlsym(g_readline_api.handle, "rl_attempted_completion_function");

  if (g_readline_api.readline_fn == NULL ||
      g_readline_api.add_history_fn == NULL ||
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
