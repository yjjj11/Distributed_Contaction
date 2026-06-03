#include "db_proxy.hpp"
#include <cstdio>
#include <set>
#include <sstream>

MySQLProxy::MySQLProxy()  = default;
MySQLProxy::~MySQLProxy() { disconnect(); }

// ---------- connect / disconnect ----------

bool MySQLProxy::connect(const std::string& host, int port,
                         const std::string& user, const std::string& pass,
                         const std::string& dbname) {
  if (conn_) mysql_close(conn_);
  conn_ = mysql_init(nullptr);
  if (!conn_) {
    fprintf(stderr, "[MySQL] mysql_init failed\n");
    return false;
  }
  if (!mysql_real_connect(conn_, host.c_str(), user.c_str(), pass.c_str(),
                          nullptr, port, nullptr, 0)) {
    fprintf(stderr, "[MySQL] connect error: %s\n", mysql_error(conn_));
    return false;
  }
  if (mysql_select_db(conn_, dbname.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create database ...\n");
  }
  return true;
}

void MySQLProxy::disconnect() {
  if (conn_) {
    mysql_close(conn_);
    conn_ = nullptr;
  }
}

// ---------- schema ----------

bool MySQLProxy::initSchema(const std::string& dbname) {
  std::string sql = "CREATE DATABASE IF NOT EXISTS `" + dbname + "` "
                    "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create db error: %s\n", mysql_error(conn_));
    return false;
  }
  if (mysql_select_db(conn_, dbname.c_str()) != 0) {
    fprintf(stderr, "[MySQL] select db error: %s\n", mysql_error(conn_));
    return false;
  }

  sql = R"(CREATE TABLE IF NOT EXISTS users (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    username   VARCHAR(64)  NOT NULL UNIQUE,
    password   VARCHAR(128) NOT NULL,
    qq_email   VARCHAR(128) DEFAULT '',
    nickname   VARCHAR(64)  DEFAULT '',
    phone      VARCHAR(20)  DEFAULT '',
    avatar_url VARCHAR(255) DEFAULT '',
    status     TINYINT      DEFAULT 1,
    created_at DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4)";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create users error: %s\n", mysql_error(conn_));
    return false;
  }

  // 好友请求表
  sql = R"(CREATE TABLE IF NOT EXISTS friend_requests (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    from_user_id  BIGINT       NOT NULL,
    to_user_id    BIGINT       NOT NULL,
    from_username VARCHAR(64)  NOT NULL DEFAULT '',
    to_username   VARCHAR(64)  NOT NULL DEFAULT '',
    status        TINYINT      NOT NULL DEFAULT 0,
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_to_user (to_user_id, status)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4)";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create friend_requests error: %s\n", mysql_error(conn_));
    return false;
  }

  // 好友关系表
  sql = R"(CREATE TABLE IF NOT EXISTS friendships (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id       BIGINT       NOT NULL,
    friend_id     BIGINT       NOT NULL,
    friend_username VARCHAR(64) NOT NULL DEFAULT '',
    created_at    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_friend (user_id, friend_id),
    INDEX idx_user (user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4)";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create friendships error: %s\n", mysql_error(conn_));
    return false;
  }

  // 消息记录表
  sql = R"(CREATE TABLE IF NOT EXISTS messages (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    from_user_id BIGINT      NOT NULL,
    to_user_id  BIGINT       NOT NULL,
    content     TEXT,
    msg_type    TINYINT      DEFAULT 0,
    status      TINYINT      DEFAULT 0,
    created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_users (from_user_id, to_user_id, created_at),
    INDEX idx_user_time (to_user_id, created_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4)";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] create messages error: %s\n", mysql_error(conn_));
    return false;
  }

  return true;
}

// ---------- User CRUD (existing) ----------

std::string MySQLProxy::userToValues(const db::User& u) {
  char buf[4096];
  std::string pwd = u.password();
  // escape
  char* epwd = (char*)malloc(pwd.size() * 2 + 1);
  mysql_real_escape_string(conn_, epwd, pwd.data(), pwd.size());
  std::string qq = u.qq_email();
  char* eqq = (char*)malloc(qq.size() * 2 + 1);
  mysql_real_escape_string(conn_, eqq, qq.data(), qq.size());
  std::string nn = u.nickname();
  char* enn = (char*)malloc(nn.size() * 2 + 1);
  mysql_real_escape_string(conn_, enn, nn.data(), nn.size());
  std::string ph = u.phone();
  char* eph = (char*)malloc(ph.size() * 2 + 1);
  mysql_real_escape_string(conn_, eph, ph.data(), ph.size());
  std::string av = u.avatar_url();
  char* eav = (char*)malloc(av.size() * 2 + 1);
  mysql_real_escape_string(conn_, eav, av.data(), av.size());
  std::string un = u.username();
  char* eun = (char*)malloc(un.size() * 2 + 1);
  mysql_real_escape_string(conn_, eun, un.data(), un.size());

  snprintf(buf, sizeof(buf),
    "('%s','%s','%s','%s','%s','%s',%d)",
    eun, epwd, eqq, enn, eph, eav, u.status());

  free(epwd); free(eqq); free(enn); free(eph); free(eav); free(eun);
  return buf;
}

db::User MySQLProxy::rowToUser(MYSQL_ROW row) {
  db::User u;
  if (row[0]) u.set_id(atoll(row[0]));
  if (row[1]) u.set_username(row[1]);
  if (row[2]) u.set_password(row[2]);
  if (row[3]) u.set_qq_email(row[3]);
  if (row[4]) u.set_nickname(row[4]);
  if (row[5]) u.set_phone(row[5]);
  if (row[6]) u.set_avatar_url(row[6]);
  if (row[7]) u.set_status(atoi(row[7]));
  if (row[8]) u.set_created_at(row[8]);
  if (row[9]) u.set_updated_at(row[9]);
  return u;
}

int64_t MySQLProxy::batchCreate(std::vector<db::User>& users) {
  if (users.empty()) return 0;
  std::string sql = "INSERT INTO users (username,password,qq_email,nickname,phone,avatar_url,status) VALUES ";
  for (size_t i = 0; i < users.size(); ++i) {
    if (i > 0) sql += ",";
    sql += userToValues(users[i]);
  }
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] batchCreate error: %s\n", mysql_error(conn_));
    return -1;
  }
  int64_t affected = mysql_affected_rows(conn_);
  // 给传入的 users 设置自增 id
  int64_t insert_id = mysql_insert_id(conn_);
  for (size_t i = 0; i < users.size() && insert_id > 0; ++i) {
    users[i].set_id(insert_id + i);
  }
  return affected;
}

std::vector<db::User> MySQLProxy::batchGet(const std::vector<int64_t>& ids) {
  std::vector<db::User> result;
  if (ids.empty()) return result;
  std::string sql = "SELECT * FROM users WHERE id IN (";
  for (size_t i = 0; i < ids.size(); ++i) {
    if (i > 0) sql += ",";
    sql += std::to_string(ids[i]);
  }
  sql += ")";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] batchGet error: %s\n", mysql_error(conn_));
    return result;
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (!res) return result;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    result.push_back(rowToUser(row));
  }
  mysql_free_result(res);
  return result;
}

int64_t MySQLProxy::batchUpdate(const std::vector<db::User>& users) {
  int64_t total = 0;
  for (auto& u : users) {
    std::string pwd = u.password();
    char epwd[256];
    mysql_real_escape_string(conn_, epwd, pwd.data(), pwd.size());
    char sql[4096];
    snprintf(sql, sizeof(sql),
      "UPDATE users SET password='%s', qq_email='%s', nickname='%s', "
      "phone='%s', avatar_url='%s', status=%d WHERE id=%ld",
      epwd, u.qq_email().c_str(), u.nickname().c_str(),
      u.phone().c_str(), u.avatar_url().c_str(), u.status(), (long)u.id());
    if (mysql_query(conn_, sql) != 0) {
      fprintf(stderr, "[MySQL] batchUpdate error: %s\n", mysql_error(conn_));
      return -1;
    }
    total += mysql_affected_rows(conn_);
  }
  return total;
}

int64_t MySQLProxy::batchDelete(const std::vector<int64_t>& ids) {
  if (ids.empty()) return 0;
  std::string sql = "DELETE FROM users WHERE id IN (";
  for (size_t i = 0; i < ids.size(); ++i) {
    if (i > 0) sql += ",";
    sql += std::to_string(ids[i]);
  }
  sql += ")";
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] batchDelete error: %s\n", mysql_error(conn_));
    return -1;
  }
  return mysql_affected_rows(conn_);
}

std::pair<std::vector<db::User>, int64_t> MySQLProxy::query(
    const std::string& field, const std::string& value,
    int64_t page, int64_t page_size) {
  std::vector<db::User> users;
  int64_t total = 0;

  // 校验 field 白名单，防止 SQL 注入
  static const std::set<std::string> allowed_fields = {"id", "username", "status"};
  if (allowed_fields.find(field) == allowed_fields.end()) {
    fprintf(stderr, "[MySQL] rejected disallowed query field: %s\n", field.c_str());
    return {users, 0};
  }

  // count
  char sql[4096];
  char escaped_val[256];
  mysql_real_escape_string(conn_, escaped_val, value.data(), value.size());
  snprintf(sql, sizeof(sql),
    "SELECT COUNT(*) FROM users WHERE `%s`='%s'", field.c_str(), escaped_val);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] query count error: %s\n", mysql_error(conn_));
    return {users, 0};
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (res) {
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row) total = atoll(row[0]);
    mysql_free_result(res);
  }

  if (total == 0) return {users, 0};

  int64_t offset = (page - 1) * page_size;
  snprintf(sql, sizeof(sql),
    "SELECT * FROM users WHERE `%s`='%s' LIMIT %ld OFFSET %ld",
    field.c_str(), escaped_val, (long)page_size, (long)offset);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] query error: %s\n", mysql_error(conn_));
    return {users, total};
  }
  res = mysql_store_result(conn_);
  if (res) {
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)))
      users.push_back(rowToUser(row));
    mysql_free_result(res);
  }
  return {users, total};
}

// ========== 好友请求 ==========

int64_t MySQLProxy::createFriendRequest(int64_t from_uid, int64_t to_uid,
                                         const std::string& from_uname,
                                         const std::string& to_uname) {
  char from_u[128], to_u[128];
  mysql_real_escape_string(conn_, from_u, from_uname.data(), from_uname.size());
  mysql_real_escape_string(conn_, to_u, to_uname.data(), to_uname.size());
  char sql[4096];
  snprintf(sql, sizeof(sql),
    "INSERT INTO friend_requests (from_user_id,to_user_id,from_username,to_username,status) "
    "VALUES (%ld,%ld,'%s','%s',0)",
    (long)from_uid, (long)to_uid, from_u, to_u);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] createFriendRequest error: %s\n", mysql_error(conn_));
    return -1;
  }
  return mysql_insert_id(conn_);
}

std::vector<db::FriendRequestRecord> MySQLProxy::getFriendRequestsByUser(
    int64_t user_id, int32_t status) {
  std::vector<db::FriendRequestRecord> result;
  char sql[4096];
  if (status < 0) {
    snprintf(sql, sizeof(sql),
      "SELECT * FROM friend_requests WHERE to_user_id=%ld ORDER BY created_at DESC",
      (long)user_id);
  } else {
    snprintf(sql, sizeof(sql),
      "SELECT * FROM friend_requests WHERE to_user_id=%ld AND status=%d ORDER BY created_at DESC",
      (long)user_id, status);
  }
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] getFriendRequestsByUser error: %s\n", mysql_error(conn_));
    return result;
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (!res) return result;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    db::FriendRequestRecord r;
    if (row[0]) r.set_id(atoll(row[0]));
    if (row[1]) r.set_from_user_id(atoll(row[1]));
    if (row[2]) r.set_to_user_id(atoll(row[2]));
    if (row[3]) r.set_from_username(row[3]);
    if (row[4]) r.set_to_username(row[4]);
    if (row[5]) r.set_status(atoi(row[5]));
    if (row[6]) r.set_created_at(row[6]);
    if (row[7]) r.set_updated_at(row[7]);
    result.push_back(std::move(r));
  }
  mysql_free_result(res);
  return result;
}

bool MySQLProxy::updateFriendRequestStatus(int64_t id, int32_t status) {
  char sql[4096];
  snprintf(sql, sizeof(sql),
    "UPDATE friend_requests SET status=%d WHERE id=%ld", status, (long)id);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] updateFriendRequestStatus error: %s\n", mysql_error(conn_));
    return false;
  }
  return mysql_affected_rows(conn_) > 0;
}

// ========== 好友关系 ==========

bool MySQLProxy::createFriendship(int64_t user_id, int64_t friend_id,
                                   const std::string& friend_username) {
  char fname[128];
  mysql_real_escape_string(conn_, fname, friend_username.data(), friend_username.size());
  char sql[4096];
  // 双向添加好友关系
  snprintf(sql, sizeof(sql),
    "INSERT IGNORE INTO friendships (user_id,friend_id,friend_username) VALUES (%ld,%ld,'%s')",
    (long)user_id, (long)friend_id, fname);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] createFriendship error: %s\n", mysql_error(conn_));
    return false;
  }
  // 反向也添加
  snprintf(sql, sizeof(sql),
    "INSERT IGNORE INTO friendships (user_id,friend_id) VALUES (%ld,%ld)",
    (long)friend_id, (long)user_id);
  mysql_query(conn_, sql);
  return true;
}

std::vector<db::FriendshipRecord> MySQLProxy::getFriendships(int64_t user_id) {
  std::vector<db::FriendshipRecord> result;
  char sql[4096];
  snprintf(sql, sizeof(sql),
    "SELECT f.id, f.user_id, f.friend_id, "
    "COALESCE((SELECT username FROM users WHERE id=f.friend_id), f.friend_username) as friend_name, "
    "f.created_at FROM friendships f WHERE f.user_id=%ld ORDER BY f.created_at DESC",
    (long)user_id);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] getFriendships error: %s\n", mysql_error(conn_));
    return result;
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (!res) return result;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    db::FriendshipRecord r;
    if (row[0]) r.set_id(atoll(row[0]));
    if (row[1]) r.set_user_id(atoll(row[1]));
    if (row[2]) r.set_friend_id(atoll(row[2]));
    if (row[3]) r.set_friend_username(row[3]);
    if (row[4]) r.set_created_at(row[4]);
    result.push_back(std::move(r));
  }
  mysql_free_result(res);
  return result;
}

bool MySQLProxy::deleteFriendship(int64_t user_id, int64_t friend_id) {
  char sql[4096];
  snprintf(sql, sizeof(sql),
    "DELETE FROM friendships WHERE (user_id=%ld AND friend_id=%ld) OR (user_id=%ld AND friend_id=%ld)",
    (long)user_id, (long)friend_id, (long)friend_id, (long)user_id);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] deleteFriendship error: %s\n", mysql_error(conn_));
    return false;
  }
  return true;
}

// ========== 用户查询 ==========

std::pair<bool, int64_t> MySQLProxy::getUserIdByUsername(const std::string& username) {
  char esc[128];
  mysql_real_escape_string(conn_, esc, username.data(), username.size());
  char sql[4096];
  snprintf(sql, sizeof(sql), "SELECT id FROM users WHERE username='%s' LIMIT 1", esc);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] getUserIdByUsername error: %s\n", mysql_error(conn_));
    return {false, 0};
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (!res) return {false, 0};
  MYSQL_ROW row = mysql_fetch_row(res);
  if (!row) { mysql_free_result(res); return {false, 0}; }
  int64_t id = atoll(row[0]);
  mysql_free_result(res);
  return {true, id};
}

// ========== 消息记录 ==========

int64_t MySQLProxy::batchCreateMessages(std::vector<db::MessageRecord>& msgs) {
  if (msgs.empty()) return 0;
  std::string sql = "INSERT INTO messages (from_user_id,to_user_id,content,msg_type,status) VALUES ";
  for (size_t i = 0; i < msgs.size(); ++i) {
    if (i > 0) sql += ",";
    auto& m = msgs[i];
    char content_esc[8192];
    mysql_real_escape_string(conn_, content_esc, m.content().data(), m.content().size());
    char buf[4096];
    snprintf(buf, sizeof(buf), "(%ld,%ld,'%s',%d,%d)",
      (long)m.from_user_id(), (long)m.to_user_id(), content_esc, m.msg_type(), m.status());
    sql += buf;
  }
  if (mysql_query(conn_, sql.c_str()) != 0) {
    fprintf(stderr, "[MySQL] batchCreateMessages error: %s\n", mysql_error(conn_));
    return -1;
  }
  return mysql_affected_rows(conn_);
}

std::vector<db::MessageRecord> MySQLProxy::getMessagesBetween(
    int64_t uid_a, int64_t uid_b, int32_t limit, int32_t offset) {
  std::vector<db::MessageRecord> result;
  char sql[4096];
  snprintf(sql, sizeof(sql),
    "SELECT * FROM messages WHERE "
    "(from_user_id=%ld AND to_user_id=%ld) OR (from_user_id=%ld AND to_user_id=%ld) "
    "ORDER BY created_at DESC LIMIT %d OFFSET %d",
    (long)uid_a, (long)uid_b, (long)uid_b, (long)uid_a, limit, offset);
  if (mysql_query(conn_, sql) != 0) {
    fprintf(stderr, "[MySQL] getMessagesBetween error: %s\n", mysql_error(conn_));
    return result;
  }
  MYSQL_RES* res = mysql_store_result(conn_);
  if (!res) return result;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    db::MessageRecord m;
    if (row[0]) m.set_id(atoll(row[0]));
    if (row[1]) m.set_from_user_id(atoll(row[1]));
    if (row[2]) m.set_to_user_id(atoll(row[2]));
    if (row[3]) m.set_content(row[3]);
    if (row[4]) m.set_msg_type(atoi(row[4]));
    if (row[5]) m.set_status(atoi(row[5]));
    if (row[6]) m.set_created_at(row[6]);
    result.push_back(std::move(m));
  }
  mysql_free_result(res);
  return result;
}
