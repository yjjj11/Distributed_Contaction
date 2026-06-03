#ifndef DB_PROXY_HPP
#define DB_PROXY_HPP

#include <mysql/mysql.h>
#include <string>
#include <vector>
#include <utility>
#include "db.pb.h"

// 数据库代理抽象接口 —— 更换数据库引擎只需实现此接口
class DBProxy {
public:
  virtual ~DBProxy() = default;

  virtual bool connect(const std::string& host, int port,
                       const std::string& user, const std::string& pass,
                       const std::string& dbname) = 0;
  virtual void disconnect() = 0;

  // 自动创建数据库和表
  virtual bool initSchema(const std::string& dbname) = 0;

  // 批量 CRUD —— 返回受影响行数
  virtual int64_t batchCreate(std::vector<db::User>& users) = 0;
  virtual std::vector<db::User> batchGet(const std::vector<int64_t>& ids) = 0;
  virtual int64_t batchUpdate(const std::vector<db::User>& users) = 0;
  virtual int64_t batchDelete(const std::vector<int64_t>& ids) = 0;
  virtual std::pair<std::vector<db::User>, int64_t> query(
      const std::string& field, const std::string& value,
      int64_t page, int64_t page_size) = 0;

  // ========== 好友请求 ==========
  virtual int64_t createFriendRequest(int64_t from_uid, int64_t to_uid,
                                       const std::string& from_uname,
                                       const std::string& to_uname) = 0;
  virtual std::vector<db::FriendRequestRecord> getFriendRequestsByUser(
      int64_t user_id, int32_t status) = 0;
  virtual bool updateFriendRequestStatus(int64_t id, int32_t status) = 0;

  // ========== 好友关系 ==========
  virtual bool createFriendship(int64_t user_id, int64_t friend_id,
                                 const std::string& friend_username) = 0;
  virtual std::vector<db::FriendshipRecord> getFriendships(int64_t user_id) = 0;
  virtual bool deleteFriendship(int64_t user_id, int64_t friend_id) = 0;

  // ========== 用户查询 ==========
  virtual std::pair<bool, int64_t> getUserIdByUsername(const std::string& username) = 0;

  // ========== 消息记录 ==========
  virtual int64_t batchCreateMessages(std::vector<db::MessageRecord>& msgs) = 0;
  virtual std::vector<db::MessageRecord> getMessagesBetween(
      int64_t uid_a, int64_t uid_b, int32_t limit, int32_t offset) = 0;
};

// ========== MySQL 实现 ==========

class MySQLProxy : public DBProxy {
public:
  MySQLProxy();
  ~MySQLProxy() override;

  bool connect(const std::string& host, int port,
               const std::string& user, const std::string& pass,
               const std::string& dbname) override;
  void disconnect() override;

  bool initSchema(const std::string& dbname) override;

  int64_t batchCreate(std::vector<db::User>& users) override;
  std::vector<db::User> batchGet(const std::vector<int64_t>& ids) override;
  int64_t batchUpdate(const std::vector<db::User>& users) override;
  int64_t batchDelete(const std::vector<int64_t>& ids) override;
  std::pair<std::vector<db::User>, int64_t> query(
      const std::string& field, const std::string& value,
      int64_t page, int64_t page_size) override;

  // 好友请求
  int64_t createFriendRequest(int64_t from_uid, int64_t to_uid,
                               const std::string& from_uname,
                               const std::string& to_uname) override;
  std::vector<db::FriendRequestRecord> getFriendRequestsByUser(
      int64_t user_id, int32_t status) override;
  bool updateFriendRequestStatus(int64_t id, int32_t status) override;

  // 好友关系
  bool createFriendship(int64_t user_id, int64_t friend_id,
                         const std::string& friend_username) override;
  std::vector<db::FriendshipRecord> getFriendships(int64_t user_id) override;
  bool deleteFriendship(int64_t user_id, int64_t friend_id) override;

  // 用户查询
  std::pair<bool, int64_t> getUserIdByUsername(const std::string& username) override;

  // 消息记录
  int64_t batchCreateMessages(std::vector<db::MessageRecord>& msgs) override;
  std::vector<db::MessageRecord> getMessagesBetween(
      int64_t uid_a, int64_t uid_b, int32_t limit, int32_t offset) override;

private:
  MYSQL* conn_ = nullptr;

  std::string userToValues(const db::User& u);
  db::User rowToUser(MYSQL_ROW row);
};

#endif // DB_PROXY_HPP
