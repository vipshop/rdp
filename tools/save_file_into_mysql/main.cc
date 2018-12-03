//
//Copyright 2018 vip.com.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
//the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
//an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
//specific language governing permissions and limitations under the License.
//


#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include <string>  
#include <fstream>  
#include <sstream>  
#include <iostream>

#include <mysql.h>

using namespace std;

int SaveIntoDb(MYSQL *db_conn, const string &table, const string &column, const string &content) {
  // Store the file content into mysql, not very graceful ^_^!
  string sql = "INSERT INTO " + table +" SET " + column + "=?";
  MYSQL_STMT *stmt;    
  MYSQL_BIND bind[1];  

  stmt = mysql_stmt_init(db_conn);
  if (mysql_stmt_prepare(stmt, sql.c_str(), sql.size())) {
    cerr << "mysql_stmt_prepare() failed: " << mysql_stmt_error(stmt) << endl;
    mysql_stmt_close(stmt);
    return -1;
  }

  memset(bind, 0, sizeof(bind));
  bind[0].buffer_type= MYSQL_TYPE_BLOB;    
  bind[0].buffer= (void*)(content.c_str());
  bind[0].buffer_length= content.size();

  if (mysql_stmt_bind_param(stmt, bind)) {    
    cerr << "mysql_stmt_bind_param() failed: " << mysql_stmt_error(stmt) << endl;
    mysql_stmt_close(stmt);
    return -1;
  }    

  if (mysql_stmt_execute(stmt)) {    
    cerr << "mysql_stmt_execute() failed: " << mysql_stmt_error(stmt) << endl;
    mysql_stmt_close(stmt);
    return -1;
  }    
  mysql_stmt_close(stmt);

  return 0;
}

MYSQL* InitDbConn(const string &host, int port, 
    const string &user, const string &passwd, const string &database) {
  MYSQL *db_conn = mysql_init(NULL);
  if (!db_conn) {
    cerr << "mysql_init failed" << endl;
    return NULL;
  }

  if (!mysql_real_connect(db_conn, host.c_str(), user.c_str(), 
        passwd.c_str(), database.c_str(), port, NULL, 0)) {
    cerr << "Connect mysql " << host << ":" << port << 
      " failed: " << mysql_error(db_conn);
    mysql_close(db_conn);
    return NULL;
  }
  if (mysql_set_character_set(db_conn, "utf8")) {
    cerr << "Set charset failed: " << mysql_error(db_conn) << endl;
    mysql_close(db_conn);
    return NULL;
  }

  return db_conn; 

}

int main(int argc, char **argv) {
  MYSQL *db_conn;
  string host, port, user, passwd, database, table, column;
  string file_name;
  char ch;  
  while((ch = getopt(argc, argv, "h:P:u:p:d:t:c:f:")) != -1) {  
    switch(ch){
      cout << ch << ":" << optarg << endl;
      case 'h': 
        host = optarg;
        break;
      case 'P': 
        port = optarg;
        break;
      case 'u': 
        user = optarg;
        break;
      case 'p': 
        passwd = optarg;
        break;
      case 'd': 
        database = optarg;
        break;
      case 't': 
        table = optarg;
        break;
      case 'c': 
        column = optarg;
        break;
      case 'f': 
        file_name = optarg;
        break;
      default:
        cerr << "Unkown option: " << ch << endl;
        return -1;

    }
  }

  if (host.empty() || port.empty() || user.empty() || passwd.empty() 
      || database.empty() || table.empty() || column.empty() 
      || file_name.empty()) {
    cerr << "Usage: " << argv[0] << " -h<host> -P<port> -u<user> -p<passwd> -d<database> -t<table> -c<column> -f<file_name>" << endl;
    return -1; 
  }

  std::ifstream in_file;
  in_file.open(file_name.c_str());  
  if (in_file.fail()) {
    cerr << "Failed to open file: " << file_name << endl;
    return -1;
  }
  std::stringstream buffer;  
  buffer << in_file.rdbuf();  

  if ( (db_conn = InitDbConn(host, atoi(port.c_str()), user, passwd, database)) == NULL) {
    return -1;
  }
  if (SaveIntoDb(db_conn, table, column, buffer.str())) {
    return -1;
  }

  return 0;
}
