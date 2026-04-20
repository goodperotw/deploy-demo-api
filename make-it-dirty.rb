require 'pg'

# table initialize
db_info = {
  host: ENV['DB_HOST'],
  dbname: ENV['DB_NAME'],
  user: ENV['DB_USER'],
  password: ENV['DB_PASS']
}
conn = PG.connect(db_info)
dirty_sql = <<~SQL
  UPDATE url_mappings
  SET long_url = long_url || 'g'
  WHERE id BETWEEN 106 AND 164
SQL
conn.exec(dirty_sql)
