require 'sinatra'
require 'pg'
require 'json'
require 'securerandom'

sleep 10

logger = Logger.new(STDERR)

# table initialize
db_info = {
  host: ENV['DB_HOST'],
  dbname: ENV['DB_NAME'],
  user: ENV['DB_USER'],
  password: ENV['DB_PASS']
}
conn = PG.connect(db_info)
init_sql = <<~SQL
  CREATE TABLE IF NOT EXISTS url_mappings (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(10) NOT NULL,
    long_url TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
  );

  CREATE UNIQUE INDEX IF NOT EXISTS uidx_slug_lookup
  ON url_mappings(slug)
  INCLUDE (long_url);

  CREATE UNIQUE INDEX IF NOT EXISTS uidx_long_url_lookup ON url_mappings (long_url);
SQL

conn.exec(init_sql)


set :bind, '0.0.0.0'
# allow hosts app[1-9].pero.pc
set :host_authorization, { permitted_hosts: (1..9).map { "app#{_1}.pero.pc" } + ['lb.pero.pc', 'localhost'] }

before do
  if request.content_type == 'application/json' && request.body.size > 0
    request.body.rewind
    payload = JSON.parse(request.body.read)
    params.merge!(payload)
  end
end

def random_slug
  SecureRandom.alphanumeric(ENV['SLUG_SIZE'] || 5)
end

post '/api/v1/url' do
  content_type :json

  # ignore the case that has duplicated long_url, it's NORMAL
  insert_or_get_sql = <<~SQL
    WITH inserted AS (
      INSERT INTO url_mappings (slug, long_url)
      VALUES ($1, $2)
      ON CONFLICT (long_url) DO NOTHING
      RETURNING slug
    )
    SELECT * FROM inserted
    UNION ALL
    SELECT slug FROM url_mappings WHERE long_url = $2
  SQL

  error = nil
  error_status_code = nil
  result = nil
  # set the maximum times limit for slug retry
  3.times do
    begin
      slug = random_slug
      result = conn.exec_params(insert_or_get_sql, [slug, params[:url]])
      break
    rescue PG::UniqueViolation => err
      # if slug conflict, retry it
      if err.message.include?('uidx_slug_lookup')
        logger.error("slug '#{slug}' is duplicated, retrying...")
        next
      end

      error = { code: 'UNKNOWN_CONSTRAINT_ERROR', message: 'unexpected data constraint error happened' }
      error_status_code = 500
      logger.error("unexpected db constraint error happended: #{err.message}")
    rescue => err
      error = { code: 'UNKNOWN_ERROR', message: 'unexpected error happened' }
      error_status_code = 500
      logger.error("unexpected error happended: #{err.message}")
    end
  end

  if result.nil?
    error = { code: 'ERROR_GEN_SLUG', message: 'error happened when generating slug' }
    error_status_code = 500
    logger.error('slug retry times reach limit, probably requires a slug length extension')
  end

  if error
    status error_status_code
    return error.to_json
  end
  # if conflict happened on slug, retry multiple times
  { data: { slug: result.first['slug'] } }.to_json
end

get '/api/v1/hi' do
  content_type :json

  { message: 'hi(updated-2-launch-so-slow)' }.to_json
end

get '/:slug' do
  query_sql = <<~SQL
    SELECT long_url FROM url_mappings
    WHERE slug = $1
  SQL
  result = conn.exec_params(query_sql, [params[:slug]])

  if result.first.nil?
    status 404
    content_type :html

    return '<h1>Url Not Found</h1>'
  end

  return redirect result.first['long_url'], 301
end
