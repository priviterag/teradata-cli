module RubyCLITestUtils

  def logon_string
    s = ENV['TEST_LOGON_STRING'] or
        raise ArgumentError, "environ TEST_LOGON_STRING not given"
    Teradata::LogonString.parse(s)
  end

  def connect(*args)
    options = {}
    unless args.empty?
      charset, internal = args
      options[:session_charset] = charset if charset
      options[:internal_encoding] = internal if internal
    end
    Teradata::Connection.open(logon_string, options) {|conn|
      begin
        @conn = conn
        yield conn
      ensure
        @conn = nil
      end
    }
  end

  def using_test_table(name = 't', conn = @conn, &block)
    unless conn
      connect {|_conn| using_test_table(name, _conn, &block) }
      return
    end
    using_table(name, 'x INTEGER, y INTEGER', conn) {|name|
      %w(1,2 3,4 5,6).each do |values|
        insert name, values, conn
      end
      yield name
    }
  end

  def using_table(name, fields, conn = @conn, &block)
    unless conn
      connect {|_conn| using_table(name, fields, _conn, &block) }
      return
    end
    drop_table_force name, conn
    conn.execute_update "CREATE TABLE #{name} (#{fields});"
    begin
      yield name
    ensure
      drop_table_force name, conn
    end
  end

  def create_table(name, fields, conn = @conn)
    conn.execute_update "CREATE TABLE #{name} (#{fields});"
  end

  ERR_OBJECT_NOT_EXIST = 3807
  ERR_INDEX_NOT_EXIST = 3526

  def drop_table_force(name, conn = @conn)
    drop_table name, conn
  rescue Teradata::SQLError => err
    raise err unless err.code == ERR_OBJECT_NOT_EXIST
  end

  def drop_table(name, conn = @conn)
    drop 'TABLE', name, conn
  end

  def drop(type, name, conn = @conn)
    conn.execute_update "DROP #{type} #{name};"
  end

  def delete(table, conn = @conn)
    conn.execute_update "DELETE FROM #{table};"
  end

  def insert(table, values, conn = @conn)
    conn.execute_update "INSERT INTO #{table} (#{values});"
  end

  def select(table, conn = @conn)
    conn.entries "SELECT * FROM #{table} ORDER BY 1;"
  end

end
