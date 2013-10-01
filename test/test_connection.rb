require 'teradata'
require 'test/unit'
libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift libdir unless $LOAD_PATH.include?(libdir)
require 'rubyclitestutils'

class Test_Connection < Test::Unit::TestCase

  include RubyCLITestUtils

  def test_s_open
    begin
      conn = Teradata::Connection.open(logon_string)
      assert_instance_of Teradata::Connection, conn
      assert_equal false, conn.closed?
    ensure
      begin
        conn.close
      rescue
      end
      assert_equal true, conn.closed?
    end

    Teradata::Connection.open(logon_string) {|conn|
      assert_instance_of Teradata::Connection, conn
      assert_equal false, conn.closed?
    }
    assert_equal true, conn.closed?
  end

  def test_execute_update
    connect {
      drop_table_force 't'
      x = @conn.execute_update("CREATE TABLE t (x INTEGER);")
      assert_instance_of Teradata::ResultSet, x
      assert_equal true, x.closed?
    }
  end

  def test_txn
    using_table('t', "x INTEGER, y INTEGER") {|name|
      @conn.execute_update "BEGIN TRANSACTION;"
      @conn.execute_update "INSERT INTO #{name} (x,y) VALUES (1,2);"
      @conn.execute_update "INSERT INTO #{name} (x,y) VALUES (3,4);"
      @conn.execute_update "END TRANSACTION;"
      recs = @conn.entries("SELECT * FROM #{name} ORDER BY 1;")
      assert_equal 2, recs.size

      begin
        @conn.execute_update "BEGIN TRANSACTION;"
        @conn.execute_update "DELETE FROM #{name};"
        @conn.execute_update "ABORT;"
      rescue Teradata::UserAbort
      end
      recs = @conn.entries("SELECT * FROM #{name} ORDER BY 1;")
      assert_equal 2, recs.size
      assert_equal 1, recs[0][:x]
    }
  end

  def test_execute_query
    using_test_table {|name|
      _test_single_rs name, @conn
      _test_single_rs2 name, @conn
      _test_multiple_rs name, @conn
    }
  end

  def _test_single_rs(name, conn)
    buf = []
    conn.execute_query("SELECT * FROM #{name} ORDER BY 1") {|rs|
      assert_instance_of Teradata::ResultSet, rs
      rs.each do |rec|
        buf.push rec
      end
    }
    assert_equal 3, buf.size
    assert_instance_of Teradata::Record, buf[0]
    assert_equal 1, buf[0][:x]
    assert_equal 2, buf[0][:y]
    assert_instance_of Teradata::Record, buf[1]
    assert_equal 3, buf[1][:x]
    assert_equal 4, buf[1][:y]
    assert_instance_of Teradata::Record, buf[2]
    assert_equal 5, buf[2][:x]
    assert_equal 6, buf[2][:y]
  end

  def _test_single_rs2(name, conn)
    buf = []
    num_rs = 0
    conn.execute_query("SELECT * FROM #{name} ORDER BY 1") {|sets|
      assert_instance_of Teradata::ResultSet, sets
      sets.each_result_set do |rs|
        num_rs += 1
        assert_instance_of Teradata::ResultSet, rs
        rs.each do |rec|
          buf.push rec
        end
      end
    }
    assert_equal 1, num_rs
    assert_equal 3, buf.size
    buf.each do |r|
      assert_instance_of Teradata::Record, r
    end
    assert_equal [1,2], [buf[0][:x], buf[0][:y]]
    assert_equal [3,4], [buf[1][:x], buf[1][:y]]
    assert_equal [5,6], [buf[2][:x], buf[2][:y]]
  end

  def _test_multiple_rs(name, conn)
    buf = []
    num_rs = 0
    conn.execute_query(
        "SELECT * FROM #{name} ORDER BY 1;
         SELECT * FROM #{name} ORDER BY 1 DESC;") {|sets|
      assert_instance_of Teradata::ResultSet, sets
      sets.each_result_set do |rs|
        num_rs += 1
        assert_instance_of Teradata::ResultSet, rs
        rs.each do |rec|
          buf.push rec
        end
      end
    }
    assert_equal 2, num_rs
    assert_equal 6, buf.size
    buf.each do |r|
      assert_instance_of Teradata::Record, r
    end
    assert_equal [1,2], [buf[0][:x], buf[0][:y]]
    assert_equal [3,4], [buf[1][:x], buf[1][:y]]
    assert_equal [5,6], [buf[2][:x], buf[2][:y]]
    assert_equal [5,6], [buf[3][:x], buf[3][:y]]
    assert_equal [3,4], [buf[4][:x], buf[4][:y]]
    assert_equal [1,2], [buf[5][:x], buf[5][:y]]
  end

  def test_execute_query_without_block
    using_test_table {|name|
      rs = @conn.execute_query('SELECT * FROM t ORDER BY 1;')

      recs = []
      rs.each do |rec|
        recs.push rec
      end
      assert_equal 3, recs.size
      assert_equal 1, recs[0][:x]
      assert_equal 6, recs[2][:y]

      recs = rs.entries
      assert_equal 3, recs.size
      assert_equal 1, recs[0][:x]
      assert_equal 6, recs[2][:y]
    }
  end

  def test_entries
    using_test_table {|name|
      recs = @conn.entries("SELECT * FROM #{name} ORDER BY 1;")
      assert_equal 3, recs.size
      assert_equal 1, recs[0][:x]
      assert_equal 6, recs[2][:y]
    }
  end

  # Teradata hates "\n", check it.
  def test_line_terms
    using_test_table {|name|
      recs = @conn.entries("SELECT *\nFROM #{name} \n ORDER BY 1;")
      assert_equal 3, recs.size

      assert_nothing_thrown {
        @conn.execute_update "INSERT INTO #{name}\n(x,y)\nVALUES \n (7,8);"
      }
      recs = @conn.entries("SELECT *\nFROM #{name} \n ORDER BY 1;")
      assert_equal 4, recs.size
    }
  end

  # connection/request intersection test
  def test_duplicated_connections
    assert_nothing_thrown {
      connect {|c1|
        connect {|c2|
          drop_table_force 't', c1
          c2.execute_update "CREATE TABLE t (x INTEGER);"
          drop_table_force 't', c1
          c2.execute_update "CREATE TABLE t (x INTEGER);"
          drop_table_force 't', c1
        }
      }
    }
  end

  def test_tables
    db = logon_string.user
    connect {|conn|
      assert_equal [], @conn.tables(db)
      using_test_table('t1') {
        using_test_table('t2') {
          list = @conn.tables(db).sort_by {|t| t.name }
          assert_equal [
            Teradata::Table.new(db, 't1'),
            Teradata::Table.new(db, 't2')
          ], list
          assert_equal 'size', list[0].size > 0 ? 'size' : 'empty'
          assert_equal 'size', list[1].size > 0 ? 'size' : 'empty'
        }
      }
    }
  end

  def test_views
    db = logon_string.user
    using_test_table {
      assert_equal [], @conn.views(db)
      using_view('v', 'select 1 as i') {|name|
        assert_equal [Teradata::View.new(db, name)], @conn.views(db)
      }
    }
  end

  def test_objects
    db = logon_string.user
    connect {
      assert_equal [], @conn.objects(db)
      using_test_table {|table|
        assert_equal [Teradata::Table.new(db, table)], @conn.objects(db)
        using_view('v', 'select 1 as i') {|view|
          assert_equal [
            Teradata::Table.new(db, table),
            Teradata::View.new(db, view)
          ], @conn.objects(db).sort_by {|obj| [obj.type_char, obj.name] }
        }
      }
    }
  end

  def using_view(name, query, conn = @conn)
    drop_view_force name, conn
    begin
      conn.execute_update "CREATE VIEW #{name} AS #{query}"
      yield name
    ensure
      drop_view_force name, conn
    end
  end

  def drop_view_force(name, conn = @conn)
    conn.execute_update "DROP VIEW #{name}"
  rescue Teradata::SQLError
  end

  def test_info
    connect {
      info = @conn.info
      assert_instance_of Teradata::SessionInfo, info
      assert_equal logon_string.user.downcase, info.user_name.downcase
    }
  end

  def test_column
    db = logon_string.user
    using_table('t', "x INTEGER, y INTEGER") {|name|
      col = @conn.column(Teradata::Table.new(db, 't'), 'x')
      assert_instance_of Teradata::Column, col
      assert_equal 'x', col.column_name.strip.downcase
    }
  end

  def test_transaction
    connect {|conn|
      using_test_table('t') {|table|
        n_records = count(table, conn)

        # transaction fails #1
        assert_raise(RuntimeError) {
          conn.transaction {
            conn.query "DELETE FROM #{table}"
            raise RuntimeError, "USER ABORT"
          }
        }
        assert_equal n_records, count(table, conn)

        # transaction fails #2
        assert_raise(Teradata::UserAbort) {
          conn.transaction {
            conn.query "DELETE FROM #{table}"
            conn.abort
          }
        }
        assert_equal n_records, count(table, conn)

        # transaction success
        conn.transaction {
          conn.query "DELETE FROM #{table}"
        }
        assert_equal 0, count(table, conn)
      }
    }
  end

  def count(table, conn)
    conn.entries("SELECT count(*) FROM #{table}").first[0]
  end

end
