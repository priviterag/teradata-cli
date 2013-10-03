require 'teradata-cli'
require 'test/unit'
libdir = File.dirname(__FILE__) 
$LOAD_PATH.unshift libdir unless $LOAD_PATH.include?(libdir)
require 'rubyclitestutils'

class Test_Teradata_DBObject < Test::Unit::TestCase

  include RubyCLITestUtils

  def test_s_intern_string
    t = TeradataCli::Table.intern('db.tab')
    assert_instance_of TeradataCli::Table, t
    assert_equal 'db.tab', t.name
  end

  def test_s_intern_class
    t0 = TeradataCli::Table.new('db', 'tab')
    t = TeradataCli::Table.intern(t0)
    assert_instance_of TeradataCli::Table, t
    assert_equal t0, t
    assert_equal 'db.tab', t.name
  end

  def test_names_qualified
    t = TeradataCli::Table.new('bwtest', 'tab')
    assert_equal 'bwtest', t.database
    assert_equal 'bwtest.tab', t.name
    assert_equal 'tab', t.unqualified_name
  end

  def test_names_unqualified
    t = TeradataCli::Table.new('tab')
    assert_nil t.database
    assert_equal 'tab', t.name
    assert_equal 'tab', t.unqualified_name
  end

  def test_to_s
    assert_equal 'db.tab', TeradataCli::Table.new('db', 'tab').to_s
    assert_equal 'bwtest.tab', TeradataCli::Table.new('bwtest', 'tab').to_s
    assert_equal 'tab', TeradataCli::Table.new('tab').to_s
  end

  def test_EQ
    a = TeradataCli::Table.new('db', 'tab')
    b = TeradataCli::Table.new('db', 'tab')
    assert_equal a, a
    assert_equal a, b
    assert_equal b, a

    # object name is different
    c = TeradataCli::Table.new('db', 'other')
    assert_not_equal a, c
    assert_not_equal c, a

    # database name is different
    d = TeradataCli::Table.new('other', 'tab')
    assert_not_equal a, d
    assert_not_equal d, a

    # database name is missing
    e = TeradataCli::Table.new('tab')
    assert_not_equal a, e
    assert_not_equal e, a

    # same name, but different type
    v = TeradataCli::View.new('db', 'tab')
    assert_not_equal a, v
    assert_not_equal v, a
  end

  def test_type_char
    assert_equal 'T', TeradataCli::Table.type_char
    assert_equal 'V', TeradataCli::View.type_char
    assert_equal 'M', TeradataCli::Macro.type_char
    assert_equal 'N', TeradataCli::HashIndex.type_char
  end

  def test_type_name
    assert_equal 'TABLE', TeradataCli::Table.type_name
    assert_equal 'VIEW', TeradataCli::View.type_name
    assert_equal 'MACRO', TeradataCli::Macro.type_name
    assert_equal 'JOIN INDEX', TeradataCli::JoinIndex.type_name
    assert_equal 'HASH INDEX', TeradataCli::HashIndex.type_name
    assert_equal 'PROCEDURE', TeradataCli::Procedure.type_name
  end

  def test_table_size
    t = TeradataCli::Table.new('db', 'tbl', 17, 39)
    assert_equal 17, t.size
    assert_equal 17, t.current_perm
    assert_equal 39, t.peak_perm
  end

  def test_table_no_peak
    t = TeradataCli::Table.new('db', 'tbl', 17)
    assert_equal 17, t.size
    assert_equal 17, t.current_perm
    assert_nil t.peak_perm
  end

  def test_table_size_none
    t = TeradataCli::Table.new('db', 'tbl')
    assert_nil t.size
    assert_nil t.current_perm
    assert_nil t.peak_perm
  end

  def test_root_database
    connect {|conn|
      dbc = conn.root_database
      assert_kind_of TeradataCli::Database, dbc
      assert_instance_of TeradataCli::User, dbc
      assert_equal 'dbc', dbc.name.downcase
      assert_equal true, dbc.user?
    }
  end

  def test_database
    connect {|conn|
      dbc = conn.database('dbc')
      assert_instance_of TeradataCli::User, dbc
      assert_equal 'dbc', dbc.name.downcase
      assert_equal true, dbc.user?
    }
  end

  def test_Database_hier
    connect {|conn|
      dbc = conn.dbc
      assert_nil dbc.owner
      assert_equal [], dbc.parents

      cs = dbc.children
      assert_equal true, (cs.size > 0)
      assert_kind_of TeradataCli::Database, cs.first
      assert_kind_of TeradataCli::Database, cs.first.owner
      assert_equal 'dbc', cs.first.owner.name.downcase

      sysdba = cs.detect {|c| c.name.downcase == 'sysdba' }
      assert_instance_of TeradataCli::User, sysdba
      assert_equal true, sysdba.user?
      assert_instance_of TeradataCli::User, sysdba.parent

      syslib = cs.detect {|c| c.name.downcase == 'syslib' }
      assert_instance_of TeradataCli::Database, syslib
      assert_equal false, syslib.user?
      assert_instance_of TeradataCli::User, syslib.parent
    }
  end

end
