require 'teradata'
require 'test/unit'
libdir = File.dirname(__FILE__)
$LOAD_PATH.unshift libdir unless $LOAD_PATH.include?(libdir)
require 'rubyclitestutils'

class Test_Record < Test::Unit::TestCase

  include RubyCLITestUtils

  def test_integers
    using_table(get_table_name('ints'), 'b BYTEINT, s SMALLINT, i INTEGER, q BIGINT') {|name|
      insert name, '7, 777, 777777, 777777777'
      rec = select(name).first
      assert_equal 4, rec.size
      assert_equal 7, rec[:b]
      assert_equal 777, rec[:s]
      assert_equal 777777, rec[:i]
      assert_equal 777777777, rec[:q]
    }
  end

  def test_real_numbers
    using_table(get_table_name('nums'), 'f FLOAT, d1 DECIMAL(3,0), d2 DECIMAL(15,1)') {|name|
      insert name, '1.6, 123, 12345678901234.5'
      rec = select(name).first
      assert_equal 3, rec.size
      assert_in_delta 1.6, rec[:f], 0.005
      assert_equal '123', rec[:d1]
      assert_equal '12345678901234.5', rec[:d2]
    }
  end

  def test_strings
    using_table(get_table_name('strs'), 'c CHAR(4), vc VARCHAR(4), b BYTE(4), vb VARBYTE(4)') {|name|
      insert name, "'ab', 'cd', '6566'XBF, '6768'XBV"
      rec = select(name).first
      assert_equal 4, rec.size
      assert_equal 'ab', rec[:c].rstrip
      assert_equal 'cd', rec[:vc].rstrip
      assert_equal 'ef', rec[:b].rstrip
      assert_equal 'gh', rec[:vb].rstrip
    }
  end

  def test_session_charset_UTF8
    connect('UTF8') {
      using_table(get_table_name('strs'), 'c CHAR(1) CHARACTER SET UNICODE, vc VARCHAR(1) CHARACTER SET UNICODE') {|name|
        insert name, utf8("'\343\201\202', '\343\201\202'")
        rec = select(name).first
        assert_equal 2, rec.size
        assert_equal utf8("\343\201\202"), rec[:c].rstrip
        assert_equal utf8("\343\201\202"), rec[:vc].rstrip
      }
    }
  end

  # TODO Teradata::CLIError: CLI error: [EM_227] MTDP: EM_CHARNAME(227): invalid character set name specified.
  #def test_session_charset_EUC
  #  connect('KANJIEUC_0U') {
  #    using_table(get_table_name('strs'), 'c CHAR(1) CHARACTER SET UNICODE, vc VARCHAR(1) CHARACTER SET UNICODE') {|name|
  #      insert name, euc("'\xA4\xA2', '\xA4\xA2'")
  #      rec = select(name).first
  #      assert_equal 2, rec.size
  #      assert_equal euc("\xA4\xA2"), rec[:c].rstrip
  #      assert_equal euc("\xA4\xA2"), rec[:vc].rstrip
  #    }
  #  }
  #end

  # TODO Teradata::CLIError: CLI error: [EM_227] MTDP: EM_CHARNAME(227): invalid character set name specified.
  #def test_session_charset_SJIS
  #  connect('KANJISJIS_0S') {
  #    using_table(get_table_name('strs'), 'c CHAR(1) CHARACTER SET UNICODE, vc VARCHAR(1) CHARACTER SET UNICODE') {|name|
  #      insert name, sjis("'\202\240', '\202\240'")
  #      rec = select(name).first
  #      assert_equal 2, rec.size
  #      assert_equal sjis("\202\240"), rec[:c].rstrip
  #      assert_equal sjis("\202\240"), rec[:vc].rstrip
  #    }
  #  }
  #end

  if defined?(::Encoding)   # Ruby 1.9
    # with external encoding (session charset), without internal encoding
    def test_encoding
      connect('UTF8') {
        using_table(get_table_name('strs'), 'c CHAR(1) CHARACTER SET UNICODE, vc VARCHAR(1) CHARACTER SET UNICODE') {|name|
          insert name, utf8("'\343\201\202', '\343\201\202'")
          rec = select(name).first
          assert_equal 2, rec.size
          assert_equal ::Encoding::UTF_8, rec[:c].encoding
          assert_equal utf8("\343\201\202"), rec[:c].rstrip
          assert_equal ::Encoding::UTF_8, rec[:vc].encoding
          assert_equal utf8("\343\201\202"), rec[:vc].rstrip
        }
      }
    end

    # with external and internal encoding
    def test_enc_conversion
      connect('UTF8', Encoding::EUC_JP) {
        using_table(get_table_name('strs'), 'c CHAR(1) CHARACTER SET UNICODE, vc VARCHAR(1) CHARACTER SET UNICODE') {|name|
          insert name, euc("'\xA4\xA2', '\xA4\xA2'")
          rec = select(name).first
          assert_equal 2, rec.size
          assert_equal ::Encoding::EUC_JP, rec[:c].encoding
          assert_equal euc("\xA4\xA2"), rec[:c].rstrip
          assert_equal ::Encoding::EUC_JP, rec[:vc].encoding
          assert_equal euc("\xA4\xA2"), rec[:vc].rstrip
        }
      }
    end
  end

  if defined?(::Encoding)
    # Ruby 1.9
    def utf8(str)
      str.force_encoding ::Encoding::UTF_8
      str
    end

    def euc(str)
      str.force_encoding ::Encoding::EUC_JP
      str
    end

    def sjis(str)
      str.force_encoding ::Encoding::Windows_31J
      str
    end
  else
    # Ruby 1.8
    def utf8(str) str end
    def euc(str) str end
    def sjis(str) str end
  end

  def test_datetimes
    using_table(get_table_name('times'), 'd DATE, t TIME(1), ts TIMESTAMP(1)') {|name|
      insert name, "DATE '2009-01-23', TIME '12:34:56.0', TIMESTAMP '2009-01-23 12:34:56.0'"
      rec = select(name).first
      assert_equal 3, rec.size
      assert_equal '2009-01-23', rec[:d]
      assert_equal '12:34:56.0', rec[:t]
      assert_equal '2009-01-23 12:34:56.0', rec[:ts]
    }
  end

  def test_values_at
    using_table(get_table_name('t'), 'x INTEGER, y INTEGER, z INTEGER') {|name|
      insert name, '1,2,3'
      rec = select(name).first
      assert_equal 3, rec.size
      assert_equal [], rec.values_at
      assert_equal [1], rec.values_at(:x)
      assert_equal [2,3], rec.values_at(:y, :z)
      assert_equal [3,2], rec.values_at(:z, :y)
      assert_equal [1,2,3], rec.values_at(:x, :y, :z)
      assert_equal [3,2,1], rec.values_at(:z, :y, :x)
      assert_equal [3,1,2], rec.values_at(:z, :x, :y)
    }
  end

  INTEGER_N = 497

  def test_field
    connect {
      using_table(get_table_name('t'), 'x INTEGER, y INTEGER, z INTEGER') {|name|
        insert name, '1,2,NULL'
        rec = select(name).first
        assert_equal 3, rec.size
        assert_instance_of Teradata::Field, rec.field(:x)
        assert_instance_of Teradata::Field, rec.field(:y)
        assert_instance_of Teradata::Field, rec.field(:z)
        assert_equal 1, rec.field(:x).value
        assert_equal 2, rec.field(:y).value
        assert_equal nil, rec.field(:z).value
        assert_equal 'x', rec.field(:x).name
        assert_equal 'y', rec.field(:y).name
        assert_equal 'z', rec.field(:z).name
        assert_equal :INTEGER_N, rec.field(:x).type
        assert_equal INTEGER_N, rec.field(:x).type_code
        assert_equal false, rec.field(:x).null?
        assert_equal true, rec.field(:z).null?
      }
    }
  end

  def test_to_a
    connect {|conn|
      conn.execute_query('select 1, 2, 3') {|rs|
        rs.each do |rec|
          assert_equal [1,2,3], rec.to_a
        end
      }
    }
  end

  def test_to_h
    connect {|conn|
      conn.execute_query('select 1 as a, 2 as b, 3 as c') {|rs|
        rs.each do |rec|
          assert_equal({"a" => 1, "b" => 2, "c" => 3}, rec.to_h)
        end
      }
    }
  end

end
