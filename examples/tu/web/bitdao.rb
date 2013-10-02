# $Id: bitdao.rb 184 2009-08-12 08:46:22Z aamine $

class BitDAO
  module Error; end

  class BaseError < ::StandardError
    include Error
  end

  class IntegrityError < BaseError; end
  class ObjectNotExist < BaseError; end

  def error
    @connection.error_class
  end

  def sql_error
    @connection.sql_error_class
  end

  def initialize(log, connection)
    @log = log
    @connection = connection
  end

  def load_object(_class, sql)
    list = load_objects(_class, sql)
    if list.empty?
      raise ObjectNotExist, "no record exist: #{_class}"
    end
    if list.size > 1
      raise IntegrityError, "too many #{_class} loaded: #{list.size} for 1"
    end
    list.first
  end

  def load_objects(_class, sql)
    @log.info(self.class) { "[SEL] #{sql}" }
    result = []
    @connection.execute_query(sql) {|rs|
      rs.each_record do |rec|
        result.push _class.for_record(self, rec)
      end
    }
    @log.info(self.class) { "#{result.size} records" }
    result
  end

  def exec_sql(sql, level = Logger::INFO)
    @log.add(level, nil, self.class) { "[UPD] #{sql}" } if level
    @connection.execute_update sql
  end

  def transaction
    aborting = false
    exec_sql "BEGIN TRANSACTION;"
    begin
      yield
    rescue Teradata::CLI::UserAbort => err
      aborting = true
      raise err
    ensure
      if $@
        begin
          abort unless aborting
        rescue Teradata::CLI::UserAbort   # do not override original exception
        end
      else
        exec_sql "END TRANSACTION;"
      end
    end
  end

  def abort
    exec_sql "ABORT;"
  end

  private

  def int(n)
    return 'NULL' unless n
    n
  end

  def string(str)
    return 'NULL' unless str
    "'" + str.gsub(/'/, "''") + "'"
  end

  def date(d)
    return 'NULL' unless d
    "DATE '#{d.strftime('%Y-%m-%d')}'"
  end

  def timestamp(t)
    return 'NULL' unless t
    "TIMESTAMP '#{t.strftime('%Y-%m-%d %H:%M:%S')}'"
  end


  def BitDAO.define(&block)
    PersistentObject.define(&block)
  end

  class PersistentObject

    def PersistentObject.define(&block)
      c = Class.new(PersistentObject)
      c.module_eval(&block)
      c.define_initialize c.slots
      c
    end

    def PersistentObject.slot(name, _class, column = nil)
      attr_reader name
      (@slots ||= []).push Slot.new(name, _class, column)
    end

    class << self
      attr_reader :slots
    end

    class Slot
      def initialize(name, _class, column)
        @name = name
        @class = _class
        @column = column || name
      end

      attr_reader :name
      attr_reader :column

      def parse(s)
        @class.parse(s)
      end
    end

    def PersistentObject.sql_integer
      SQLInteger.new
    end

    class SQLInteger
      def parse(i)
        i
      end
    end

    def PersistentObject.sql_string
      SQLString.new
    end

    class SQLString
      # CHAR/VARCHAR field returns extra spaces, remove it always.
      def parse(str)
        str ? str.rstrip : nil
      end
    end

    def PersistentObject.sql_date
      SQLDate.new
    end

    def PersistentObject.sql_timestamp
      SQLDate.new
    end

    class SQLDate
      def parse(str)
        # "2009-01-23"
        Time.parse(str)
      end
    end

    def PersistentObject.for_record(dao, rec)
      unless rec.size >= @slots.size
        raise DatabaseError, "wrong column number of record (#{rec.size} for #{@slots.size})"
      end
      obj = new(* @slots.map {|slot| slot.parse(rec[slot.column]) })
      obj._dao = dao
      obj
    end

    def PersistentObject.define_initialize(slots)
      param_list = slots.map {|s| s.name }.join(', ')
      ivar_list = slots.map {|s| "@#{s.name}" }.join(', ')
      module_eval(<<-End, __FILE__, __LINE__ + 1)
        def initialize(#{param_list})
          #{ivar_list} = #{param_list}
        end
      End
    end

    attr_writer :_dao

  end

end
