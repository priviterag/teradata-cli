#
# $Id: dbobject.rb 7 2010-03-04 16:54:09Z tdaoki $
#
# Copyright (C) 2009,2010 Teradata Japan, LTD.
#
# This program is free software.
# You can distribute/modify this program under the terms of
# the GNU LGPL2, Lesser General Public License version 2.
#

require 'teradata/utils'
require 'teradata/connection'
require 'teradata/exception'

module Teradata

  class ObjectError < Error; end

  class Connection   # reopen

    include SQLUtils

    ROOT_DATABASE_NAME = 'DBC'

    def root_database
      User.new(ROOT_DATABASE_NAME, self)
    end

    alias dbc root_database

    def database(name)
      kind = database_kind(name) or
          raise ObjectError, "no such database: #{name.inspect}"
      kind.new(name, self)
    end

    def database_kind(name)
      recs = entries(<<-EndSQL)
        SELECT dbKind
        FROM dbc.databases
        WHERE databaseName = #{sql_string name}
      EndSQL
      return nil if recs.empty?
      if recs.size > 1
        raise "multiple database entries exist in dbc.databases???: #{name.inspect}"
      end
      class_from_kind_char(recs.first[0].strip.upcase)
    end

    def database_exist?(name)
      database_kind(name) ? true : false
    end

    alias database? database_exist?

    def user_exist?(name)
      database_kind(name) == User
    end

    alias user? user_exist?

    def parent_databases(name)
      parents = [Database.new(name, self)]
      while true
        db = database_owner(parents.last.name)
        break unless db
        parents.push db
      end
      parents.shift   # remove myself
      parents
    end

    # Database owner.
    # Returns nil for root database (DBC).
    def database_owner(name)
      return nil if name.downcase == 'dbc'
      owners = entries(<<-EndSQL).map {|rec| [rec[0].strip.upcase, rec[1].strip] }
        SELECT owner.dbKind, self.ownerName
        FROM dbc.databases self INNER JOIN dbc.databases owner
                ON self.ownerName = owner.databaseName
        WHERE self.databaseName = #{sql_string name}
      EndSQL
      if owners.empty?
        raise ObjectError, "database not exist: #{name.inspect}"
      end
      if owners.size > 1
        raise "multiple database entries exist in dbc.databases???: #{name.inspect}"
      end
      kind_char, owner = owners.first
      return nil if owner.downcase == name.downcase
      new_database(kind_char, owner)
    end

    def child_databases(name)
      entries(<<-EndSQL).map {|rec| new_database(rec[0].strip.upcase, rec[1].strip) }
        SELECT dbKind, databaseName
        FROM dbc.databases
        WHERE ownerName = #{sql_string name}
      EndSQL
    end

    def class_from_kind_char(c)
      c == 'U' ? User : Database
    end
    private :class_from_kind_char

    def new_database(kind_char, name)
      class_from_kind_char(kind_char).new(name, self)
    end
    private :new_database

    Perms = Struct.new(:current, :max, :peak)

    def database_own_perms(name)
      perms = entries(<<-EndSQL).first
        SELECT
                sum(currentPerm)
                , sum(maxPerm)
                , sum(peakPerm)
        FROM dbc.diskSpace
        WHERE databaseName = #{sql_string name}
      EndSQL
      unless perms
        raise ObjectError, "database does not exist in dbc.diskSpace: #{name.inspect}"
      end
      Perms.new(* perms.to_a.map {|n| n.to_i })
    end

    def database_total_perms(name)
      recs = entries(<<-EndSQL)
        SELECT
                sum(ds.currentPerm)
                , sum(ds.maxPerm)
                , sum(ds.peakPerm)
        FROM
                (
                  (SELECT d.databaseName, d.databaseName FROM dbc.databases d)
                  UNION
                  (SELECT parent, child FROM dbc.children)
                ) as c (parent, child)
                INNER JOIN dbc.diskSpace ds
                ON c.child = ds.databaseName
        WHERE
                c.parent = #{sql_string name}
      EndSQL
      if recs.empty?
        raise ObjectError, "database does not exist in dbc.diskSpace: #{@name.inspect}"
      end
      if recs.size > 1
        raise "multiple database entry exist on dbc.diskSpace???: #{name.inspect}; size=#{recs.size}"
      end
      Perms.new(* recs.first.to_a.map {|n| n.to_i })
    end

    def tables(database)
      recs = entries(<<-EndSQL)
        SELECT trim(tableName)
                , sum(currentPerm)
                , sum(peakPerm)
        FROM dbc.tableSize
        WHERE databaseName = #{sql_string database}
        GROUP BY tableName
      EndSQL
      c = ::Teradata::Table
      recs.map {|rec|
        name, curr, peak = *rec.to_a
        c.new(database, name, curr.to_i, peak.to_i)
      }
    end

    def views(database)
      fetch_objects(database, ::Teradata::View)
    end

    def fetch_objects(database, obj_class)
      # FIXME??: use dbc.tvm
      entries("HELP DATABASE #{database}")\
          .select {|rec| rec[1].strip.upcase == obj_class.type_char }\
          .map {|rec| obj_class.new(database, rec[0].strip) }
    end
    private :fetch_objects

    def objects(database)
      entries("HELP DATABASE #{database}").map {|rec|
        ::Teradata::DBObject.create(rec[1].strip, database, rec[0].strip)
      }
    end

    def column(obj, name)
      recs = entries(<<-EndSQL)
        SELECT * FROM dbc.columns
        WHERE databaseName = #{sql_string obj.database}
                AND tableName = #{sql_string obj.unqualified_name}
                AND columnName = #{sql_string name}
      EndSQL
      unless recs.size == 1
        raise ArgumentError, "could not specify a column: #{obj.name}.#{name}"
      end
      Column.for_record(recs.first)
    end

  end


  class Database

    def initialize(name, conn)
      @name = name
      @connection = conn
      invalidate_cache
    end

    attr_reader :name

    def invalidate_cache
      @parents = nil
      @children = nil
      @tables = nil
      @own_perms = nil
      @total_perms = nil
    end

    def inspect
      "\#<#{self.class} #{@name}>"
    end

    def user?
      false
    end

    def owner
      parents.first
    end

    alias parent owner

    def parents
      @parents ||= @connection.parent_databases(@name)
    end

    def children
      @children ||= @connection.child_databases(@name)
    end

    def tables
      @tables ||= @connection.tables(@name)
    end

    def own_current_perm
      load_own_perms
      @own_perms.current
    end

    alias current_perm own_current_perm

    def own_max_perm
      load_own_perms
      @own_perms.max
    end

    alias max_perm own_max_perm

    def own_peak_perm
      load_own_perms
      @own_perms.peak
    end

    alias peak_perm own_peak_perm

    def load_own_perms
      @own_perms ||= @connection.database_own_perms(@name)
    end
    private :load_own_perms

    def total_current_perm
      load_total_perms
      @total_perms.current
    end

    def total_max_perm
      load_total_perms
      @total_perms.max
    end

    def total_peak_perm
      load_total_perms
      @total_perms.peak
    end

    def load_total_perms
      @total_perms ||= @connection.database_total_perms(@name)
    end
    private :load_total_perms

  end


  class User < Database

    def user?
      true
    end

  end


  class DBObject

    def DBObject.intern(spec)
      spec.kind_of?(DBObject) ? spec : parse(spec)
    end

    def DBObject.parse(spec)
      new(* spec.split('.', 2))
    end

    OBJECT_TYPES = {}

    class << DBObject
      def declare_type(c, name)
        OBJECT_TYPES[c] = self
        @type_char = c
        @type_name = name
      end

      attr_reader :type_char
      attr_reader :type_name
    end

    def DBObject.create(type_char, *args)
      cls = OBJECT_TYPES[type_char] or
          raise ArgumentError, "unknown type char: #{type_char.inspect}"
      cls.new(*args)
    end

    def initialize(x, y = nil)
      if y
        @database = x
        @name = y
      else
        @database = nil
        @name = x
      end
    end

    attr_reader :database

    def unqualified_name
      @name
    end

    def name
      @database ? "#{@database}.#{@name}" : @name
    end

    alias to_s name

    def inspect
      "\#<#{self.class} #{name}>"
    end

    def ==(other)
      other.kind_of?(self.class) and self.name == other.name
    end

    def type_char
      self.class.type_char
    end

    def type_name
      self.class.type_name
    end

  end


  class Table < DBObject
    declare_type 'T', 'TABLE'

    def initialize(x, y = nil, curr = nil, peak = nil)
      super x, y
      @current_perm = curr
      @peak_perm = peak
    end
    
    attr_reader :current_perm
    alias size current_perm
    attr_reader :peak_perm
  end

  class View < DBObject
    declare_type 'V', 'VIEW'
  end

  class Macro < DBObject
    declare_type 'M', 'MACRO'
  end

  class Procedure < DBObject
    declare_type 'P', 'PROCEDURE'
  end

  class JoinIndex < DBObject
    declare_type 'J', 'JOIN INDEX'
  end

  class HashIndex < DBObject
    declare_type 'N', 'HASH INDEX'
  end

  COLUMN_ATTRIBUTES = [
    :database_name,
    :table_name,
    :column_name,
    :column_format,
    :column_title,
    :sp_parameter_type,
    :column_type,
    :column_udt_name,
    :column_length,
    :default_value,
    :nullable,
    :comment_string,
    :decimal_total_digits,
    :decimal_fractional_digits,
    :column_id,
    :upper_case_flag,
    :comprressible,
    :compress_value,
    :column_constraint,
    :constraint_count,
    :creator_name,
    :create_timestamp,
    :last_alter_name,
    :last_alter_timestamp,
    :char_type,
    :id_col_type,
    :access_count,
    :last_access_timestamp,
    :compress_value_list
  ]

  Column = Struct.new(* COLUMN_ATTRIBUTES)

  class Column   # reopen
    extend MetadataUtils

    def Column.for_record(rec)
      new(* adjust_list_size(rec.to_a, COLUMN_ATTRIBUTES.size))
    end
  end

end
