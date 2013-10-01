#
# $Id: utils.rb 7 2010-03-04 16:54:09Z tdaoki $
#
# Copyright (C) 2009,2010 Teradata Japan, LTD.
#
# This program is free software.
# You can distribute/modify this program under the terms of
# the GNU LGPL2, Lesser General Public License version 2.
#

require 'teradata/exception'

module Teradata

  class BadLogonString < Error; end

  class LogonString
    def LogonString.intern(arg)
      arg.kind_of?(LogonString) ? arg : LogonString.parse(arg.to_s)
    end

    def LogonString.parse(str)
      m = %r<\A(?:([^/\s]+)/)?(\w+),(\w+)(?:,('.*'))?\z>.match(str) or
          raise BadLogonString, "bad logon string: #{str.inspect}"
      new(* m.captures)
    end

    def initialize(tdpid, user, password, account = nil)
      @tdpid = tdpid
      @user = user
      @password = password
      @account = account
    end

    attr_reader :tdpid
    attr_reader :user
    attr_reader :password
    attr_reader :account

    def to_s
      "#{@tdpid ? @tdpid + '/' : ''}#{@user},#{@password}#{@account ? ',' + @account : ''}"
    end

    def safe_string
      "#{@tdpid ? @tdpid + '/' : ''}#{@user},****#{@account ? ',' + @account : ''}"
    end

    def inspect
      "\#<#{self.class} #{to_s}>"
    end
  end

  class SessionCharset
    def SessionCharset.intern(arg)
      arg.kind_of?(SessionCharset) ? arg : SessionCharset.new(arg.to_s)
    end

    def initialize(name)
      @name = name
    end

    attr_reader :name
    alias to_s name

    if defined?(::Encoding)   # M17N
      def encoding
        case @name
        when /UTF8/i then Encoding::UTF_8
        when /KANJISJIS_0S/i then Encoding::Windows_31J
        when /KANJIEUC_0U/i then Encoding::EUC_JP
        when /ASCII/i then Encoding::US_ASCII
        else
          raise ArgumentError, "could not convert session charset to encoding name: #{sc.inspect}"
        end
      end
    else
      def encoding
        nil
      end
    end
  end

  module MetadataUtils
    def adjust_list_size(list, size)
      if list.size > size
        list[0...size]
      else
        list.push nil while list.size < size
        list
      end
    end
  end

  SESSION_ATTRIBUTES = [
    :user_name,
    :account_name,
    :logon_date,
    :logon_time,
    :current_database,
    :collation,
    :character_set,
    :transaction_semantics,
    :current_dateform,
    :timezone,
    :default_character_type,
    :export_latin,
    :export_unicode,
    :export_unicode_adjust,
    :export_kanjisjis,
    :export_graphic,
    :default_date_format,
    :radix_separator,
    :group_separator,
    :grouping_rule,
    :currency_radix_separator,
    :currency_graphic_rule,
    :currency_grouping_rule,
    :currency_name,
    :currency,
    :iso_currency,
    :dual_currency_name,
    :dual_currency,
    :dual_iso_currency,
    :default_byteint_format,
    :default_integer_format,
    :default_smallint_format,
    :default_numeric_format,
    :default_real_format,
    :default_time_format,
    :default_timestamp_format,
    :current_role,
    :logon_account,
    :profile,
    :ldap,
    :audit_trail_id,
    :current_isolation_level,
    :default_bigint_format,
    :query_band
  ]

  SessionInfo = Struct.new(*SESSION_ATTRIBUTES)

  class SessionInfo   # reopen
    extend MetadataUtils

    def SessionInfo.for_record(rec)
      new(* adjust_list_size(rec.to_a, SESSION_ATTRIBUTES.size))
    end
  end

  module SQLUtils
    private

    def sql_int(n)
      return 'NULL' unless n
      n
    end

    alias int sql_int

    def sql_string(str)
      return 'NULL' unless str
      "'" + str.gsub(/'/, "''") + "'"
    end

    alias string sql_string

    def sql_date(d)
      return 'NULL' unless d
      "DATE '#{d.strftime('%Y-%m-%d')}'"
    end

    alias date sql_date

    def sql_timestamp(t)
      return 'NULL' unless t
      "TIMESTAMP '#{t.strftime('%Y-%m-%d %H:%M:%S')}'"
    end

    alias timestamp sql_timestamp
  end


end
