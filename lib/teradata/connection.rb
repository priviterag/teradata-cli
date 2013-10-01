#
# $Id: connection.rb 7 2010-03-04 16:54:09Z tdaoki $
#
# Copyright (C) 2009,2010 Teradata Japan, LTD.
#
# This program is free software.
# You can distribute/modify this program under the terms of
# the GNU LGPL2, Lesser General Public License version 2.
#

require 'teradata/cli'
require 'teradata/utils'
require 'teradata/exception'
require 'forwardable'
require 'stringio'

module Teradata

  class ConnectionError < CLIError; end
  class MetaDataFormatError < CLIError; end

  class SQLError < CLIError
    def initialize(code, info, message)
      super message
      @code = code
      @info = info
    end

    attr_reader :code
    attr_reader :info
  end

  class UserAbort < SQLError; end

  def Teradata.connect(*args, &block)
    Connection.open(*args, &block)
  end

  class Connection

    class << self
      alias open new
    end

    def Connection.default_session_charset
      Teradata::SessionCharset.new('UTF8')
    end

    def initialize(logon_string, options = {})
      session_charset = options[:session_charset] || Connection.default_session_charset
      internal_encoding = options[:internal_encoding] || default_internal_encoding()
      @logger = options[:logger] || NullLogger.new
      @logon_string = Teradata::LogonString.intern(logon_string)
      @session_charset = Teradata::SessionCharset.intern(session_charset)
      @external_encoding = @session_charset.encoding
      @internal_encoding = internal_encoding
      ex = StringExtractor.get(@external_encoding, @internal_encoding)
      log { "session charset = #{@session_charset}" }
      log { "external encoding = #{@external_encoding}" }
      log { "internal encoding = #{@internal_encoding}" }
      log { "logon... (#{@logon_string.safe_string})" }
      @cli = CLI.new(logon_string.to_s, @session_charset.name)
      log { "logon succeeded" }
      @cli.string_extractor = ex
      @cli.logger = @logger
      if block_given?
        begin
          yield self
        ensure
          close unless closed?
        end
      end
    end

    if defined?(::Encoding)
      def default_internal_encoding
        Encoding.default_internal
      end
    else
      def default_internal_encoding
        nil
      end
    end
    private :default_internal_encoding

    class NullLogger
      def debug(*args) end
      def info(*args) end
      def warn(*args) end
      def error(*args) end
      def fatal(*args) end
      def unknown(*args) end
      def close(*args) end
      def log(*args) end
      def add(*args) end
      def <<(*args) end
      def level=(*args) end
    end

    attr_reader :logon_string
    attr_reader :external_encoding
    attr_reader :internal_encoding

    def inspect
      "\#<#{self.class} #{@logon_string.safe_string}>"
    end

    if defined?(::Encoding)   # M17N enabled

      class StringExtractor
        class NoConversion
          def initialize(external)
            @external = external
          end

          def extract(str)
            str.force_encoding @external
            str
          end
        end

        def StringExtractor.get(external, internal)
          internal ? new(external, internal) : NoConversion.new(external)
        end

        def initialize(external, internal)
          @external = external
          @converter = Encoding::Converter.new(external, internal)
        end

        def extract(str)
          str.force_encoding @external
          @converter.convert(str)
        end
      end

    else   # no M17N: Ruby 1.8

      class StringExtractor
        def StringExtractor.get(external, internal)
          raise ArgumentError, "encoding conversion is not supported on Ruby 1.8" if internal
          new()
        end

        def extract(str)
          str
        end
      end

    end

    def execute_update(sql)
      log { "[UPD] #{sql}" }
      @cli.request canonicalize(sql)
      begin
        rs = @cli.read_result_set
        rs.value_all
        return rs
      ensure
        close_request
      end
    end

    alias update execute_update

    def execute_query(sql)
      log { "[SEL] #{sql}" }
      @cli.request canonicalize(sql)
      begin
        rs = @cli.read_result_set
        rs.value
        if block_given?
          yield rs
        else
          rs.fetch_all
        end
      ensure
        close_request
      end
      rs
    end

    alias query execute_query

    def canonicalize(sql)
      s = sql.gsub(/\r?\n/, "\r")
      @external_encoding ? s.encode(@external_encoding) : s
    end
    private :canonicalize

    def entries(sql)
      execute_query(sql).entries
    end

    def transaction
      aborting = false
      begin_transaction
      begin
        yield
      rescue UserAbort => err
        aborting = true
        raise err
      ensure
        if $@
          begin
            abort unless aborting
          rescue UserAbort   # do not override original exception
          end
        else
          end_transaction
        end
      end
    end

    def begin_transaction
      execute_update "BEGIN TRANSACTION"
    end

    def end_transaction
      execute_update "END TRANSACTION"
    end

    def abort
      execute_update "ABORT"
    end

    def drop(obj)
      execute_update "DROP #{obj.type_name} #{obj.name};"
    end

    def info
      recs = entries("HELP SESSION")
      unless recs.size == 1
        raise "HELP SESSION did not return 1 record??? size=#{recs.size}"
      end
      SessionInfo.for_record(recs.first)
    end

    # :nodoc: internal use only
    def close_request
      @cli.skip_current_request
      debug { "CLI.end_request" }
      @cli.end_request
    end

    def close
      log { "logoff..." }
      debug { "CLI.logoff" }
      @cli.logoff
      log { "logoff succeeded" }
    end

    def closed?
      not @cli.logging_on?
    end

    private

    def log(&block)
      @logger.info { "#{id_string}: #{yield}" }
    end

    def debug(&block)
      @logger.debug { "#{id_string}: #{yield}" }
    end

    def id_string
      "Teradata::Connection:#{'%x' % object_id}"
    end
  end


  class CLI   # reopen

    attr_accessor :string_extractor
    attr_accessor :logger

    def request(sql)
      @eor = false   # EndOfRequest
      send_request sql
    end

    # == Non-Valued Result CLI Response Sequence
    #
    # PclSUCCESS
    # PclENDSTATEMENT
    # PclSUCCESS
    # PclENDSTATEMENT
    # ...
    # PclENDREQUEST
    #
    # == Valued Result CLI Response Sequence
    #
    # === On Success
    #
    # PclSUCCESS
    # PclPREPINFO
    # PclDATAINFO
    # PclRECORD
    # PclRECORD
    # ...
    # PclENDSTATEMENT
    #
    # PclSUCCESS
    # PclPREPINFO
    # PclDATAINFO
    # PclRECORD
    # PclRECORD
    # ...
    # PclENDSTATEMENT
    #
    # PclENDREQUEST
    #
    # == CLI Response Sequence on Failure
    #
    # PclSUCCESS
    # PclENDSTATEMENT
    # ...
    # PclFAILURE

    def read_result_set
      each_fet_parcel do |parcel|
        case parcel.flavor_name
        when 'PclSUCCESS', 'PclFAILURE', 'PclERROR'
          return ResultSet.new(parcel.sql_status, self)
        end
      end
      nil
    end

    def read_metadata
      each_fet_parcel do |parcel|
        case parcel.flavor_name
        when 'PclPREPINFO'
          meta = MetaData.parse_prepinfo(parcel.data, string_extractor())
          debug { "metadata = #{meta.inspect}" }
          return meta
        when 'PclDATAINFO'
        when 'PclENDSTATEMENT'
          # null request returns no metadata.
          return nil
        else
          ;
        end
      end
      warn { "read_metadata: each_fet_parcel returned before PclENDSTATEMENT?" }
      nil   # FIXME: should raise?
    end

    def read_record
      each_fet_parcel do |parcel|
        case parcel.flavor_name
        when 'PclRECORD'
          return parcel.data
        when 'PclENDSTATEMENT'
          return nil
        else
          ;
        end
      end
      warn { "read_record: each_fet_parcel returned before PclENDSTATEMENT?" }
      nil   # FIXME: should raise?
    end

    def skip_current_statement
      each_fet_parcel do |parcel|
        case parcel.flavor_name
        when 'PclENDSTATEMENT'
          return
        end
      end
      # each_fet_parcel returns before PclENDSTATEMENT when error occured
    end

    def skip_current_request
      each_fet_parcel do |parcel|
        ;
      end
    end

    def each_fet_parcel
      return if @eor
      while true
        debug { "CLI.fetch" }
        fetch
        flavor = flavor_name()
        debug { "fetched: #{flavor}" }
        case flavor
        when 'PclENDREQUEST'
          debug { "=== End Request ===" }
          @eor = true
          return
        when 'PclFAILURE', 'PclERROR'
          @eor = true
        end
        yield FetchedParcel.new(flavor, self)
      end
    end

    private

    def warn(&block)
      @logger.warn { "Teradata::CLI:#{'%x' % object_id}: #{yield}" }
    end

    def debug(&block)
      @logger.debug { "Teradata::CLI:#{'%x' % object_id}: #{yield}" }
    end

  end


  class FetchedParcel

    def initialize(flavor_name, cli)
      @flavor_name = flavor_name
      @cli = cli
    end

    attr_reader :flavor_name

    def message
      @cli.message
    end

    def data
      @cli.data
    end

    def sql_status
      case @flavor_name
      when 'PclSUCCESS' then SuccessStatus.parse(@cli.data)
      when 'PclFAILURE' then FailureStatus.parse(@cli.data)
      when 'PclERROR'   then ErrorStatus.parse(@cli.data)
      else
        raise "must not happen: \#sql_status called for flavor #{@flavor_name}"
      end
    end

  end


  class SuccessStatus

    def SuccessStatus.parse(parcel_data)
      stmt_no, _, act_cnt, warn_code, n_fields, act_type, warn_len  = parcel_data.unpack('CCLSSSS')
      warning = parcel_data[13, warn_len]
      new(stmt_no, act_cnt, warn_code, n_fields, act_type, warning)
    end

    def initialize(stmt_no, act_cnt, warn_code, n_fields, act_type, warning)
      @statement_no = stmt_no
      @activity_count = act_cnt
      @warning_code = warn_code
      @num_fields = n_fields
      @activity_type = act_type
      @warning = warning
    end

    attr_reader :statement_no
    attr_reader :activity_count
    attr_reader :acitivity_type
    attr_reader :n_fields
    attr_reader :warning_code
    attr_reader :warning

    def inspect
      "\#<Success \##{@statement_no} cnt=#{@activity_count}>"
    end

    def error_code
      0
    end

    def info
      nil
    end

    def message
      ''
    end

    def succeeded?
      true
    end

    def failure?
      false
    end

    def error?
      false
    end

    def value
    end

    def warned?
      @warning_code != 0
    end

    ACTIVITY_ECHO = 33

    def echo?
      @activity_type == ACTIVITY_ECHO
    end

  end


  class FailureStatus

    def FailureStatus.parse(parcel_data)
      stmt_no, info, code, msg_len = parcel_data.unpack('SSSS')
      new(stmt_no, code, info, parcel_data[8, msg_len])
    end

    def initialize(stmt_no, error_code, info, msg)
      @statement_no = stmt_no
      @error_code = error_code
      @info = info
      @message = msg
    end

    attr_reader :statement_no
    attr_reader :error_code
    attr_reader :info   # error_code dependent additional (error) information.
    attr_reader :message

    def inspect
      "\#<Failure \##{@statement_no} [#{@error_code}] #{@message}>"
    end

    def activity_count
      nil
    end

    def warning_code
      nil
    end

    def n_fields
      nil
    end

    def warning
      nil
    end

    def succeeded?
      false
    end

    def failure?
      false
    end

    def error?
      false
    end

    ERROR_CODE_ABORT = 3514

    def value
      if @error_code == ERROR_CODE_ABORT
        raise UserAbort.new(@error_code, @info, @message)
      else
        raise SQLError.new(@error_code, @info,
            "SQL error [#{@error_code}]: #{@message}")
      end
    end

    def warned?
      false
    end

    def echo?
      false
    end

  end


  # PclERROR means CLI or MTDP error.
  # PclFAILURE and PclERROR have same data format, we reuse its code.
  class ErrorStatus < FailureStatus

    def inspect
      "\#<Error \##{@statement_no} [#{@error_code}] #{@message}>"
    end

    def failure?
      false
    end

    def error?
      true
    end

    def value
      raise Error, "CLI error: #{@message}"
    end

  end


  class ResultSet

    include Enumerable
    extend Forwardable

    def initialize(status, cli)
      @status = status
      @cli = cli
      @next = nil
      @closed = false
      @metadata_read = false
      @metadata = nil
      @valued = false
      @entries = nil
    end

    def inspect
      "\#<ResultSet #{@status.inspect} next=#{@next.inspect}>"
    end

    def_delegator '@status', :error_code
    def_delegator '@status', :info
    def_delegator '@status', :message
    def_delegator '@status', :statement_no
    def_delegator '@status', :activity_count
    def_delegator '@status', :n_fields
    def_delegator '@status', :warning_code
    def_delegator '@status', :warning

    def next
      return @next if @next
      close unless closed?
      value
      rs = @cli.read_result_set
      @next = rs
      rs.value if rs
      rs
    end

    def each_result_set
      rs = self
      while rs
        begin
          yield rs
        ensure
          rs.close unless rs.closed?
        end
        rs = rs.next
      end
      nil
    end

    def value_all
      each_result_set do |rs|
        ;
      end
    end

    def value
      return if @valued
      @status.value
      @valued = true
    end

    def closed?
      @closed
    end

    def close
      check_connection
      @cli.skip_current_statement
      @closed = true
    end

    def each_record(&block)
      return @entries.each(&block) if @entries
      check_connection
      unless @metadata_read
        @metadata = @cli.read_metadata
        unless @metadata
          @closed = true
          return
        end
        @metadata_read = true
      end
      while rec = @cli.read_record
        yield @metadata.unmarshal(rec)
      end
      @closed = true
    end

    alias each each_record

    # read all record and return it
    def entries
      return @entries if @entries
      check_connection
      map {|rec| rec }
    end

    # read all records and save it for later reference.
    def fetch_all
      return if @entries
      check_connection
      @entries = map {|rec| rec }
      nil
    end

    private

    def check_connection
      raise ConnectionError, "already closed ResultSet" if closed?
    end

  end


  class MetaData

    def MetaData.parse_prepinfo(binary, extractor)
      f = StringIO.new(binary)
      cost_estimate, summary_count = f.read(10).unpack('dS')
      return new([]) if f.eof?   # does not have column count
      count, = f.read(2).unpack('S')
      new(count.times.map {
        type, data_len, name_len = f.read(6).unpack('SSS')
        column_name = f.read(name_len)
        format_len, = f.read(2).unpack('S')
        format = f.read(format_len)
        title_len, = f.read(2).unpack('S')
        title = f.read(title_len)
        FieldType.create(type, data_len, column_name, format, title, extractor)
      })
    end

    def MetaData.parse_datainfo(binary)
      n_entries, *entries = binary.unpack('S*')
      unless entries.size % 2 == 0 and entries.size / 2 == n_entries
        raise MetaDataFormatError, "could not get correct size of metadata (expected=#{n_entries * 2}, really=#{entries.size})"
      end
      new(entries.each_slice(2).map {|type, len| FieldType.create(type, len) })
    end

    def initialize(types)
      @types = types
    end

    def num_columns
      @types.size
    end

    def column(nth)
      @types[nth]
    end

    def each_column(&block)
      @types.each(&block)
    end

    def field_names
      @types.map {|t| t.name }
    end

    def inspect
      "\#<#{self.class} [#{@types.map {|t| t.to_s }.join(', ')}]>"
    end

    def unmarshal(data)
      f = StringIO.new(data)
      cols = @types.zip(read_indicator(f)).map {|type, is_null|
        val = type.unmarshal(f)   # We must read value regardless of NULL.
        is_null ? nil : val
      }
      Record.new(self, @types.zip(cols).map {|type, col| Field.new(type, col) })
    end

    private

    def read_indicator(f)
      f.read(num_indicator_bytes())\
          .unpack(indicator_template()).first\
          .split(//)[0, num_indicator_bits()]\
          .map {|c| c == '1' }
    end

    def indicator_template
      'B' + (num_indicator_bytes() * 8).to_s
    end

    def num_indicator_bytes
      (num_indicator_bits() + 7) / 8
    end

    def num_indicator_bits
      @types.size
    end

  end


  # Unsupported Types:
  # BLOB            400
  # BLOB_DEFERRED   404
  # BLOB_LOCATOR    408
  # CLOB            416
  # CLOB_DEFERRED   420
  # CLOB_LOCATOR    424
  # GRAPHIC_NN      468
  # GRAPHIC_N       469
  # LONG_VARBYTE_NN 696
  # LONG_VARBYTE_N  697
  # LONG_VARCHAR_NN 456
  # LONG_VARCHAR_N  457
  # LONG_VARGRAPHIC_NN      472
  # LONG_VARGRAPHIC_N       473
  # VARGRAPHIC_NN   464
  # VARGRAPHIC_N    465

  class FieldType
    @@types = {}

    def self.bind_code(name, code)
      @@types[code] = [name, self]
    end
    private_class_method :bind_code

    def FieldType.create(code, len, name, format, title, extractor)
      type_name, type_class = @@types[code]
      raise MetaDataFormatError, "unknown type code: #{code}" unless name
      type_class.new(type_name, code, len, name, format, title, extractor)
    end

    def FieldType.codes
      @@types.keys
    end

    def FieldType.code_names
      @@types.values.map {|name, c| name }
    end

    def initialize(type_name, type_code, len, name, format, title, extractor)
      @type_name = type_name
      @type_code = type_code
      @length = len
      @name = name
      @format = format
      @title = title
      @extractor = extractor
    end

    attr_reader :type_name
    attr_reader :type_code
    attr_reader :name
    attr_reader :format
    attr_reader :title

    def to_s
      "(#{@name} #{@type_name}:#{@type_code})"
    end

    def inspect
      "\#<FieldType #{@name} (#{@type_name}:#{@type_code})>"
    end

    # default implementation: only read as string.
    def unmarshal(f)
      f.read(@length)
    end
  end

  # CHAR: fixed-length character string
  # BYTE: fixed-length byte string
  class FixStringType < FieldType
    bind_code :CHAR_NN, 452
    bind_code :CHAR_N, 453
    bind_code :BYTE_NN, 692
    bind_code :BYTE_N, 693

    def unmarshal(f)
      @extractor.extract(f.read(@length))
    end
  end

  # VARCHAR: variable-length character string
  # VARBYTE: variable-length byte string
  class VarStringType < FieldType
    bind_code :VARCHAR_NN, 448
    bind_code :VARCHAR_N, 449
    bind_code :VARBYTE_NN, 688
    bind_code :VARBYTE_N, 689

    def unmarshal(f)
      real_len = f.read(2).unpack('S').first
      @extractor.extract(f.read(real_len))
    end
  end

  # 1 byte signed integer
  class ByteIntType < FieldType
    bind_code :BYTEINT_NN, 756
    bind_code :BYTEINT_N, 757

    def unmarshal(f)
      f.read(@length).unpack('c').first
    end
  end

  # 2 byte signed integer
  class SmallIntType < FieldType
    bind_code :SMALLINT_NN, 500
    bind_code :SMALLINT_N, 501

    def unmarshal(f)
      f.read(@length).unpack('s').first
    end
  end

  # 4 byte signed integer
  class IntegerType < FieldType
    bind_code :INTEGER_NN, 496
    bind_code :INTEGER_N, 497

    def unmarshal(f)
      f.read(@length).unpack('l').first
    end
  end

  # 8 byte signed integer
  class BigIntType < FieldType
    bind_code :BIGINT_NN, 600
    bind_code :BIGINT_N, 601

    def unmarshal(f)
      f.read(@length).unpack('q').first
    end
  end

  class FloatType < FieldType
    bind_code :FLOAT_NN, 480
    bind_code :FLOAT_N, 481

    def unmarshal(f)
      f.read(@length).unpack('d').first
    end
  end

  class DecimalType < FieldType
    bind_code :DECIMAL_NN, 484
    bind_code :DECIMAL_N, 485

    def initialize(type_name, type_code, len, name, format, title, extractor)
      super
      @width, @fractional = len.divmod(256)
      @length, @template = get_binary_data(@width)
    end

    def get_binary_data(width)
      case
      when width <=  2 then return 1, 'c'
      when width <=  4 then return 2, 's'
      when width <=  9 then return 4, 'l'
      when width <= 18 then return 8, 'q'
      else return 16, nil
      end
    end

    attr_reader :width
    attr_reader :fractional

    def unmarshal(f)
      insert_fp(read_base_int(f).to_s, @fractional)
    end

    private

    def read_base_int(f)
      if @template
        f.read(@length).unpack(@template).first
      else
        # PLATFORM SPECIFIC: little endian
        lower, upper = f.read(@length).unpack('qQ')
        sign = upper >= 0 ? +1 : -1
        sign * (upper.abs << 64 | lower)
      end
    end

    def insert_fp(str, frac)
      if frac == 0
        str
      else
        return '0.' + str if str.size == frac
        str[-frac, 0] = '.'
        str
      end
    end
  end

  class DateType < FieldType
    bind_code :DATE_NN, 752
    bind_code :DATE_N, 753

    def unmarshal(f)
      d = (f.read(@length).unpack('l').first + 19000000).to_s
      d[0,4] + '-' + d[4,2] + '-' + d[6,2]
    end
  end

  # TIME, TIMESTAMP are same as CHAR.


  class Record

    include Enumerable

    def initialize(metadata, fields)
      @metadata = metadata
      @fields = fields
      @index = build_name_index(metadata)
    end

    def build_name_index(meta)
      h = {}
      idx = 0
      meta.each_column do |c|
        h[c.name.downcase] = idx
        h[idx] = idx
        idx += 1
      end
      h
    end
    private :build_name_index

    def size
      @fields.size
    end

    def keys
      @metadata.field_names
    end

    def [](key)
      field(key).value
    end

    def field(key)
      i = (@index[key.to_s.downcase] || @index[key]) or
          raise ArgumentError, "bad field key: #{key}"
      @fields[i]
    end

    def each_field(&block)
      @fields.each(&block)
    end

    def each_value
      @fields.each {|c|
        yield c.value
      }
    end

    alias each each_value

    def values_at(*keys)
      keys.map {|k| self[k] }
    end

    def to_a
      @fields.map {|f| f.value }
    end

    def to_h
      h = {}
      @metadata.field_names.zip(@fields) do |name, field|
        h[name] = field.value
      end
      h
    end

    def inspect
      "\#<Record #{@fields.map {|c| c.to_s }.join(', ')}>"
    end

  end


  class Field

    def initialize(metadata, value)
      @metadata = metadata
      @value = value
    end

    attr_reader :value
    alias data value

    extend Forwardable
    def_delegator "@metadata", :name
    def_delegator "@metadata", :format
    def_delegator "@metadata", :title

    def type
      @metadata.type_name
    end

    def type_code
      @metadata.type_code
    end

    def null?
      @value.nil?
    end

    def to_s
      "(#{name} #{@value.inspect})"
    end

  end

end
