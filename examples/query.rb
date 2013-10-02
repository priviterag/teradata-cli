#!/usr/bin/env ruby
#
# Dispatches SQL query and shows result records.
#
# Usage:
#    $ export LOGON_STRING=dbc/user,pass
#    $ ruby example/query.rb 'SELECT * FROM x'
#

require 'teradata'
require 'logger'
require 'pp'

logon_string = ENV['LOGON_STRING']
unless logon_string
  $stderr.puts "set environment variable LOGON_STRING"
  exit 1
end

sql = ARGV[0]
unless sql
  $stderr.puts "Usage: ruby #{File.basename($0)} QUERY"
  exit 1
end

log = Logger.new($stderr)
log.sev_threshold = $DEBUG ? Logger::DEBUG : Logger::INFO

Teradata.connect(logon_string, :logger => log) {|conn|
  conn.query(sql) {|result_sets|
    result_sets.each_result_set do |rs|
      pp rs
      rs.each_record do |rec|
        pp rec
      end
    end
  }
}
