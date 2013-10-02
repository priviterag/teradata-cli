#!/usr/bin/env ruby
#
# Dispatches non-SELECT DML or DDL.
#
# Usage:
#    $ export LOGON_STRING=dbc/user,pass
#    $ ruby example/update.rb 'CREATE TABLE x (x INTEGER)'
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
  pp conn.update(sql)
}
