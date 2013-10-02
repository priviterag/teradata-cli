#!/usr/bin/env ruby
#
# Show Query Bands of current sessions.
#
# Usage:
#    $ export LOGON_STRING=dbc/user,pass
#    $ ruby example/show-queryband.rb
#

require 'teradata'

logon_string = ENV['LOGON_STRING']
unless logon_string
  $stderr.puts "set environment variable LOGON_STRING"
  exit 1
end

Teradata.connect(logon_string) {|conn|
  conn.query("SELECT * FROM dbc.sessionInfo") {|rs|
    rs.each do |rec|
      user = rec[:UserName].strip
      band = rec[:QueryBand]
      puts "#{user}\t#{band}"
    end
  }
}
