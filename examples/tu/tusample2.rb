require 'teradata'
Teradata.connect(ENV['LOGON_STRING']) {|conn|
  conn.tables("tudemo").
      sort_by {|table| -table.size }.
      first(5).
      each {|table| puts "#{table.name}\t#{table.size}" }
}
