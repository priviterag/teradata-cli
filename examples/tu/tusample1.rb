require 'teradata'
Teradata.connect(ENV['LOGON_STRING']) {|conn|
  conn.tables("tudemo").
      select {|table| /_bak$/ =~ table.name }.
      each {|table| conn.drop table }
}
