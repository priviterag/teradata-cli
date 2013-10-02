require 'excel'
require 'fileutils'
require 'pp'
require 'teradata'

def main
  logon_string, template, output = ARGV
  usage_exit unless logon_string
  usage_exit unless template
  output ||= 'out.xls'

  FileUtils.cp template, output
  Excel.open(true) {|app|
    app.open_worksheet(output, 'Sheet1', 'w') {|sheet|
      fill_sheet sheet, logon_string
    }
  }
end

def fill_sheet(sheet, logon_string)
  # Get SQL Cell
  sql_cell = sheet.cells.find('%SQL')
  p ['sql_cell', sql_cell.address]
  sql = sql_cell.value.slice(/%SQL\s+(.*)/m, 1).strip
  p sql

  # Get Value Cells
  value_cell = sheet.cells.find('%=', sql_cell)
  p ['value', value_cell.address]
  exprs = [expr_proc(value_cell)]
  c = value_cell
  while true
    next_c = sheet.range(address(c.row.to_i, c.column.to_i + 1))
    break unless /^%=/ =~ next_c.value
    c = next_c
    p ['value', c.address]
    exprs.push expr_proc(c)
  end
  pp exprs

  tmpl_range = sheet.range(value_cell.address + ':' + c.address)
  xl = value_cell.column
  xr = c.column
  y = value_cell.row + 1

  # Execute SQL and fill cells by data
  Teradata.connect(logon_string) {|conn|
    conn.query(sql) {|rs|
      rs.each do |rec|
        pp rec
        values = exprs.map {|expr| expr.call(rec) }
        pp values
        tmpl_range.copy
        sheet.range(address(y, xl) + ':' + address(y, xr)).insert Excel::XlShiftDown
        sheet.range(address(y, xl) + ':' + address(y, xr)).value = values
        y += 1
      end
    }
  }

  # remove metadata cells
  tmpl_range.delete Excel::XlShiftUp
  sheet.rows(sql_cell.row).delete
end

def usage_exit
  $stderr.puts "Usage: #{$0} LOGON_STRING TEMPLATE [OUTPUT]"
  exit 1
end

def expr_proc(cell)
  expr = cell.value.slice(/%=(.*)/m, 1).strip
  lambda {|_| eval(expr).to_s }
end

def address(row, col)
  "$#{column_string(col)}$#{row}"
end

ALPHA = ('a'..'z').to_a

def column_string(col)
  result = []
  n = col - 1
  while n >= 26
    n, mod = n.divmod(26)
    result.unshift mod
    n -= 1
  end
  result.unshift n
  result.map {|i| ALPHA[i] }.join('')
end

main
