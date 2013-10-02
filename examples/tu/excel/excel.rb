require 'win32ole'
require 'fileutils'

class Excel

  def Excel.open_worksheet(path, sheet_name, mode, &block)
    open {|app| app.open_worksheet(path, sheet_name, mode, &block) }
  end

  def Excel.open_book(path, mode, &block)
    open {|app| app.open_book(path, mode, &block) }
  end

  FileSystem = WIN32OLE.new('Scripting.FileSystemObject')

  def Excel.open(visible = $DEBUG, &block)
    new(visible, &block)
  end

  @@const_loaded = false

  def initialize(visible = $DEBUG)
    @app = WIN32OLE.new('Excel.Application')
    unless @@const_loaded
      WIN32OLE.const_load @app, Excel
      @@const_loaded = true
    end
    @app.visible = true if visible
    yield self if block_given?
  ensure
    quit if block_given? and @app
  end

  def quit
    @app.quit
  end

  def open_book(path, mode)
    begin
      book = @app.workbooks.open(win_extend_path(path))
      yield book
      book.save if mode == 'w'
    ensure
      book.saved = true   # avoid confirmation message
      @app.workbooks.close
    end
  end

  def open_worksheet(path, sheet_name, mode)
    open_book(path, mode) {|book|
      sheet = book.worksheets.item(1)
      sheet.extend WorkSheetMethods
      yield sheet
    }
  end

  def win_extend_path(path)
    FileSystem.getAbsolutePathName(path)
  end
  private :win_extend_path

  module WorkSheetMethods
    def [](y, x)
      cell = cells().item(y, x)
      if cell.mergeCells
        cell.mergeArea.item(1, 1).value
      else
        cell.value
      end
    end

    def []=(y, x, value)
      cell = cells().item(y, x)
      if cell.mergeCells
        cell.mergeArea.item(1, 1).value = value
      else
        cell.value = value
      end
    end

    def last_cell
      cells().specialCells(Excel::XlLastCell)
    end
  end

end
