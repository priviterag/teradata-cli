# $Id: bitdao.rb 184 2009-08-12 08:46:22Z aamine $

require 'teradata'

module Teradata
  class Error
    include ::BitDAO::Error
  end

  class SQLError
    include ::BitDAO::Error
  end

  class Connection   # reopen
    def error_class
      Error
    end

    def sql_error_class
      SQLError
    end
  end
end
