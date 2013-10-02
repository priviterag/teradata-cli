require 'bitweb'
require 'bitdao'

module TDWalker

  class RequestHandler < BitWeb::RequestHandler; end

  Models = BitWeb::Models.new(:dao)

  ViewManager = BitWeb::ViewManager

  class Screen < BitWeb::TemplateScreen
    def database_url(name)
      url("/database/show?name=#{name}")
    end
  end

  class Controller < BitWeb::Controller; end

  class DatabaseController < Controller
    depends :dao

    ShowScreen = Screen.new_class(:database)

    def handle_show(req)
      name = req.get('name') {|val|
        val.must_string
        val.string.strip
      }
      @views.new(ShowScreen, @dao.database(name))
    end
  end

  class DAO < BitDAO
    def database(name)
      @connection.database(name)
    end
  end

  class Error < ::StandardError; end

end
